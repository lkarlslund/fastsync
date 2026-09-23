package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"runtime"
	"runtime/metrics"
	"runtime/pprof"
	"strings"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/lkarlslund/fastsync"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"github.com/ugorji/go/codec"
	"golang.org/x/term"
)

func terminalMode(stdinTTY, stdoutTTY bool, termName string) bool {
	return stdinTTY && stdoutTTY && termName != "" && termName != "dumb"
}

func interactiveTerminal() bool {
	return terminalMode(
		term.IsTerminal(int(os.Stdin.Fd())),
		term.IsTerminal(int(os.Stdout.Fd())),
		strings.ToLower(os.Getenv("TERM")),
	)
}

func configureConsoleLogger(level zerolog.Level, noColor bool) {
	fastsync.Logger = zerolog.New(zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: time.RFC3339,
		NoColor:    noColor,
	}).With().Timestamp().Logger().Level(level)
}

var (
	// General/root options
	directory        string
	loglevel         string
	cpuprofile       string
	cpuprofilelength int
	ramlimit         uint64
	passwordFile     string
	memoryAbort      = make(chan error, 1)
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "fastsync [command]",
		Short: "fastsync client/server",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			// Logging setup
			var zll zerolog.Level
			switch strings.ToLower(loglevel) {
			case "trace":
				zll = zerolog.TraceLevel
			case "debug":
				zll = zerolog.DebugLevel
			case "info":
				zll = zerolog.InfoLevel
			case "warn":
				zll = zerolog.WarnLevel
			case "error":
				zll = zerolog.ErrorLevel
			default:
				fastsync.Logger.Fatal().Msgf("Invalid log level: %v", loglevel)
			}
			configureConsoleLogger(zll, !term.IsTerminal(int(os.Stderr.Fd())))
			if ramlimit > 0 {
				if cmd.Name() == "client" {
					startMemoryWatch(ramlimit, memoryAbort)
				} else {
					startMemoryWatch(ramlimit)
				}
			}

			// CPU profiling setup
			if cpuprofile == "auto" {
				go autoProfile()
			} else if cpuprofile != "" {
				f, err := fastsync.OpenFileNoFollow(cpuprofile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
				if err != nil {
					log.Fatal(err)
				}
				err = pprof.StartCPUProfile(f)
				if err != nil {
					fastsync.Logger.Fatal().Msgf("Can't start profiling: %v", err)
				}
				if cpuprofilelength > 0 {
					go func() {
						time.Sleep(time.Duration(cpuprofilelength) * time.Second)
						pprof.StopCPUProfile()
						if err := f.Close(); err != nil {
							fastsync.Logger.Error().Msgf("Error closing CPU profile: %v", err)
						}
						fastsync.Logger.Warn().Msgf("CPU profiling stopped")
					}()
				} else {
					defer pprof.StopCPUProfile()
				}
			}

			// Directory setup
			if directory == "." {
				var err error
				directory, err = os.Getwd()
				if err != nil {
					fastsync.Logger.Fatal().Msgf("Error getting working directory: %v", err)
				}
			}
		},
	}

	// Root persistent flags
	rootCmd.PersistentFlags().StringVar(&passwordFile, "password-file", "", "Owner-only file containing the shared password (server/client/verify/shutdown)")
	rootCmd.PersistentFlags().StringVar(&directory, "directory", ".", "Directory to use as source or target")
	rootCmd.PersistentFlags().StringVar(&loglevel, "loglevel", "info", "Log level")
	rootCmd.PersistentFlags().StringVar(&cpuprofile, "cpuprofile", "", "Write cpu profile to file (filename, use 'auto' to trigger auto profiling)")
	rootCmd.PersistentFlags().IntVar(&cpuprofilelength, "cpuprofilelength", 0, "Stop profiling after N seconds, 0 to profile until program terminates")
	rootCmd.PersistentFlags().Uint64Var(&ramlimit, "ramlimit", 0, "Abort with nonzero exit when sampled process memory exceeds this many bytes (0 disables)")

	// Server command
	var bind string
	var serverMetadata int
	var serverReads int
	var serverAuto bool
	var serverCmd = &cobra.Command{
		Use:   "server",
		Short: "Run as server",
		Run: func(cmd *cobra.Command, args []string) {
			password, err := readPasswordFile(passwordFile)
			if err != nil {
				fastsync.Logger.Fatal().Err(err).Msg("Invalid password file")
			}
			fastsyncserver := fastsync.NewServer()
			fastsyncserver.ConfigurePassword(password)
			fastsyncserver.BasePath = directory
			if err := fastsyncserver.PinRoot(); err != nil {
				fastsync.Logger.Fatal().Err(err).Msg("Cannot open source root")
			}
			defer fastsyncserver.CloseFiles()
			if err := fastsyncserver.ConfigureMetadata(serverMetadata); err != nil {
				fastsync.Logger.Fatal().Err(err).Msg("Invalid metadata limit")
			}
			if err := fastsyncserver.ConfigureIO(serverReads, serverAuto); err != nil {
				fastsync.Logger.Fatal().Err(err).Msg("Invalid source IO limits")
			}

			listener, err := net.Listen("tcp", bind)
			if err != nil {
				fastsync.Logger.Fatal().Msgf("Error binding listener: %v", err)
			}
			shutdownReplied := make(chan struct{}, 1)
			fastsync.Logger.Info().Msgf("Listening on %s", bind)
			go func() {
				for {
					conn, err := listener.Accept()
					if err != nil {
						fastsync.Logger.Error().Msgf("Error accepting connection: %v", err)
						return
					}
					fastsync.Logger.Info().Msgf("Accepted connection from %v", conn.RemoteAddr())
					wconn := fastsync.NewPerformanceWrapper(conn, fastsyncserver.Perf.GetAtomicAdder(fastsync.RecievedOverWire), fastsyncserver.Perf.GetAtomicAdder(fastsync.SentOverWire))
					cconn := fastsync.CompressedReadWriteCloser(wconn)
					wcconn := fastsync.NewPerformanceWrapper(cconn, fastsyncserver.Perf.GetAtomicAdder(fastsync.RecievedBytes), fastsyncserver.Perf.GetAtomicAdder(fastsync.SentBytes))
					go func() {
						session := fastsyncserver.NewSession()
						defer session.CloseFiles()
						rpcserver := rpc.NewServer()
						if err := rpcserver.Register(session); err != nil {
							_ = conn.Close()
							return
						}
						var h codec.MsgpackHandle
						rpcserver.ServeCodec(&shutdownReplyCodec{ServerCodec: codec.GoRpc.ServerCodec(wcconn, &h), replied: shutdownReplied})
						fastsync.Logger.Info().Msgf("Closed connection from %v", conn.RemoteAddr())
					}()
				}
			}()
			fastsyncserver.Wait()
			<-shutdownReplied
			_ = listener.Close()
		},
	}
	serverCmd.Flags().IntVar(&serverMetadata, "metadata-parallel", 8, "Maximum source metadata operations across all clients")
	serverCmd.Flags().IntVar(&serverReads, "read-parallel", 64, "Maximum concurrent source reads across all clients")
	serverCmd.Flags().BoolVar(&serverAuto, "autotune", false, "Adapt source read concurrency when driven by an autotuning client")
	serverCmd.Flags().StringVar(&bind, "bind", "0.0.0.0:7331", "Address to bind to")

	// Client command
	var (
		hardlinks         bool
		xattr             bool
		checksum          bool
		deleteOpt         bool
		durable           bool
		parallelfile      int
		paralleldir       int
		queuesize         int
		transferblocksize int
	)
	var sourcePath string
	var resumeCache, resumeIdentity string
	var resumePosition bool
	var pipeline, autoTune bool
	var flushInterval time.Duration
	var flushBytes int64
	var flushFiles, flushWorkers int
	var metadataParallel int
	var writeParallel, cachedFiles int
	var bufferBytes int64
	var clientCmd = &cobra.Command{
		SilenceUsage: true,
		Use:          "client",
		Short:        "Run as client",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			if len(args) < 1 {
				return errors.New("please provide server address and port to connect to")
			}
			serveraddr := args[0]
			interactive := interactiveTerminal()
			if !interactive {
				configureConsoleLogger(fastsync.Logger.GetLevel(), true)
			}

			c := fastsync.NewClient()
			c.BasePath = directory
			c.ResumeCache = resumeCache
			c.ResumePosition = resumePosition
			if resumeIdentity != "" {
				c.ResumeIdentity = serveraddr + "/" + sourcePath + "/" + resumeIdentity
			}
			c.Pipeline = pipeline || autoTune
			c.AutoTune = autoTune
			c.FlushInterval = flushInterval
			c.FlushBytes = flushBytes
			c.FlushFiles = flushFiles
			c.FlushWorkers = flushWorkers
			c.WriteParallel = writeParallel
			c.MetadataParallel = metadataParallel
			c.CachedFiles = cachedFiles
			c.BufferBytes = bufferBytes
			c.SourcePath = sourcePath
			c.PreserveHardlinks = hardlinks
			c.ParallelDir = paralleldir
			c.ParallelFile = parallelfile
			c.QueueSize = queuesize
			c.BlockSize = transferblocksize
			c.AlwaysChecksum = checksum
			c.Options.SendXattr = xattr
			c.Delete = deleteOpt
			c.Durable = durable

			sourceLabel := strings.TrimSuffix(serveraddr, "/") + "/" + strings.TrimPrefix(c.SourcePath, "/")
			copyMessage := fmt.Sprintf("Copying from %s to %s", sourceLabel, c.BasePath)
			collector := startStatsCollector(c, time.Second)
			var tuiDone chan error
			signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
			defer stopSignals()
			interrupted := signalCtx.Done()
			clientLogLevel := fastsync.Logger.GetLevel()
			dashboardActive := false
			if interactive {
				tuiDone = make(chan error, 1)
				ready := make(chan dashboardReady, 1)
				go func() {
					tuiDone <- showStatsTUI(collector.samples, ready)
				}()
				dashboard := <-ready
				if dashboard.err != nil {
					fastsync.Logger.Error().Msgf("Dashboard error: %v", dashboard.err)
					<-tuiDone
					tuiDone = nil
				} else {
					fastsync.Logger = zerolog.New(dashboard.logWriter).With().Timestamp().Logger().Level(clientLogLevel)
					dashboardActive = true
				}
			}

			collectorStopped := false
			var finalTotals fastsync.PerformanceEntry
			stopCollector := func() fastsync.PerformanceEntry {
				if !collectorStopped {
					finalTotals = collector.Stop()
					collectorStopped = true
				}
				return finalTotals
			}
			dashboardStopped := false
			finishDashboard := func() fastsync.PerformanceEntry {
				dashboardStopped = true
				total := stopCollector()
				close(collector.samples)
				if tuiDone != nil {
					tuiErr := <-tuiDone
					if dashboardActive {
						configureConsoleLogger(clientLogLevel, false)
					}
					if tuiErr != nil {
						fastsync.Logger.Error().Err(tuiErr).Msg("Dashboard error")
					}
				}
				return total
			}
			defer func() {
				if !dashboardStopped {
					if err != nil {
						fastsync.Logger.Error().Err(err).Msg("Client startup failed")
					}
					finishDashboard()
				}
			}()
			fastsync.Logger.Info().Msg(copyMessage)
			var passwordErr error
			c.Password, passwordErr = readPasswordFile(passwordFile)
			if passwordErr != nil {
				return passwordErr
			}
			conn, err := (&net.Dialer{Timeout: 30 * time.Second}).DialContext(signalCtx, "tcp", serveraddr)
			if err != nil {
				return fmt.Errorf("connect to %s: %w", serveraddr, err)
			}
			fastsync.Logger.Info().Msgf("Connected to %s", serveraddr)

			wconn := fastsync.NewPerformanceWrapper(conn, c.Perf.GetAtomicAdder(fastsync.RecievedOverWire), c.Perf.GetAtomicAdder(fastsync.SentOverWire))
			cconn := fastsync.CompressedReadWriteCloser(wconn)
			wcconn := fastsync.NewPerformanceWrapper(cconn, c.Perf.GetAtomicAdder(fastsync.RecievedBytes), c.Perf.GetAtomicAdder(fastsync.SentBytes))

			var h codec.MsgpackHandle
			rpcCodec := codec.GoRpc.ClientCodec(wcconn, &h)
			rpcClient := rpc.NewClientWithCodec(rpcCodec)

			fastsync.Logger.Info().Msgf("Client processing with up to %v incoming file blocks at %v bytes (RAM usage could be %v bytes or more)", c.ParallelFile, c.BlockSize, c.ParallelFile*c.BlockSize)

			rpcClosed := false
			{
				syncDone := make(chan error, 1)
				go func() {
					syncDone <- c.Run(rpcClient)
				}()
				select {
				case err = <-syncDone:
				case memoryErr := <-memoryAbort:
					c.NotifyShutdown()
					fastsync.Logger.Warn().Msg("Memory limit reached; shutting down and saving resume state")
					rpcClosed = true
					_ = rpcClient.Close()
					err = errors.Join(<-syncDone, memoryErr)
				case <-interrupted:
					c.NotifyShutdown()
					fastsync.Logger.Info().Msg("Shutdown requested; finishing pending writes and saving resume state. Please wait.")
					rpcClosed = true
					_ = rpcClient.Close()
					err = errors.Join(<-syncDone, errors.New("transfer interrupted"))
				}
			}
			if err != nil {
				fastsync.Logger.Error().Msgf("Error running client: %v", err)
			}

			if !rpcClosed {
				err = errors.Join(err, rpcClient.Close())
			}

			fastsync.Logger.Info().Msg("Transfer cleanup complete")
			totalhistory := stopCollector()

			fastsync.Logger.Warn().Msgf("Final statistics")
			fastsync.Logger.Warn().Msgf("Wired %v, transferred %v, local read/write %v - %v files - %v dirs",
				humanize.Bytes(totalhistory.Get(fastsync.SentOverWire)+totalhistory.Get(fastsync.RecievedOverWire)),
				humanize.Bytes(totalhistory.Get(fastsync.SentBytes)+totalhistory.Get(fastsync.RecievedBytes)),
				humanize.Bytes(totalhistory.Get(fastsync.ReadBytes)+totalhistory.Get(fastsync.WrittenBytes)),
				totalhistory.Get(fastsync.FilesProcessed),
				totalhistory.Get(fastsync.DirectoriesProcessed))
			fastsync.Logger.Warn().Msgf("Processed data %s (all paths), unique data %s (once per source inode)", humanize.Bytes(totalhistory.Get(fastsync.BytesProcessed)), humanize.Bytes(totalhistory.Get(fastsync.BytesUniqueProcessed)))
			fastsync.Logger.Warn().Msgf("Existing pass: examined %d paths, found %d reusable inode groups", totalhistory.Get(fastsync.ExistingExamined), totalhistory.Get(fastsync.ReuseGroups))
			fastsync.Logger.Warn().Msgf("Deleted %v", totalhistory.Get(fastsync.EntriesDeleted))
			finishDashboard()
			return err
		},
	}
	clientCmd.Flags().BoolVar(&resumePosition, "resume-position", false, "Skip checkpointed top-level subtrees; requires an immutable source and exclusively managed destination")
	clientCmd.Flags().StringVar(&resumeCache, "resume-cache", "", "Persistent hardlink hint cache outside the destination")
	clientCmd.Flags().StringVar(&resumeIdentity, "resume-id", "", "Stable source snapshot identity for the resume cache")
	clientCmd.Flags().Int64Var(&flushBytes, "flush-bytes", 256<<20, "Maximum written bytes awaiting file flush")
	clientCmd.Flags().IntVar(&flushFiles, "flush-files", 64, "Maximum files awaiting flush, including open files")
	clientCmd.Flags().IntVar(&flushWorkers, "flush-workers", 64, "Maximum background file-flush workers (share destination IO limit)")
	clientCmd.Flags().DurationVar(&flushInterval, "flush-interval", 30*time.Second, "Age interval for background file flushes (0 disables; also flushes completed files and batches at byte limits)")
	clientCmd.Flags().BoolVar(&pipeline, "pipeline", false, "Use bounded read-ahead and independent destination writers")
	clientCmd.Flags().BoolVar(&autoTune, "autotune", false, "Adapt source reads and destination writes (enables pipeline)")
	clientCmd.Flags().IntVar(&metadataParallel, "metadata-parallel", 8, "Maximum concurrent destination stat, metadata update and publication operations in pipeline mode")
	clientCmd.Flags().IntVar(&writeParallel, "write-parallel", 64, "Maximum active destination data files/write operations")
	clientCmd.Flags().IntVar(&cachedFiles, "cached-files", 64, "Maximum admitted streaming files, including files awaiting writes")
	clientCmd.Flags().Int64Var(&bufferBytes, "buffer-bytes", 128*1024*1024, "Payload buffer reservation budget; excludes kernel cache and other process memory")
	clientCmd.Flags().StringVar(&sourcePath, "source", "", "Subdirectory of the remote server root")
	clientCmd.Flags().BoolVar(&durable, "durable", false, "Flush each copied file and parent directory to stable storage (slower)")
	clientCmd.Flags().BoolVar(&hardlinks, "hardlinks", true, "Preserve hardlinks")
	clientCmd.Flags().BoolVar(&xattr, "xattr", true, "Transfer xattrs")
	clientCmd.Flags().BoolVar(&checksum, "checksum", false, "Checksum files")
	clientCmd.Flags().BoolVar(&deleteOpt, "delete", false, "Delete extra local files (mirror)")
	clientCmd.Flags().IntVar(&parallelfile, "pfile", 64, "Number of parallel file IO operations")
	clientCmd.Flags().IntVar(&paralleldir, "pdir", 8, "Maximum concurrent directory listing requests (alphabetical traversal)")
	clientCmd.Flags().IntVar(&queuesize, "queuesize", 1024, "Incoming block queue size")
	clientCmd.Flags().IntVar(&transferblocksize, "blocksize", 64*1024, "Transfer/checksum block size")

	// Shutdown command
	var shutdownCmd = &cobra.Command{
		Use:   "shutdown [server:port]",
		Short: "Shutdown remote server",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) < 1 {
				fastsync.Logger.Fatal().Msgf("Please provide server address and port to connect to")
			}

			serveraddr := args[0]

			conn, err := net.Dial("tcp", serveraddr)
			if err != nil {
				fastsync.Logger.Fatal().Msgf("Error connecting to %s: %v", serveraddr, err)
			}
			var h codec.MsgpackHandle
			cconn := fastsync.CompressedReadWriteCloser(conn)
			rpcCodec := codec.GoRpc.ClientCodec(cconn, &h)
			rpcClient := rpc.NewClientWithCodec(rpcCodec)
			client := fastsync.NewClient()
			client.Password, err = readPasswordFile(passwordFile)
			if err != nil {
				fastsync.Logger.Fatal().Err(err).Msg("Invalid password file")
			}
			if err := client.Handshake(rpcClient); err != nil {
				fastsync.Logger.Fatal().Msgf("Error saying hello: %v", err)
			}
			fastsync.Logger.Info().Msg("Shutting down server")
			err = rpcClient.Call("Server.Shutdown", nil, nil)
			if err != nil {
				fastsync.Logger.Fatal().Msgf("Error shutting down: %v", err)
			}
			if err := rpcClient.Close(); err != nil {
				fastsync.Logger.Error().Msgf("Error closing RPC client: %v", err)
			}
			fastsync.Logger.Info().Msg("Server is shut down")
			os.Exit(0)
		},
	}

	rootCmd.AddCommand(serverCmd, clientCmd, shutdownCmd, newVerifyCommand())
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func autoProfile() {
	s := []metrics.Sample{
		{Name: "/cpu/classes/user:cpu-seconds"},
	}
	var lastvalue float64
	var loops, highmarks int
	var autoprofiling bool
	for {
		time.Sleep(time.Second)
		loops++
		metrics.Read(s)
		cpu := s[0].Value.Float64()
		relative := cpu - lastvalue
		if relative == 0 {
			continue
		}
		lastvalue = cpu

		percpu := relative / float64(runtime.NumCPU()) / float64(loops)
		if percpu > 0.9 {
			fastsync.Logger.Warn().Msg("CPU high instance detected")
			highmarks++
		} else if percpu < 0.6 {
			highmarks = 0
		}
		loops = 0
		fastsync.Logger.Warn().Msgf("CPU: %f, percpu %f", cpu, percpu)

		if highmarks > 15 && !autoprofiling {
			autoprofiling = true
			highmarks = 0
			fastsync.Logger.Warn().Msg("CPU high, auto profiling starting")
			f, err := fastsync.OpenFileNoFollow(fmt.Sprintf("cpu-autoprofile-%v.prof", time.Now()), os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0666)
			if err != nil {
				log.Fatal(err)
			}
			err = pprof.StartCPUProfile(f)
			if err != nil {
				fastsync.Logger.Fatal().Msgf("Can't start profiling: %v", err)
			}
			go func() {
				time.Sleep(time.Minute)
				pprof.StopCPUProfile()
				autoprofiling = false
				if err := f.Close(); err != nil {
					fastsync.Logger.Error().Msgf("Error closing CPU profile: %v", err)
				}
				fastsync.Logger.Warn().Msgf("CPU auto profiling stopped")
			}()
		}
	}
}
