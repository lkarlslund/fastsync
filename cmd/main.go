package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"runtime"
	"runtime/metrics"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/lkarlslund/fastsync"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"github.com/ugorji/go/codec"
)

var (
	// General/root options
	directory        string
	loglevel         string
	cpuprofile       string
	cpuprofilelength int
	ramlimit         uint64
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
			fastsync.Logger = fastsync.Logger.Level(zll)

			// CPU profiling setup
			if cpuprofile == "auto" {
				go autoProfile()
			} else if cpuprofile != "" {
				f, err := os.Create(cpuprofile)
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
						f.Close()
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
	rootCmd.PersistentFlags().StringVar(&directory, "directory", ".", "Directory to use as source or target")
	rootCmd.PersistentFlags().StringVar(&loglevel, "loglevel", "info", "Log level")
	rootCmd.PersistentFlags().StringVar(&cpuprofile, "cpuprofile", "", "Write cpu profile to file (filename, use 'auto' to trigger auto profiling)")
	rootCmd.PersistentFlags().IntVar(&cpuprofilelength, "cpuprofilelength", 0, "Stop profiling after N seconds, 0 to profile until program terminates")
	rootCmd.PersistentFlags().Uint64Var(&ramlimit, "ramlimit", 0, "Abort if process uses more than this amount bytes of RAM")

	// Server command
	var bind string
	var serverCmd = &cobra.Command{
		Use:   "server",
		Short: "Run as server",
		Run: func(cmd *cobra.Command, args []string) {
			rpcserver := rpc.NewServer()
			fastsyncserver := fastsync.NewServer()
			fastsyncserver.BasePath = directory
			err := rpcserver.Register(fastsyncserver)
			if err != nil {
				fastsync.Logger.Fatal().Msgf("Error registering server object: %v", err)
			}

			listener, err := net.Listen("tcp", bind)
			if err != nil {
				fastsync.Logger.Fatal().Msgf("Error binding listener: %v", err)
			}
			fastsync.Logger.Info().Msgf("Listening on %s", bind)
			go func() {
				for {
					conn, err := listener.Accept()
					fastsync.Logger.Info().Msgf("Accepted connection from %v", conn.RemoteAddr())
					if err != nil {
						fastsync.Logger.Error().Msgf("Error accepting connection: %v", err)
						continue
					}
					wconn := fastsync.NewPerformanceWrapper(conn, fastsyncserver.Perf.GetAtomicAdder(fastsync.RecievedOverWire), fastsyncserver.Perf.GetAtomicAdder(fastsync.SentOverWire))
					cconn := fastsync.CompressedReadWriteCloser(wconn)
					wcconn := fastsync.NewPerformanceWrapper(cconn, fastsyncserver.Perf.GetAtomicAdder(fastsync.RecievedBytes), fastsyncserver.Perf.GetAtomicAdder(fastsync.SentBytes))
					go func() {
						var h codec.MsgpackHandle
						rpcserver.ServeCodec(codec.GoRpc.ServerCodec(wcconn, &h))
						fastsync.Logger.Info().Msgf("Closed connection from %v", conn.RemoteAddr())
					}()
				}
			}()
			fastsyncserver.Wait()
		},
	}
	serverCmd.Flags().StringVar(&bind, "bind", "0.0.0.0:7331", "Address to bind to")

	// Client command
	var (
		hardlinks             bool
		xattr                 bool
		checksum              bool
		deleteOpt             bool
		parallelfile          int
		paralleldir           int
		queuesize             int
		transferblocksize     int
		transferstatsinterval int
		queuestatsinterval    int
	)
	var clientCmd = &cobra.Command{
		Use:   "client",
		Short: "Run as client",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) < 1 {
				fastsync.Logger.Fatal().Msgf("Please provide server address and port to connect to")
			}
			serveraddr := args[0]

			c := fastsync.NewClient()
			c.BasePath = directory
			c.PreserveHardlinks = hardlinks
			c.ParallelDir = paralleldir
			c.ParallelFile = parallelfile
			c.QueueSize = queuesize
			c.BlockSize = transferblocksize
			c.AlwaysChecksum = checksum
			c.Options.SendXattr = xattr
			c.Delete = deleteOpt

			conn, err := net.Dial("tcp", serveraddr)
			if err != nil {
				fastsync.Logger.Fatal().Msgf("Error connecting to %s: %v", bind, err)
			}
			fastsync.Logger.Info().Msgf("Connected to %s", bind)

			wconn := fastsync.NewPerformanceWrapper(conn, c.Perf.GetAtomicAdder(fastsync.RecievedOverWire), c.Perf.GetAtomicAdder(fastsync.SentOverWire))
			cconn := fastsync.CompressedReadWriteCloser(wconn)
			wcconn := fastsync.NewPerformanceWrapper(cconn, c.Perf.GetAtomicAdder(fastsync.RecievedBytes), c.Perf.GetAtomicAdder(fastsync.SentBytes))

			var h codec.MsgpackHandle
			rpcCodec := codec.GoRpc.ClientCodec(wcconn, &h)
			rpcClient := rpc.NewClientWithCodec(rpcCodec)

			var totalhistory fastsync.PerformanceEntry

			fastsync.Logger.Info().Msgf("Client processing with up to %v incoming file blocks at %v bytes (RAM usage could be %v bytes or more)", c.ParallelFile, c.BlockSize, c.ParallelFile*c.BlockSize)

			statsCh := make(chan stats, 10)
			go showStatsTUI(statsCh)
			go func() {
				ticker := time.NewTicker(time.Second)
				defer ticker.Stop()
				for !c.Done() {
					<-ticker.C

					lasthistory := c.Perf.NextHistory()
					totalhistory = totalhistory.Add(lasthistory)
					inodecache, directorycache, files, stack := c.Stats()

					statsCh <- stats{
						startTime:      time.Now(),
						performance:    lasthistory,
						inodecache:     inodecache,
						directorycache: directorycache,
						files:          files,
						stack:          stack,
					}
				}
			}()

			err = c.Run(rpcClient)
			if err != nil {
				fastsync.Logger.Error().Msgf("Error running client: %v", err)
			}

			rpcClient.Close()

			lasthistory := c.Perf.NextHistory()
			totalhistory = totalhistory.Add(lasthistory)

			fastsync.Logger.Warn().Msgf("Final statistics")
			fastsync.Logger.Warn().Msgf("Wired %v, transferred %v, local read/write %v processed %v - %v files - %v dirs",
				humanize.Bytes(totalhistory.Get(fastsync.SentOverWire)+totalhistory.Get(fastsync.RecievedOverWire)),
				humanize.Bytes(totalhistory.Get(fastsync.SentBytes)+totalhistory.Get(fastsync.RecievedBytes)),
				humanize.Bytes(totalhistory.Get(fastsync.ReadBytes)+totalhistory.Get(fastsync.WrittenBytes)),
				humanize.Bytes(totalhistory.Get(fastsync.BytesProcessed)),
				totalhistory.Get(fastsync.FilesProcessed),
				totalhistory.Get(fastsync.DirectoriesProcessed))
			fastsync.Logger.Warn().Msgf("Deleted %v", totalhistory.Get(fastsync.EntriesDeleted))
		},
	}
	clientCmd.Flags().IntVar(&transferstatsinterval, "statsinterval", 5, "Show transfer stats every N seconds, 0 to disable")
	clientCmd.Flags().IntVar(&queuestatsinterval, "queueinterval", 30, "Show internal queue sizes every N seconds, 0 to disable")
	clientCmd.Flags().BoolVar(&hardlinks, "hardlinks", true, "Preserve hardlinks")
	clientCmd.Flags().BoolVar(&xattr, "xattr", true, "Transfer xattrs")
	clientCmd.Flags().BoolVar(&checksum, "checksum", false, "Checksum files")
	clientCmd.Flags().BoolVar(&deleteOpt, "delete", false, "Delete extra local files (mirror)")
	clientCmd.Flags().IntVar(&parallelfile, "pfile", 4096, "Number of parallel file IO operations")
	clientCmd.Flags().IntVar(&paralleldir, "pdir", 512, "Number of parallel dir scanning operations")
	clientCmd.Flags().IntVar(&queuesize, "queuesize", 65536, "Incoming block queue size")
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
				fastsync.Logger.Fatal().Msgf("Error connecting to %s: %v", bind, err)
			}
			var h codec.MsgpackHandle
			rpcCodec := codec.GoRpc.ClientCodec(conn, &h)
			rpcClient := rpc.NewClientWithCodec(rpcCodec)
			fastsync.Logger.Info().Msg("Shutting down server")
			err = rpcClient.Call("Server.Shutdown", nil, nil)
			if err != nil {
				fastsync.Logger.Fatal().Msgf("Error shutting down: %v", err)
			}
			fastsync.Logger.Info().Msg("Server is shut down")
			os.Exit(0)
		},
	}

	rootCmd.AddCommand(serverCmd, clientCmd, shutdownCmd)
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
			f, err := os.Create(fmt.Sprintf("cpu-autoprofile-%v.prof", time.Now()))
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
				f.Close()
				fastsync.Logger.Warn().Msgf("CPU auto profiling stopped")
			}()
		}
	}
}
