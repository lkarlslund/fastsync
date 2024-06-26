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
	"github.com/rs/zerolog"
	"github.com/spf13/pflag"
	"github.com/ugorji/go/codec"
)

var logger zerolog.Logger

func main() {
	logger = zerolog.New(
		zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339},
	).With().Timestamp().Caller().Logger()

	// sync settings
	bind := pflag.String("bind", "0.0.0.0:7331", "Address to bind/connect to")
	hardlinks := pflag.Bool("hardlinks", true, "Preserve hardlinks")
	directory := pflag.String("directory", ".", "Directory to use as source or target")
	// transfer decision settings
	acl := pflag.Bool("acl", true, "Transfer ACLs")
	checksum := pflag.Bool("checksum", false, "Checksum files")
	delete := pflag.Bool("delete", false, "Delete extra local files (mirror)")
	// performance settings
	parallelfile := pflag.Int("pfile", 4096, "Number of parallel file IO operations")
	paralleldir := pflag.Int("pdir", 512, "Number of parallel dir scanning operations")
	transferblocksize := pflag.Int("blocksize", 128*1024, "Transfer/checksum block size")
	// debugging etc
	loglevel := pflag.String("loglevel", "info", "Log level")
	cpuprofile := pflag.String("cpuprofile", "", "Write cpu profile to file (filename, use 'auto' to trigger auto profiling)")
	cpuprofilelength := pflag.Int("cpuprofilelength", 0, "Stop profiling after N seconds, 0 to profile until program terminates")
	transferstatsinterval := pflag.Int("statsinterval", 5, "Show transfer stats every N seconds, 0 to disable")
	queuestatsinterval := pflag.Int("queueinterval", 30, "Show internal queue sizes every N seconds, 0 to disable")
	ramlimit := pflag.Int("ramlmit", 1*1024*1024*1024, "Abort if process uses more than this amount bytes of RAM")

	pflag.Parse()

	if *cpuprofile == "auto" {
		// auto profile
		go func() {
			// detect cpu usage
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
					logger.Warn().Msg("CPU high instance detected")
					highmarks++
				} else if percpu < 0.6 {
					highmarks = 0
				}
				loops = 0
				logger.Warn().Msgf("CPU: %f, percpu %f", cpu, percpu)

				if highmarks > 15 && !autoprofiling {
					autoprofiling = true
					highmarks = 0
					logger.Warn().Msg("CPU high, auto profiling starting")
					f, err := os.Create(fmt.Sprintf("cpu-autoprofile-%v.prof", time.Now()))
					if err != nil {
						log.Fatal(err)
					}
					err = pprof.StartCPUProfile(f)
					if err != nil {
						logger.Fatal().Msgf("Can't start profiling: %v", err)
					}
					go func() {
						time.Sleep(time.Minute)
						pprof.StopCPUProfile()
						autoprofiling = false
						f.Close()
						logger.Warn().Msgf("CPU auto profiling stopped")
					}()
				}
			}
		}()
	} else if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		err = pprof.StartCPUProfile(f)
		if err != nil {
			logger.Fatal().Msgf("Can't start profiling: %v", err)
		}
		if *cpuprofilelength > 0 {
			go func() {
				time.Sleep(time.Duration(*cpuprofilelength) * time.Second)
				pprof.StopCPUProfile()
				f.Close()
				logger.Warn().Msgf("CPU profiling stopped")
			}()
		} else {
			defer pprof.StopCPUProfile()
		}
	}

	var err error
	if *directory == "." {
		// Get current working directory as absolute path
		*directory, err = os.Getwd()
		if err != nil {
			logger.Fatal().Msgf("Error getting working directory: %v", err)
		}
	}

	var zll zerolog.Level
	switch strings.ToLower(*loglevel) {
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
		logger.Fatal().Msgf("Invalid log level: %v", *loglevel)
	}
	logger = logger.Level(zll)

	if len(pflag.Args()) == 0 {
		logger.Fatal().Msg("Need command argument")
	}

	// register signal handler
	signals := make(chan os.Signal, 1)
	// signal.Notify(signals, os.Interrupt)

	switch strings.ToLower(pflag.Arg(0)) {
	case "server":
		server := rpc.NewServer()
		serverobject := &Server{
			BasePath: *directory,
			ReadOnly: true,
			shutdown: make(chan struct{}),
		}
		err := server.Register(serverobject)
		if err != nil {
			logger.Fatal().Msgf("Error registering server object: %v", err)
		}

		listener, err := net.Listen("tcp", *bind)
		if err != nil {
			logger.Fatal().Msgf("Error binding listener: %v", err)
		}
		logger.Info().Msgf("Listening on %s", *bind)
		go func() {
			for {
				conn, err := listener.Accept()
				logger.Info().Msgf("Accepted connection from %v", conn.RemoteAddr())
				if err != nil {
					logger.Error().Msgf("Error accepting connection: %v", err)
					continue
				}
				wconn := NewPerformanceWrapper(conn, p.GetAtomicAdder(RecievedOverWire), p.GetAtomicAdder(SentOverWire))
				cconn := CompressedReadWriteCloser(wconn)
				wcconn := NewPerformanceWrapper(cconn, p.GetAtomicAdder(RecievedBytes), p.GetAtomicAdder(SentBytes))
				go func() {
					var h codec.MsgpackHandle
					server.ServeCodec(codec.GoRpc.ServerCodec(wcconn, &h))
					logger.Info().Msgf("Closed connection from %v", conn.RemoteAddr())
				}()
			}
		}()
		go func() {
			<-signals
			serverobject.Shutdown(nil, nil)
		}()
		serverobject.Wait()
	case "client", "shutdown":
		//RPC Communication (client side)
		conn, err := net.Dial("tcp", *bind)
		if err != nil {
			logger.Fatal().Msgf("Error connecting to %s: %v", *bind, err)
		}
		logger.Info().Msgf("Connected to %s", *bind)

		wconn := NewPerformanceWrapper(conn, p.GetAtomicAdder(RecievedOverWire), p.GetAtomicAdder(SentOverWire))
		cconn := CompressedReadWriteCloser(wconn)
		wcconn := NewPerformanceWrapper(cconn, p.GetAtomicAdder(RecievedBytes), p.GetAtomicAdder(SentBytes))

		var h codec.MsgpackHandle
		rpcCodec := codec.GoRpc.ClientCodec(wcconn, &h)
		rpcClient := rpc.NewClientWithCodec(rpcCodec)

		if strings.ToLower(pflag.Arg(0)) == "shutdown" {
			logger.Info().Msg("Shutting down server")
			err := rpcClient.Call("Server.Shutdown", nil, nil)
			if err != nil {
				logger.Fatal().Msgf("Error shutting down: %v", err)
			}
			logger.Info().Msg("Server is shut down")
			os.Exit(0)
		}

		c := NewClient()
		c.BasePath = *directory
		c.PreserveHardlinks = *hardlinks
		c.ParallelDir = *paralleldir
		c.ParallelFile = *parallelfile
		c.BlockSize = *transferblocksize
		c.AlwaysChecksum = *checksum
		c.SendACL = *acl
		c.Delete = *delete

		var totalhistory performanceentry

		if *transferstatsinterval > 0 {
			go func() {
				for !c.Done() {
					time.Sleep(time.Duration(*transferstatsinterval) * time.Second)
					lasthistory := p.NextHistory()
					totalhistory = totalhistory.Add(lasthistory)
					logger.Warn().Msgf("Wired %v/sec, transferred %v/sec, local read %v/sec, local write %v/sec, processed %v/sec, %v files/sec, %v dirs/sec",
						humanize.Bytes((lasthistory.counters[SentOverWire]+lasthistory.counters[RecievedOverWire])/uint64(*transferstatsinterval)),
						humanize.Bytes((lasthistory.counters[SentBytes]+lasthistory.counters[RecievedBytes])/uint64(*transferstatsinterval)),
						humanize.Bytes(lasthistory.counters[ReadBytes]/uint64(*transferstatsinterval)), humanize.Bytes((lasthistory.counters[WrittenBytes])/uint64(*transferstatsinterval)),
						humanize.Bytes((lasthistory.counters[BytesProcessed])/uint64(*transferstatsinterval)),
						(lasthistory.counters[FilesProcessed])/uint64(*transferstatsinterval),
						(lasthistory.counters[DirectoriesProcessed])/uint64(*transferstatsinterval))
				}
			}()
		}

		if *queuestatsinterval > 0 {
			go func() {
				for !c.Done() {
					time.Sleep(time.Duration(*queuestatsinterval) * time.Second)
					inodecache, directorycache, files, stack := c.Stats()
					logger.Warn().Msgf("Inode cache %v, directory cache %v, file queue %v, directory queue %v",
						inodecache, directorycache, files, stack)

					if *ramlimit > 0 {
						runtime.GC()
						var m runtime.MemStats
						runtime.ReadMemStats(&m)
						if m.Alloc > *ramlimit {
							// Using more than 4GB, wooot
							// Write debug memory info to file
							mp, _ := os.Create("/tmp/memprofile.pprof")
							pprof.WriteHeapProfile(mp)
							mp.Close()
							logger.Fatal().Msgf("Aborting due to absurd RAM consumption")
						}
					}
				}
			}()
		}

		go func() {
			<-signals
			c.Abort()
		}()
		err = c.Run(rpcClient)
		if err != nil {
			logger.Error().Msgf("Error running client: %v", err)
		}

		rpcClient.Close()

		lasthistory := p.NextHistory()
		totalhistory = totalhistory.Add(lasthistory)
		logger.Warn().Msgf("Final statistics")
		logger.Warn().Msgf("Wired %v, transferred %v, local read/write %v processed %v - %v files - %v dirs",
			humanize.Bytes(totalhistory.counters[SentOverWire]+totalhistory.counters[RecievedOverWire]),
			humanize.Bytes(totalhistory.counters[SentBytes]+totalhistory.counters[RecievedBytes]),
			humanize.Bytes(totalhistory.counters[ReadBytes]+totalhistory.counters[WrittenBytes]),
			humanize.Bytes(totalhistory.counters[BytesProcessed]),
			totalhistory.counters[FilesProcessed],
			totalhistory.counters[DirectoriesProcessed])
		logger.Warn().Msgf("Deleted %v", totalhistory.counters[EntriesDeleted])

	default:
		logger.Fatal().Msgf("Invalid mode: %v", pflag.Arg(0))
	}
}
