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
	"github.com/spf13/pflag"
	"github.com/ugorji/go/codec"
)

func main() {
	// sync settings
	bind := pflag.String("bind", "0.0.0.0:7331", "Address to bind/connect to")
	hardlinks := pflag.Bool("hardlinks", true, "Preserve hardlinks")
	directory := pflag.String("directory", ".", "Directory to use as source or target")
	// transfer decision settings
	xattr := pflag.Bool("xattr", true, "Transfer xattrs")
	checksum := pflag.Bool("checksum", false, "Checksum files")
	delete := pflag.Bool("delete", false, "Delete extra local files (mirror)")
	// performance settings
	parallelfile := pflag.Int("pfile", 4096, "Number of parallel file IO operations")
	paralleldir := pflag.Int("pdir", 512, "Number of parallel dir scanning operations")
	queuesize := pflag.Int("queuesize", 65536, "Incoming block queue size")
	transferblocksize := pflag.Int("blocksize", 64*1024, "Transfer/checksum block size")

	// debugging etc
	loglevel := pflag.String("loglevel", "info", "Log level")
	cpuprofile := pflag.String("cpuprofile", "", "Write cpu profile to file (filename, use 'auto' to trigger auto profiling)")
	cpuprofilelength := pflag.Int("cpuprofilelength", 0, "Stop profiling after N seconds, 0 to profile until program terminates")
	transferstatsinterval := pflag.Int("statsinterval", 5, "Show transfer stats every N seconds, 0 to disable")
	queuestatsinterval := pflag.Int("queueinterval", 30, "Show internal queue sizes every N seconds, 0 to disable")
	ramlimit := pflag.Uint64("ramlimit", 0, "Abort if process uses more than this amount bytes of RAM")

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
		}()
	} else if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		err = pprof.StartCPUProfile(f)
		if err != nil {
			fastsync.Logger.Fatal().Msgf("Can't start profiling: %v", err)
		}
		if *cpuprofilelength > 0 {
			go func() {
				time.Sleep(time.Duration(*cpuprofilelength) * time.Second)
				pprof.StopCPUProfile()
				f.Close()
				fastsync.Logger.Warn().Msgf("CPU profiling stopped")
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
			fastsync.Logger.Fatal().Msgf("Error getting working directory: %v", err)
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
		fastsync.Logger.Fatal().Msgf("Invalid log level: %v", *loglevel)
	}
	fastsync.Logger = fastsync.Logger.Level(zll)

	if len(pflag.Args()) == 0 {
		fastsync.Logger.Fatal().Msg("Need command argument")
	}

	// register signal handler
	signals := make(chan os.Signal, 1)
	// signal.Notify(signals, os.Interrupt)

	switch strings.ToLower(pflag.Arg(0)) {
	case "server":
		rpcserver := rpc.NewServer()
		fastsyncserver := fastsync.NewServer()
		fastsyncserver.BasePath = *directory
		err := rpcserver.Register(fastsyncserver)
		if err != nil {
			fastsync.Logger.Fatal().Msgf("Error registering server object: %v", err)
		}

		listener, err := net.Listen("tcp", *bind)
		if err != nil {
			fastsync.Logger.Fatal().Msgf("Error binding listener: %v", err)
		}
		fastsync.Logger.Info().Msgf("Listening on %s", *bind)
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
		go func() {
			<-signals
			fastsyncserver.Shutdown(nil, nil)
		}()
		fastsyncserver.Wait()
	case "client", "shutdown":
		//RPC Communication (client side)
		c := fastsync.NewClient()
		c.BasePath = *directory
		c.PreserveHardlinks = *hardlinks
		c.ParallelDir = *paralleldir
		c.ParallelFile = *parallelfile
		c.QueueSize = *queuesize
		c.BlockSize = *transferblocksize
		c.AlwaysChecksum = *checksum
		c.Options.SendXattr = *xattr
		c.Delete = *delete

		conn, err := net.Dial("tcp", *bind)
		if err != nil {
			fastsync.Logger.Fatal().Msgf("Error connecting to %s: %v", *bind, err)
		}
		fastsync.Logger.Info().Msgf("Connected to %s", *bind)

		wconn := fastsync.NewPerformanceWrapper(conn, c.Perf.GetAtomicAdder(fastsync.RecievedOverWire), c.Perf.GetAtomicAdder(fastsync.SentOverWire))
		cconn := fastsync.CompressedReadWriteCloser(wconn)
		wcconn := fastsync.NewPerformanceWrapper(cconn, c.Perf.GetAtomicAdder(fastsync.RecievedBytes), c.Perf.GetAtomicAdder(fastsync.SentBytes))

		var h codec.MsgpackHandle
		rpcCodec := codec.GoRpc.ClientCodec(wcconn, &h)
		rpcClient := rpc.NewClientWithCodec(rpcCodec)

		if strings.ToLower(pflag.Arg(0)) == "shutdown" {
			fastsync.Logger.Info().Msg("Shutting down server")
			err := rpcClient.Call("Server.Shutdown", nil, nil)
			if err != nil {
				fastsync.Logger.Fatal().Msgf("Error shutting down: %v", err)
			}
			fastsync.Logger.Info().Msg("Server is shut down")
			os.Exit(0)
		}

		var totalhistory fastsync.PerformanceEntry

		fastsync.Logger.Info().Msgf("Client processing with up to %v incoming file blocks at %v bytes (RAM usage could be %v bytes or more)", c.ParallelFile, c.BlockSize, c.ParallelFile*c.BlockSize)

		if *transferstatsinterval > 0 {
			go func() {
				for !c.Done() {
					time.Sleep(time.Duration(*transferstatsinterval) * time.Second)
					lasthistory := c.Perf.NextHistory()
					totalhistory = totalhistory.Add(lasthistory)
					fastsync.Logger.Warn().Msgf("Wired %v/sec, transferred %v/sec, local read %v/sec, local write %v/sec, processed %v/sec, %v files/sec, %v dirs/sec",
						humanize.Bytes((lasthistory.Get(fastsync.SentOverWire)+lasthistory.Get(fastsync.RecievedOverWire))/uint64(*transferstatsinterval)),
						humanize.Bytes((lasthistory.Get(fastsync.SentBytes)+lasthistory.Get(fastsync.RecievedBytes))/uint64(*transferstatsinterval)),
						humanize.Bytes(lasthistory.Get(fastsync.ReadBytes)/uint64(*transferstatsinterval)), humanize.Bytes((lasthistory.Get(fastsync.WrittenBytes))/uint64(*transferstatsinterval)),
						humanize.Bytes((lasthistory.Get(fastsync.BytesProcessed))/uint64(*transferstatsinterval)),
						(lasthistory.Get(fastsync.FilesProcessed))/uint64(*transferstatsinterval),
						(lasthistory.Get(fastsync.DirectoriesProcessed))/uint64(*transferstatsinterval))
				}
			}()
		}

		if *queuestatsinterval > 0 {
			go func() {
				for !c.Done() {
					time.Sleep(time.Duration(*queuestatsinterval) * time.Second)
					inodecache, directorycache, files, stack := c.Stats()
					fastsync.Logger.Warn().Msgf("Inode cache %v, directory cache %v, file queue %v, directory queue %v",
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
							fastsync.Logger.Fatal().Msgf("Aborting due to excessive RAM consumption")
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

	default:
		fastsync.Logger.Fatal().Msgf("Invalid mode: %v", pflag.Arg(0))
	}
}
