package main

import (
	"net"
	"net/rpc"
	"os"
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

	hardlinks := pflag.Bool("hardlinks", true, "Preserve hardlinks")
	directory := pflag.String("directory", ".", "Directory to use as source or target")
	checksum := pflag.Bool("checksum", false, "Checksum files")
	bind := pflag.String("bind", "0.0.0.0:7331", "Address to bind/connect to")
	parallelfile := pflag.Int("pfile", 4096, "Number of parallel file IO operations")
	paralleldir := pflag.Int("pdir", 512, "Number of parallel dir scanning operations")
	loglevel := pflag.String("loglevel", "info", "Log level")
	transferblocksize := pflag.Int("blocksize", 128*1024, "Transfer/checksum block size")

	transferstatsinterval := pflag.Int("statsinterval", 5, "Show transfer stats every N seconds, 0 to disable")
	queuestatsinterval := pflag.Int("queueinterval", 30, "Show internal queue sizes every N seconds, 0 to disable")

	pflag.Parse()

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

	switch strings.ToLower(pflag.Arg(0)) {
	case "server":
		server := rpc.NewServer()
		serverobject := &Server{
			BasePath: *directory,
			ReadOnly: true,
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
	case "client":
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

		c := NewClient()
		c.BasePath = *directory
		c.PreserveHardlinks = *hardlinks
		c.ParallelDir = *paralleldir
		c.ParallelFile = *parallelfile
		c.BlockSize = *transferblocksize
		c.AlwaysChecksum = *checksum

		if *transferstatsinterval > 0 {
			go func() {
				for !c.Done() {
					time.Sleep(time.Duration(*transferstatsinterval) * time.Second)
					lasthistory := p.NextHistory()
					logger.Info().Msgf("Wired %v/sec, transferred %v/sec, local read/write %v/sec processed %v/sec - %v files/sec - %v dirs/sec",
						humanize.Bytes((lasthistory.counters[SentOverWire]+lasthistory.counters[RecievedOverWire])/uint64(*transferstatsinterval)),
						humanize.Bytes((lasthistory.counters[SentBytes]+lasthistory.counters[RecievedBytes])/uint64(*transferstatsinterval)),
						humanize.Bytes((lasthistory.counters[ReadBytes]+lasthistory.counters[WrittenBytes])/uint64(*transferstatsinterval)),
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
					logger.Info().Msgf("Inode cache %v, directory cache %v, file queue %v, directory queue %v",
						inodecache, directorycache, files, stack)
				}
			}()
		}

		err = c.Run(rpcClient)
		if err != nil {
			logger.Error().Msgf("Error running client: %v", err)
		}

		rpcClient.Close()
	default:
		logger.Fatal().Msgf("Invalid mode: %v", pflag.Arg(0))
	}
}
