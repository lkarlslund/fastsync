package main

import (
	"io"
	"io/fs"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/lkarlslund/gonk"
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
	folder := pflag.String("folder", ".", "Folder to use as source or target")
	checksum := pflag.Bool("checksum", false, "Checksum files")
	bind := pflag.String("bind", "0.0.0.0:7331", "Address to bind/connect to")
	parallel := pflag.Int("parallel", 1024, "Number of parallel operations")
	loglevel := pflag.String("loglevel", "info", "Log level")
	pflag.Parse()

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
	logger.Level(zll)

	var h codec.MsgpackHandle

	if len(pflag.Args()) == 0 {
		logger.Fatal().Msg("Need command argument")
	}

	switch strings.ToLower(pflag.Arg(0)) {
	case "server":
		server := rpc.NewServer()
		serverobject := &Server{
			BasePath: *folder,
			ReadOnly: true,
		}
		err := server.Register(serverobject)
		if err != nil {
			panic(err)
		}

		listener, err := net.Listen("tcp", *bind)
		if err != nil {
			panic(err)
		}
		logger.Info().Msgf("Listening on %s", *bind)
		for {
			conn, err := listener.Accept()
			logger.Info().Msgf("Accepted connection from %v", conn.RemoteAddr())
			if err != nil {
				panic(err)
			}
			go func() {
				server.ServeCodec(codec.GoRpc.ServerCodec(CompressedReadWriteCloser(conn), &h))
				logger.Info().Msgf("Closed connection from %v", conn.RemoteAddr())
			}()
		}
	case "client":
		//RPC Communication (client side)
		conn, err := net.Dial("tcp", *bind)
		if err != nil {
			panic(err)
		}
		logger.Info().Msgf("Connected to %s", *bind)

		// Start the process
		queue := make(chan QueueItem, 32768)

		rpcCodec := codec.GoRpc.ClientCodec(CompressedReadWriteCloser(conn), &h)
		client := rpc.NewClientWithCodec(rpcCodec)

		var inodes gonk.Gonk[inodeinfo]

		var wg sync.WaitGroup
		var listfiles sync.WaitGroup

		for i := 0; i < *parallel; i++ {
			wg.Add(1)
			go func() {
				logger.Trace().Msg("Starting worker")
				for item := range queue {
					localpath := filepath.Join(*folder, item.Path)
					logger.Trace().Msgf("Processing queue item command %s for %s", item.Command, item.Path)
					switch item.Command {
					case "listfiles":
						var filelistresponse FileListResponse
						err := client.Call("Server.Listfiles", item.Path, &filelistresponse)
						logger.Trace().Msgf("Listfiles response: %v, err: %v", filelistresponse, err)
						if err != nil {
							logger.Error().Msgf("Error listing files in %v: %v", item.Path, err)
							continue
						}
						for _, fi := range filelistresponse.Files {
							logger.Trace().Msgf("Found file %s", fi.Name)
							if fi.IsDir {
								err = os.MkdirAll(filepath.Dir(localpath), 0755)
								if err != nil {
									logger.Error().Msgf("Error creating directory %v: %v", filepath.Dir(localpath), err)
									continue
								}

								listfiles.Add(1)
								queue <- QueueItem{
									Command: "listfiles",
									Path:    fi.Name,
								}
							} else {
								queue <- QueueItem{
									Command:  "processfile",
									Path:     fi.Name,
									FileInfo: &fi,
								}
							}
						}
						listfiles.Done()
					case "processfile":
						logger.Debug().Msgf("Processing file %s", localpath)

						remotefi := item.FileInfo

						exists := true    // do we have it locally
						copyfile := false // do we need to copy it
						changed := false  // do we need to update owner etc.

						localfi, err := os.Stat(localpath)
						if err != nil {
							if os.IsNotExist(err) {
								logger.Trace().Msgf("File %s does not exist", localpath)
								exists = false
							} else {
								panic(err)
							}
						}

						// var localstat *syscall.Stat_t
						if exists {
							var ok bool
							localstat, ok := localfi.Sys().(*syscall.Stat_t)
							if !ok {
								logger.Fatal().Msgf("Could not obtain local stat for %s", localpath)
							}

							if localfi.Mode()&fs.ModeType != remotefi.Mode&fs.ModeType {
								logger.Debug().Msgf("File %s is indicating type change, unlinking", localpath)
								err = os.Remove(localpath)
								if err != nil {
									logger.Error().Msgf("Error unlinking %s: %v", localpath, err)
								}
								copyfile = true
								exists = false
							} else {
								localmtime := time.Unix(localstat.Mtim.Sec, localstat.Mtim.Nsec)
								if *checksum || localfi.Size() != remotefi.Size || localfi.Mode() != remotefi.Mode ||
									localstat.Uid != remotefi.Owner || localstat.Gid != remotefi.Group ||
									localmtime.Compare(remotefi.ModifyTime) != 0 {
									logger.Debug().Msgf("File %s is indicating changes", localpath)
									changed = true
									copyfile = true
								}
							}
						} else {
							// Not existing
							copyfile = true
							if *hardlinks && item.FileInfo.Nlink > 1 {
								if ini, found := inodes.Load(inodeinfo{
									inode: item.FileInfo.Inode,
								}); found {
									logger.Debug().Msgf("Hardlinking %s to %s", localpath, ini.path)
									err = os.Link(filepath.Join(ini.path), localpath)
									if err != nil {
										panic(err)
									}
									copyfile = false
									changed = true

									// handle inode counters
									nukeit := false
									inodes.AtomicMutate(ini, func(i *inodeinfo) {
										rest := atomic.AddInt32(&i.remaining, -1)
										if rest == 0 {
											nukeit = true
										}
									}, false)
									if nukeit {
										// No more references, free up some memory
										logger.Trace().Msgf("No more references to %s, removing it from inode cache", ini.path)
										inodes.Delete(ini)
									}
								}
							}
						}

						if copyfile {
							// file exists but is different, copy it
							logger.Trace().Msgf("Processing blocks for %s", item.Path)
							var localfile *os.File
							var existingsize int64
							if exists {
								localfile, err = os.OpenFile(localpath, os.O_RDWR, fs.FileMode(remotefi.Mode))
								if err != nil {
									logger.Error().Msgf("Error opening existing local file %s: %v", localpath, err)
									continue
								}
								fi, _ := os.Stat(localpath)
								existingsize = fi.Size()
							} else {
								if remotefi.Mode&fs.ModeDevice != 0 {
									err = syscall.Mknod(localpath, uint32(remotefi.Mode), int(remotefi.Rdev))
									if err != nil {
										logger.Error().Msgf("Error creating char device %s: %v", localpath, err)
									}
									continue
								} else if remotefi.Mode&fs.ModeSymlink != 0 {
									err = os.Symlink(remotefi.LinkTo, localpath)
									if err != nil {
										logger.Error().Msgf("Error creating symlink %s: %v", localpath, err)
									}
									continue
								} else {
									localfile, err = os.Create(localpath)
								}
							}
							if err != nil {
								logger.Error().Msgf("Error accessing local file %s: %v", localpath, err)
								continue
							}

							if *hardlinks && item.FileInfo.Nlink > 1 {
								logger.Trace().Msgf("Saving remote inode number to cache for file %s", item.Path)
								inodes.Store(inodeinfo{
									inode:     item.FileInfo.Inode,
									path:      localpath,
									remaining: int32(remotefi.Nlink - 1),
								})
							}

							err = client.Call("Server.Open", item.Path, nil)
							if err != nil {
								panic(err)
							}

							for i := int64(0); i < remotefi.Size; i += 1000000 {
								// Read the chunk
								length := int64(1000000)
								if i+length > remotefi.Size {
									length = remotefi.Size - i
								}
								chunkArgs := GetChunkArgs{
									Path:   item.Path,
									Offset: uint64(i),
									Size:   uint64(length),
								}
								if exists && i+length <= existingsize {
									var hash uint64
									err = client.Call("Server.ChecksumChunk", chunkArgs, &hash)
									if err != nil {
										panic(err)
									}
									localdata := make([]byte, length)
									n, err := localfile.ReadAt(localdata, i)
									if err != nil {
										logger.Error().Msgf("Error reading existing local file %s: %v", localpath, err)
										continue
									}
									if n == int(length) {
										localhash := xxhash.Sum64(localdata)
										logger.Trace().Msgf("Checksum for file %s chunk at %d is %X, remote is %X", item.Path, i, localhash, hash)
										if localhash == hash {
											continue // Block matches
										}
									}
								}

								var data []byte
								logger.Trace().Msgf("Transferring file %s chunk at %d", item.Path, i)
								err = client.Call("Server.GetChunk", chunkArgs, &data)
								if err != nil {
									panic(err)
								}
								_, err = localfile.Seek(i, io.SeekStart)
								if err != nil {
									panic(err)
								}
								n, err := localfile.Write(data)
								if err != nil {
									panic(err)
								}
								if n != int(length) {
									logger.Fatal().Msgf("Wrote %v bytes but expected to write %v", n, length)
									panic(err)
								}
								changed = true
							}
							localfile.Close()
							err = client.Call("Server.Close", item.Path, nil)
							if err != nil {
								panic(err)
							}
						}

						if changed {
							logger.Debug().Msgf("Updating metadata for %s", item.Path)
							err = os.Chown(localpath, int(remotefi.Owner), int(remotefi.Group))
							if err != nil {
								panic(err)
							}
							err = os.Chmod(localpath, fs.FileMode(remotefi.Mode))
							if err != nil {
								panic(err)
							}
							err = os.Chtimes(localpath, remotefi.AccessTime, remotefi.ModifyTime)
							if err != nil {
								panic(err)
							}
						}
					default:
						logger.Fatal().Msgf("Invalid internal command: %v", item.Command)
					}
				}
				logger.Trace().Msg("Shutting down worker")
				wg.Done()
			}()
		}

		listfiles.Add(1)
		logger.Debug().Msg("Requesting list of files in / from remote")
		queue <- QueueItem{
			Command: "listfiles",
			Path:    "/",
		}

		listfiles.Wait()
		close(queue)

		wg.Wait()

		client.Close()
	default:
		logger.Fatal().Msgf("Invalid mode: %v", pflag.Arg(0))
	}
}
