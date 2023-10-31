package main

import (
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

	wunix "github.com/akihirosuda/x-sys-unix-auto-eintr"
	"github.com/cespare/xxhash/v2"
	"github.com/joshlf/go-acl"
	"github.com/lkarlslund/gonk"
	"github.com/rs/zerolog"
	"github.com/spf13/pflag"
	"github.com/ugorji/go/codec"
	unix "golang.org/x/sys/unix"
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

	var err error
	if *folder == "." {
		*folder, err = os.Getwd()
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
			go func() {
				server.ServeCodec(codec.GoRpc.ServerCodec(CompressedReadWriteCloser(conn), &h))
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
							localpath = filepath.Join(*folder, fi.Name)
							logger.Trace().Msgf("Found file %s", fi.Name)
							if fi.IsDir {
								logger.Trace().Msgf("Creating directory %s", localpath)
								err = os.MkdirAll(filepath.Join(*folder, fi.Name), 0755)
								if err != nil {
									logger.Error().Msgf("Error creating directory %v: %v", localpath, err)
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
									FileInfo: fi,
								}
							}
						}
						listfiles.Done()
					case "processfile":
						logger.Debug().Msgf("Processing file %s", localpath)

						remotefi := item.FileInfo

						exists := true // do we have it locally
						create_file := false
						copy_verify_file := false // do we need to copy it
						apply_attributes := false // do we need to update owner etc.

						localfi, err := os.Lstat(localpath)
						if err != nil {
							if os.IsNotExist(err) {
								logger.Trace().Msgf("File %s does not exist", localpath)
								exists = false
							} else {
								logger.Error().Msgf("Error getting stat stat for local path %s: %v", localpath, err)
								continue
							}
						}

						// var localstat *syscall.Stat_t
						if exists {
							var ok bool
							localstat, ok := localfi.Sys().(*syscall.Stat_t)
							if !ok {
								logger.Error().Msgf("Could not obtain local native stat for %s", localpath)
								continue
							}

							if localstat.Mode&^uint32(os.ModePerm) != remotefi.Permissions&^uint32(os.ModePerm) {
								logger.Debug().Msgf("File %s is indicating type change from %X to %X, unlinking", localpath, localstat.Mode&^uint32(os.ModePerm), remotefi.Permissions&^uint32(os.ModePerm))
								err = os.Remove(localpath)
								if err != nil {
									logger.Error().Msgf("Error unlinking %s: %v", localpath, err)
									continue
								}

								create_file = true
								apply_attributes = true
								exists = false
							} else {
								if localfi.Size() > remotefi.Size && remotefi.Mode&fs.ModeSymlink == 0 {
									logger.Debug().Msgf("File %s is indicating size change from %v to %v, truncating", localpath, localfi.Size(), remotefi.Size)
									err = os.Truncate(localpath, int64(remotefi.Size))
									if err != nil {
										logger.Error().Msgf("Error truncating %s to %v bytes to match remote: %v", localpath, remotefi.Size, err)
										continue
									}
									apply_attributes = true
								}

								if localstat.Mtim.Nano() != remotefi.Mtim.Nano() {
									logger.Debug().Msgf("File %s is indicating time change from %v to %v, applying attribute changes", localpath, time.Unix(0, localstat.Mtim.Nano()), time.Unix(0, remotefi.Mtim.Nano()))
									apply_attributes = true
								}

								if localfi.Mode().Perm() != remotefi.Mode.Perm() || localstat.Uid != remotefi.Owner || localstat.Gid != remotefi.Group {
									logger.Debug().Msgf("File %s is indicating permissions changes, applying attribute changes", localpath)
									apply_attributes = true
								}

								if remotefi.Size > 0 && remotefi.Mode&fs.ModeSymlink == 0 && (apply_attributes || *checksum) {
									logger.Debug().Msgf("File %s is indicating size changes, remote size is %v", localpath, remotefi.Size)
									copy_verify_file = true
								}
							}
						} else {
							// Not existing
							if !*hardlinks || item.FileInfo.Nlink == 1 {
								create_file = true
								if remotefi.Size > 0 {
									copy_verify_file = true
								}
								apply_attributes = true
							} else {
								if ini, found := inodes.Load(inodeinfo{
									inode: item.FileInfo.Inode,
								}); found {
									err = os.Link(filepath.Join(ini.path), localpath)
									if err != nil {
										logger.Error().Msgf("Error hardlinking %s to %s: %v", localpath, ini.path, err)
										continue
									}
									create_file = false
									copy_verify_file = false
									apply_attributes = true

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
						transfersuccess := true

						var localfile *os.File
						if create_file {
							if remotefi.Mode&fs.ModeDevice != 0 {
								if remotefi.Mode&fs.ModeCharDevice != 0 {
									err = syscall.Mknod(localpath, syscall.S_IFCHR, int(remotefi.Rdev))
									if err != nil {
										logger.Error().Msgf("Error creating char device %s: %v", localpath, err)
										continue
									}
								} else {
									// Block device
									err = syscall.Mknod(localpath, syscall.S_IFBLK, int(remotefi.Rdev))
									if err != nil {
										logger.Error().Msgf("Error creating block device %s: %v", localpath, err)
										continue
									}
								}
							} else if remotefi.Mode&fs.ModeSymlink != 0 {
								err = syscall.Symlink(remotefi.LinkTo, localpath)
								if err != nil {
									logger.Error().Msgf("Error creating symlink from %s to %s: %v", localpath, remotefi.LinkTo, err)
									continue
								}
							} else {
								localfile, err = os.Create(localpath)
								if err != nil {
									logger.Error().Msgf("Error creating local file %s: %v", localpath, err)
									continue
								}
							}
						}

						if copy_verify_file {
							// file exists but is different, copy it
							logger.Trace().Msgf("Processing blocks for %s", item.Path)
							var existingsize int64

							// Open file if we didn't create it earlier
							if localfile == nil {
								localfile, err = os.OpenFile(localpath, os.O_RDWR, fs.FileMode(remotefi.Mode))
								if err != nil {
									logger.Error().Msgf("Error opening existing local file %s: %v", localpath, err)
									continue
								}
								fi, _ := os.Stat(localpath)
								existingsize = fi.Size()
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
								logger.Error().Msgf("Error opening remote file %s: %v", item.Path, err)
								logger.Error().Msgf("Item fileinfo: %+v", item.FileInfo)
								break
							}

							for i := int64(0); i < remotefi.Size; i += 1000000 {
								if !transfersuccess {
									break // we couldn't open the remote file
								}

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
										logger.Error().Msgf("Error getting remote checksum for file %s chunk at %d: %v", item.Path, i, err)
										transfersuccess = false
									}
									localdata := make([]byte, length)
									n, err := localfile.ReadAt(localdata, i)
									if err != nil {
										logger.Error().Msgf("Error reading existing local file %s chunk at %d: %v", localpath, i, err)
										transfersuccess = false
										break
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
									logger.Error().Msgf("Error transferring file %s chunk at %d: %v", item.Path, i, err)
									transfersuccess = false
									break
								}
								n, err := localfile.WriteAt(data, i)
								if err != nil {
									logger.Error().Msgf("Error writing to local file %s chunk at %d: %v", localpath, i, err)
									transfersuccess = false
									break
								}
								if n != int(length) {
									logger.Error().Msgf("Wrote %v bytes but expected to write %v", n, length)
									transfersuccess = false
									break
								}
								apply_attributes = true
							}
							err = client.Call("Server.Close", item.Path, nil)
							if err != nil {
								logger.Error().Msgf("Error closing remote file %s: %v", item.Path, err)
							}
						}
						if localfile != nil {
							localfile.Close()
						}

						if apply_attributes && transfersuccess {
							logger.Debug().Msgf("Updating metadata for %s", item.Path)
							err = os.Lchown(localpath, int(remotefi.Owner), int(remotefi.Group))
							if err != nil {
								logger.Error().Msgf("Error changing owner for %s: %v", localpath, err)
							}
							if remotefi.Mode&fs.ModeSymlink == 0 {
								err = os.Chmod(localpath, fs.FileMode(remotefi.Permissions))
								if err != nil {
									logger.Error().Msgf("Error changing mode for %s: %v", localpath, err)
								}
								err = acl.Set(localpath, remotefi.ACL)
								if err != nil {
									logger.Error().Msgf("Error setting ACL for %s: %v", localpath, err)
								}
							}

							err = wunix.UtimesNanoAt(unix.AT_FDCWD, localpath, []unix.Timespec{unix.Timespec(remotefi.Atim), unix.Timespec(remotefi.Mtim)}, unix.AT_SYMLINK_NOFOLLOW)
							if err != nil {
								logger.Error().Msgf("Error changing times for %s: %v", localpath, err)
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
