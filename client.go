package main

import (
	"io/fs"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/lkarlslund/gonk"
	unix "golang.org/x/sys/unix"
)

type inodeinfo struct {
	inode     uint64
	path      string
	remaining int32
}

func (i inodeinfo) LessThan(i2 inodeinfo) bool {
	return i.inode < i2.inode
}

type dirinfo struct {
	name      string
	atim      syscall.Timespec
	mtim      syscall.Timespec
	remaining int32
}

func (f dirinfo) LessThan(f2 dirinfo) bool {
	return f.name < f2.name
}

type Client struct {
	BasePath string

	AlwaysChecksum            bool
	ParallelFile, ParallelDir int
	PreserveHardlinks         bool
	BlockSize                 int

	done bool

	dirWorkerWG, fileWorkerWG sync.WaitGroup

	filequeue   chan FileInfo
	inodes      gonk.Gonk[inodeinfo]
	dircache    gonk.Gonk[dirinfo]
	dirstack    *stack[FileInfo]
	dirqueuein  chan<- FileInfo
	dirqueueout <-chan FileInfo
}

func NewClient() *Client {
	c := &Client{
		ParallelFile:      4096,
		ParallelDir:       512,
		PreserveHardlinks: true,
		BlockSize:         128 * 1024,
	}

	return c
}

func (c *Client) Run(client *rpc.Client) error {
	// Start the process

	var deletedinodes int

	var listfilesActive sync.WaitGroup

	c.dirstack, c.dirqueueout, c.dirqueuein = NewStack[FileInfo](c.ParallelDir*2, 8)
	c.filequeue = make(chan FileInfo, c.ParallelFile*16)

	for i := 0; i < c.ParallelDir; i++ {
		c.dirWorkerWG.Add(1)
		go func() {
			logger.Trace().Msg("Starting directory worker")
			for item := range c.dirqueueout {
				logger.Trace().Msgf("Processing directory queue item for %s", item.Name)

				var filelistresponse FileListResponse
				err := client.Call("Server.List", item.Name, &filelistresponse)
				logger.Trace().Msgf("Listfiles response: %v, err: %v", filelistresponse, err)
				if err != nil {
					logger.Error().Msgf("Error listing files in %v: %v", item.Name, err)
					continue
				}
				var filecount int
				for _, remotefi := range filelistresponse.Files {
					if !remotefi.IsDir {
						filecount++
					}
				}
				var directoryfound bool
				c.dircache.AtomicMutate(dirinfo{
					name: item.Name,
				}, func(f *dirinfo) {
					atomic.StoreInt32(&f.remaining, int32(filecount))
					directoryfound = true
				}, false)
				if !directoryfound {
					logger.Error().Msgf("directory %v not found in directory cache", filelistresponse.ParentDirectory)
				}

				// queue files first
				for _, remotefi := range filelistresponse.Files {
					if !remotefi.IsDir {
						logger.Trace().Msgf("Queueing file %s", remotefi.Name)
						c.filequeue <- remotefi
					}
				}

				// queue directories second
				for _, remotefi := range filelistresponse.Files {
					if remotefi.IsDir {
						localpath := filepath.Join(c.BasePath, remotefi.Name)
						// logger.Trace().Msgf("Queueing directory %s", remotefi.Name)
						// check if directory exists
						localfi, err := os.Stat(localpath)
						created := false
						if err != nil {
							logger.Trace().Msgf("Creating directory %s", localpath)
							err = os.MkdirAll(localpath, 0755)
							if err != nil {
								logger.Error().Msgf("Error creating directory %v: %v", localpath, err)
								continue
							}
							localfi, err = os.Stat(localpath)
							if err != nil {
								logger.Error().Msgf("Error getting stat for newly created directory %v: %v", localpath, err)
								continue
							}
							created = true
						}
						localstat, ok := localfi.Sys().(*syscall.Stat_t)
						_, localmtim, _ := getAMtime(*localstat)
						if created || ok && localmtim != remotefi.Mtim {
							err = unix.UtimesNanoAt(unix.AT_FDCWD, localpath, []unix.Timespec{unix.Timespec(remotefi.Atim), unix.Timespec(remotefi.Mtim)}, unix.AT_SYMLINK_NOFOLLOW)
							if err != nil {
								logger.Error().Msgf("Error setting times for directory %v: %v", localpath, err)
							}
						}
						if created || ok && uint32(localstat.Mode)&^uint32(os.ModePerm) != remotefi.Permissions&^uint32(os.ModePerm) {
							err = unix.Chmod(localpath, remotefi.Permissions)
							if err != nil {
								logger.Error().Msgf("Error setting permissions for directory %v: %v", localpath, err)
							}
						}

						logger.Trace().Msgf("Queueing directory %v", remotefi.Name)
						listfilesActive.Add(1)
						c.dircache.Store(dirinfo{
							name:      remotefi.Name,
							atim:      remotefi.Atim,
							mtim:      remotefi.Mtim,
							remaining: -1, // we don't know yet
						})
						c.dirqueuein <- remotefi
					}
				}
				p.Add(DirectoriesProcessed, 1)
				listfilesActive.Done()
			}
			logger.Trace().Msg("Shutting down directory worker")
			c.dirWorkerWG.Done()
		}()
	}

	for i := 0; i < c.ParallelFile; i++ {
		c.fileWorkerWG.Add(1)
		go func() {
			logger.Trace().Msg("Starting file worker")
			for remotefi := range c.filequeue {
				localpath := filepath.Join(c.BasePath, remotefi.Name)
				logger.Trace().Msgf("Processing file %s", localpath)

				create_file := false
				copy_verify_file := false // do we need to copy it
				apply_attributes := false // do we need to update owner etc.

				remaininghardlinks := int32(-1) // not relevant
				if c.PreserveHardlinks && remotefi.Nlink > 1 {
					logger.Trace().Msgf("Saving/updating remote inode number %v to cache for file %s with %d hardlinks", remotefi.Inode, remotefi.Name, remotefi.Nlink)
					c.inodes.AtomicMutate(inodeinfo{
						inode:     remotefi.Inode,
						path:      localpath,
						remaining: int32(remotefi.Nlink),
					}, func(i *inodeinfo) {
						remaininghardlinks = atomic.AddInt32(&i.remaining, -1)
						if remaininghardlinks == int32(remotefi.Nlink-1) {
							logger.Trace().Msgf("Added file %s to inode cache", remotefi.Name)
						}
					}, true)
				}

				localfi, err := PathToFileInfo(localpath)
				if err != nil {
					if os.IsNotExist(err) {
						logger.Debug().Msgf("File %s does not exist", localpath)
						localfi.Name = localpath
						create_file = true
					} else {
						logger.Error().Msgf("Error getting fileinfo for local path %s: %v", localpath, err)
						continue
					}
				}

				if !create_file {
					// if it's a hardlinked file, check that it's linked correctly
					if remotefi.Nlink > 1 && c.PreserveHardlinks {
						if ini, found := c.inodes.Load(inodeinfo{
							inode: remotefi.Inode,
						}); found {
							otherlocalfi, err := PathToFileInfo(ini.path)
							if err != nil {
								logger.Error().Msgf("Error getting hardlink stat for local path %s: %v", localpath, err)
								continue
							}

							if localfi.Inode != otherlocalfi.Inode {
								logger.Debug().Msgf("Hardlink %s and %s have different inodes but should match, unlinking file", localpath, ini.path)
								err = os.Remove(localpath)
								if err != nil {
									logger.Error().Msgf("Error unlinking %s: %v", localpath, err)
									continue
								}
								create_file = true
							}
						}
					}

					if !create_file && uint32(localfi.Mode)&^uint32(os.ModePerm) != remotefi.Permissions&^uint32(os.ModePerm) {
						logger.Debug().Msgf("File %s is indicating type change from %X to %X, unlinking", localpath, uint32(localfi.Mode)&^uint32(os.ModePerm), remotefi.Permissions&^uint32(os.ModePerm))
						err = os.Remove(localpath)
						if err != nil {
							logger.Error().Msgf("Error unlinking %s: %v", localpath, err)
							continue
						}

						create_file = true
					}

					if !create_file { // still exists
						if localfi.Size > remotefi.Size && remotefi.Mode&fs.ModeSymlink == 0 {
							logger.Debug().Msgf("File %s is indicating size change from %v to %v, truncating", localpath, localfi.Size, remotefi.Size)
							err = os.Truncate(localpath, int64(remotefi.Size))
							if err != nil {
								logger.Error().Msgf("Error truncating %s to %v bytes to match remote: %v", localpath, remotefi.Size, err)
								continue
							}
							apply_attributes = true
						}
						if localfi.Mtim.Nano() != remotefi.Mtim.Nano() {
							logger.Debug().Msgf("File %s is indicating time change from %v to %v, applying attribute changes", localpath, time.Unix(0, localfi.Mtim.Nano()), time.Unix(0, remotefi.Mtim.Nano()))
							apply_attributes = true
						}

						if localfi.Mode.Perm() != remotefi.Mode.Perm() || localfi.Owner != remotefi.Owner || localfi.Group != remotefi.Group {
							logger.Debug().Msgf("File %s is indicating permissions changes, applying attribute changes", localpath)
							apply_attributes = true
						}

					}
				}

				if create_file {
					apply_attributes = true
				}

				if remotefi.Size > 0 && remotefi.Mode&fs.ModeSymlink == 0 && (apply_attributes || c.AlwaysChecksum) {
					logger.Debug().Msgf("Doing file content validation for %s", localpath)
					copy_verify_file = true
				}

				// try to hard link it
				if create_file && c.PreserveHardlinks && remotefi.Nlink > 1 {
					if ini, found := c.inodes.Load(inodeinfo{
						inode: remotefi.Inode,
					}); found {
						if localpath != ini.path {
							logger.Debug().Msgf("Hardlinking %s to %s", localpath, ini.path)
							err = os.Link(filepath.Join(ini.path), localpath)
							if err != nil {
								logger.Error().Msgf("Error hardlinking %s to %s: %v", localpath, ini.path, err)
								continue
							}
							create_file = false
							copy_verify_file = false
							apply_attributes = true
						}
					} else {
						logger.Error().Msgf("Remote file %s indicates it should be hardlinked with %v others, but we don't have a match locally", remotefi.Name, remotefi.Nlink)
					}
				}

				transfersuccess := true

				var localfile *os.File
				if create_file {
					if remotefi.Mode&fs.ModeDevice != 0 {
						if remotefi.Mode&fs.ModeCharDevice != 0 {
							err = mkNod(localpath, syscall.S_IFCHR, remotefi.Rdev)
							if err != nil {
								logger.Error().Msgf("Error creating char device %s: %v", localpath, err)
								continue
							}
						} else {
							// Block device
							err = mkNod(localpath, syscall.S_IFBLK, remotefi.Rdev)
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
					logger.Debug().Msgf("Processing blocks for %s", remotefi.Name)
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

					err = client.Call("Server.Open", remotefi.Name, nil)
					if err != nil {
						logger.Error().Msgf("Error opening remote file %s: %v", remotefi.Name, err)
						logger.Error().Msgf("Item fileinfo: %+v", remotefi)
						break
					}

					for i := int64(0); i < remotefi.Size; i += int64(c.BlockSize) {
						if !transfersuccess {
							break // we couldn't open the remote file
						}

						// Read the chunk
						length := int64(c.BlockSize)
						if i+length > remotefi.Size {
							length = remotefi.Size - i
						}
						chunkArgs := GetChunkArgs{
							Path:   remotefi.Name,
							Offset: uint64(i),
							Size:   uint64(length),
						}
						if i+length <= existingsize {
							var hash uint64
							err = client.Call("Server.ChecksumChunk", chunkArgs, &hash)
							if err != nil {
								logger.Error().Msgf("Error getting remote checksum for file %s chunk at %d: %v", remotefi.Name, i, err)
								transfersuccess = false
							}
							localdata := make([]byte, length)
							n, err := localfile.ReadAt(localdata, i)
							if err != nil {
								logger.Error().Msgf("Error reading existing local file %s chunk at %d: %v", localpath, i, err)
								transfersuccess = false
								break
							}
							p.Add(ReadBytes, uint64(length))
							if n == int(length) {
								localhash := xxhash.Sum64(localdata)
								logger.Trace().Msgf("Checksum for file %s chunk at %d is %X, remote is %X", remotefi.Name, i, localhash, hash)
								if localhash == hash {
									continue // Block matches
								}
							}
						}

						var data []byte
						logger.Debug().Msgf("Transferring file %s chunk at %d", remotefi.Name, i)
						err = client.Call("Server.GetChunk", chunkArgs, &data)
						if err != nil {
							logger.Error().Msgf("Error transferring file %s chunk at %d: %v", remotefi.Name, i, err)
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
						p.Add(WrittenBytes, uint64(length))
						apply_attributes = true
					}
					err = client.Call("Server.Close", remotefi.Name, nil)
					if err != nil {
						logger.Error().Msgf("Error closing remote file %s: %v", remotefi.Name, err)
					}
				}
				if localfile != nil {
					localfile.Close()
				}

				if apply_attributes && transfersuccess {
					logger.Debug().Msgf("Updating metadata for %s", remotefi.Name)
					err = localfi.ApplyChanges(remotefi)
					if err != nil {
						logger.Error().Msgf("Error applying metadata for %s: %v", remotefi.Name, err)
					}
				}

				// handle inode counters
				if remaininghardlinks == 0 {
					// No more references, free up some memory
					logger.Trace().Msgf("No more references to this inode, removing it from inode cache")
					c.inodes.Delete(inodeinfo{inode: remotefi.Inode})
					deletedinodes++
					if c.inodes.Len()/4 < deletedinodes {
						deletedinodes = 0
						c.inodes.Optimize(gonk.Minimize)
					}
				}

				// are we done with this directory, the apply attributes to that
				donewithdirectory := false
				founddirectory := false
				lookupdirectory := dirinfo{
					name: filepath.Dir(remotefi.Name),
				}
				var atim, mtim syscall.Timespec
				c.dircache.AtomicMutate(lookupdirectory, func(item *dirinfo) {
					founddirectory = true
					left := atomic.AddInt32(&item.remaining, -1)
					logger.Trace().Msgf("directory %s has usage %v left", item.name, left)
					if left == 0 {
						// Apply modify times to directory
						atim = item.atim
						mtim = item.mtim
						donewithdirectory = true
					}
				}, false)
				if !founddirectory {
					logger.Error().Msgf("Failed to find directory info for postprocessing %s", lookupdirectory.name)
				}
				if donewithdirectory {
					localname := filepath.Join(c.BasePath, lookupdirectory.name)
					localfi, err := os.Stat(localname)
					if err != nil {
						logger.Error().Msgf("Error getting file info for directory %s: %v", lookupdirectory.name, err)
					}
					localstat, ok := localfi.Sys().(*syscall.Stat_t)
					if !ok {
						logger.Error().Msgf("Error getting file info for directory %s: %v", lookupdirectory.name, err)
					}
					localatim, localmtim, _ := getAMtime(*localstat)
					if atim != localatim || mtim != localmtim {
						logger.Debug().Msgf("Updating metadata for directory %s", localname)
						err = unix.UtimesNanoAt(unix.AT_FDCWD, localname, []unix.Timespec{unix.Timespec(atim), unix.Timespec(mtim)}, unix.AT_SYMLINK_NOFOLLOW)
						if err != nil {
							logger.Error().Msgf("Error changing times for directory %s: %v", lookupdirectory.name, err)
						}
					}
					c.dircache.Delete(lookupdirectory)
				}
				p.Add(FilesProcessed, 1)
				p.Add(BytesProcessed, uint64(remotefi.Size))
			}
			logger.Trace().Msg("Shutting down file worker")
			c.fileWorkerWG.Done()
		}()
	}

	// Get the party started
	var rootdirinfo FileInfo
	err := client.Call("Server.Stat", "/", &rootdirinfo)
	if err != nil {
		return err
	}
	logger.Debug().Msg("Queueing directory / from remote")
	listfilesActive.Add(1)
	c.dircache.Store(dirinfo{
		name:      rootdirinfo.Name,
		atim:      rootdirinfo.Atim,
		mtim:      rootdirinfo.Mtim,
		remaining: -1,
	})
	c.dirqueuein <- rootdirinfo

	// wait for all directories to be listed
	listfilesActive.Wait()
	logger.Debug().Msg("No more directories to list")
	// close the directory stack
	c.dirstack.Close()
	// wait for all directory workers to finish
	c.dirWorkerWG.Wait()
	// close the file queue so file workers can finish
	close(c.filequeue)
	// wait for all workers to finish
	c.fileWorkerWG.Wait()

	logger.Debug().Msg("Client routine done")

	return nil
}

func (c *Client) Done() bool {
	return c.done

}

func (c *Client) Stats() (inodes, directories, filequeue, directoriestack int) {
	return c.inodes.Len(),
		c.dirstack.Len(),
		len(c.filequeue),
		c.dirstack.Len()
}

func (c *Client) Wait() {
	c.fileWorkerWG.Wait()
}
