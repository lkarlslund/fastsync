package main

import (
	"io/fs"
	"net/rpc"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	wunix "github.com/akihirosuda/x-sys-unix-auto-eintr"
	"github.com/cespare/xxhash/v2"
	"github.com/joshlf/go-acl"
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

type folderinfo struct {
	name      string
	atim      syscall.Timespec
	mtim      syscall.Timespec
	remaining int32
}

func (f folderinfo) LessThan(f2 folderinfo) bool {
	return f.name < f2.name
}

type Client struct {
	BasePath string

	AlwaysChecksum    bool
	Pfile, Pdir       int
	PreserveHardlinks bool
	BlockSize         int

	done bool

	dirWorkerWG, fileWorkerWG sync.WaitGroup

	filequeue      chan FileInfo
	inodes         gonk.Gonk[inodeinfo]
	foldercache    gonk.Gonk[folderinfo]
	folderstack    *stack[FileInfo]
	folderqueuein  chan<- FileInfo
	folderqueueout <-chan FileInfo
}

func NewClient() *Client {
	c := &Client{
		Pfile:             4096,
		Pdir:              512,
		PreserveHardlinks: true,
		BlockSize:         128 * 1024,
		filequeue:         make(chan FileInfo, 4096*16),
	}

	c.folderstack, c.folderqueueout, c.folderqueuein = NewStack[FileInfo](c.Pdir*2, 8)

	return c
}

func (c *Client) Run(client *rpc.Client) error {
	// Start the process

	var deletedinodes int

	var listfilesActive sync.WaitGroup

	for i := 0; i < c.Pdir; i++ {
		c.dirWorkerWG.Add(1)
		go func() {
			logger.Trace().Msg("Starting directory worker")
			for item := range c.folderqueueout {
				logger.Trace().Msgf("Processing folder queue item for %s", item.Name)

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
				var folderfound bool
				c.foldercache.AtomicMutate(folderinfo{
					name: item.Name,
				}, func(f *folderinfo) {
					atomic.StoreInt32(&f.remaining, int32(filecount))
					folderfound = true
				}, false)
				if !folderfound {
					logger.Error().Msgf("Folder %v not found in folder cache", filelistresponse.ParentFolder)
				}

				// queue files first
				for _, remotefi := range filelistresponse.Files {
					if !remotefi.IsDir {
						logger.Trace().Msgf("Queueing file %s", remotefi.Name)
						c.filequeue <- remotefi
					}
				}

				// queue folders second
				for _, remotefi := range filelistresponse.Files {
					if remotefi.IsDir {
						localpath := filepath.Join(c.BasePath, remotefi.Name)
						// logger.Trace().Msgf("Queueing folder %s", remotefi.Name)
						// check if folder exists
						localfi, err := os.Stat(localpath)
						created := false
						if err != nil {
							logger.Trace().Msgf("Creating folder %s", localpath)
							err = os.MkdirAll(localpath, 0755)
							if err != nil {
								logger.Error().Msgf("Error creating folder %v: %v", localpath, err)
								continue
							}
							localfi, err = os.Stat(localpath)
							if err != nil {
								logger.Error().Msgf("Error getting stat for newly created folder %v: %v", localpath, err)
								continue
							}
							created = true
						}
						localstat, ok := localfi.Sys().(*syscall.Stat_t)
						if created || ok && localstat.Mtim != remotefi.Mtim {
							err = wunix.UtimesNanoAt(unix.AT_FDCWD, localpath, []unix.Timespec{unix.Timespec(remotefi.Atim), unix.Timespec(remotefi.Mtim)}, unix.AT_SYMLINK_NOFOLLOW)
							if err != nil {
								logger.Error().Msgf("Error setting times for folder %v: %v", localpath, err)
							}
						}
						if created || ok && localstat.Mode&^uint32(os.ModePerm) != remotefi.Permissions&^uint32(os.ModePerm) {
							err = wunix.Chmod(localpath, remotefi.Permissions)
							if err != nil {
								logger.Error().Msgf("Error setting permissions for folder %v: %v", localpath, err)
							}
						}

						logger.Trace().Msgf("Queueing folder %v", remotefi.Name)
						listfilesActive.Add(1)
						c.foldercache.Store(folderinfo{
							name:      remotefi.Name,
							atim:      remotefi.Atim,
							mtim:      remotefi.Mtim,
							remaining: -1, // we don't know yet
						})
						c.folderqueuein <- remotefi
					}
				}
				p.Add(FoldersProcessed, 1)
				listfilesActive.Done()
			}
			logger.Trace().Msg("Shutting down folder worker")
			c.dirWorkerWG.Done()
		}()
	}

	for i := 0; i < c.Pfile; i++ {
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
					logger.Trace().Msgf("Saving remote inode number to cache for file %s", remotefi.Name)
					c.inodes.AtomicMutate(inodeinfo{
						inode:     remotefi.Inode,
						path:      localpath,
						remaining: int32(remotefi.Nlink),
					}, func(i *inodeinfo) {
						remaininghardlinks = atomic.AddInt32(&i.remaining, -1)
					}, true)
				}

				localfi, err := os.Lstat(localpath)
				if err != nil {
					if os.IsNotExist(err) {
						logger.Debug().Msgf("File %s does not exist", localpath)
						create_file = true
					} else {
						logger.Error().Msgf("Error getting stat stat for local path %s: %v", localpath, err)
						continue
					}
				}

				// var localstat *syscall.Stat_t
				if !create_file {
					var ok bool
					localstat, ok := localfi.Sys().(*syscall.Stat_t)
					if !ok {
						logger.Error().Msgf("Could not obtain local native stat for %s", localpath)
						continue
					}

					// if it's a hardlinked file, check that it's linked correctly
					if remotefi.Nlink > 1 {
						if ini, found := c.inodes.Load(inodeinfo{
							inode: remotefi.Inode,
						}); found {
							otherlocalfi, err := os.Lstat(ini.path)
							if err != nil {
								logger.Error().Msgf("Error getting hardlink stat for local path %s: %v", localpath, err)
								continue
							}
							otherlocalstat, ok := otherlocalfi.Sys().(*syscall.Stat_t)
							if !ok {
								logger.Error().Msgf("Could not obtain local native stat for %s", localpath)
								continue
							}

							if localstat.Ino != otherlocalstat.Ino {
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

					if !create_file && localstat.Mode&^uint32(os.ModePerm) != remotefi.Permissions&^uint32(os.ModePerm) {
						logger.Debug().Msgf("File %s is indicating type change from %X to %X, unlinking", localpath, localstat.Mode&^uint32(os.ModePerm), remotefi.Permissions&^uint32(os.ModePerm))
						err = os.Remove(localpath)
						if err != nil {
							logger.Error().Msgf("Error unlinking %s: %v", localpath, err)
							continue
						}

						create_file = true
					}

					if !create_file { // still exists
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
						logger.Debug().Msgf("Hardlinking %s to %s", localpath, ini.path)
						err = os.Link(filepath.Join(ini.path), localpath)
						if err != nil {
							logger.Error().Msgf("Error hardlinking %s to %s: %v", localpath, ini.path, err)
							continue
						}
						create_file = false
						copy_verify_file = false
						apply_attributes = true
					} else {
						logger.Debug().Msgf("Remote file %s indicates it should be hardlinked, but we don't have a match locally", localpath)
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
					err = os.Lchown(localpath, int(remotefi.Owner), int(remotefi.Group))
					if err != nil {
						logger.Error().Msgf("Error changing owner for %s: %v", localpath, err)
					}
					if remotefi.Mode&fs.ModeSymlink == 0 {
						err = os.Chmod(localpath, fs.FileMode(remotefi.Permissions))
						if err != nil {
							logger.Error().Msgf("Error changing mode for %s: %v", localpath, err)
						}

						if len(remotefi.ACL) > 0 {
							currentacl, err := acl.Get(localpath)
							if err != nil {
								logger.Error().Msgf("Error getting ACL for %s: %v", localpath, err)
							} else {
								if !slices.Equal(currentacl, remotefi.ACL) {
									err = acl.Set(localpath, remotefi.ACL)
									if err != nil {
										logger.Error().Msgf("Error setting ACL %+v (was %+v) for %s: %v", remotefi.ACL, currentacl, localpath, err)
									}
								}
							}
						}
					}

					err = wunix.UtimesNanoAt(unix.AT_FDCWD, localpath, []unix.Timespec{unix.Timespec(remotefi.Atim), unix.Timespec(remotefi.Mtim)}, unix.AT_SYMLINK_NOFOLLOW)
					if err != nil {
						logger.Error().Msgf("Error changing times for %s: %v", localpath, err)
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

				// are we done with this folder, the apply attributes to that
				donewithfolder := false
				foundfolder := false
				lookupfolder := folderinfo{
					name: filepath.Dir(remotefi.Name),
				}
				var atim, mtim syscall.Timespec
				c.foldercache.AtomicMutate(lookupfolder, func(item *folderinfo) {
					foundfolder = true
					left := atomic.AddInt32(&item.remaining, -1)
					logger.Trace().Msgf("Folder %s has usage %v left", item.name, left)
					if left == 0 {
						// Apply modify times to folder
						atim = item.atim
						mtim = item.mtim
						donewithfolder = true
					}
				}, false)
				if !foundfolder {
					logger.Error().Msgf("Failed to find folder info for postprocessing %s", lookupfolder.name)
				}
				if donewithfolder {
					localname := filepath.Join(c.BasePath, lookupfolder.name)
					localfi, err := os.Stat(localname)
					if err != nil {
						logger.Error().Msgf("Error getting file info for folder %s: %v", lookupfolder.name, err)
					}
					localstat, ok := localfi.Sys().(*syscall.Stat_t)
					if !ok {
						logger.Error().Msgf("Error getting file info for folder %s: %v", lookupfolder.name, err)
					}
					if atim != localstat.Atim || mtim != localstat.Mtim {
						logger.Debug().Msgf("Updating metadata for folder %s", localname)
						err = wunix.UtimesNanoAt(unix.AT_FDCWD, localname, []unix.Timespec{unix.Timespec(atim), unix.Timespec(mtim)}, unix.AT_SYMLINK_NOFOLLOW)
						if err != nil {
							logger.Error().Msgf("Error changing times for folder %s: %v", lookupfolder.name, err)
						}
					}
					c.foldercache.Delete(lookupfolder)
				}
				p.Add(FilesProcessed, 1)
				p.Add(BytesProcessed, uint64(remotefi.Size))
			}
			logger.Trace().Msg("Shutting down file worker")
			c.fileWorkerWG.Done()
		}()
	}

	// Get the party started
	var rootfolderinfo FileInfo
	err := client.Call("Server.Stat", "/", &rootfolderinfo)
	if err != nil {
		return err
	}
	logger.Debug().Msg("Queueing folder / from remote")
	listfilesActive.Add(1)
	c.foldercache.Store(folderinfo{
		name:      rootfolderinfo.Name,
		atim:      rootfolderinfo.Atim,
		mtim:      rootfolderinfo.Mtim,
		remaining: -1,
	})
	c.folderqueuein <- rootfolderinfo

	// wait for all folders to be listed
	listfilesActive.Wait()
	logger.Debug().Msg("No more folders to list")
	// close the folder stack
	c.folderstack.Close()
	// wait for all directory workers to finish
	c.dirWorkerWG.Wait()
	// close the file queue so file workers can finish
	close(c.filequeue)
	// wait for all workers to finish
	c.fileWorkerWG.Wait()

	return nil
}

func (c *Client) Done() bool {
	return c.done

}

func (c *Client) Stats() (inodes, folders, filequeue, folderstack int) {
	return c.inodes.Len(),
		c.folderstack.Len(),
		len(c.filequeue),
		c.folderstack.Len()
}

func (c *Client) Wait() {
	c.fileWorkerWG.Wait()
}
