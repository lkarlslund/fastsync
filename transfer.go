package fastsync

import (
	"errors"
	"fmt"
	"io"
	"net/rpc"
	"os"
	"path/filepath"

	"github.com/cespare/xxhash/v2"
)

// A single worker owns each remote inode. Followers wait for successful publication,
// not merely for the destination pathname to exist.
func (c *Client) syncFile(client *rpc.Client, remote FileInfo) (err error) {
	path := filepath.Join(c.BasePath, remote.Name)
	if c.PreserveHardlinks && remote.Nlink > 1 {
		key := inodeKey{remote.Dev, remote.Inode}
		c.inodesMu.Lock()
		entry, follower := c.inodes[key]
		if !follower {
			entry = &inodeinfo{localhardlinkpath: path, done: make(chan struct{}), remaining: remote.Nlink}
			c.inodes[key] = entry
		}
		c.inodesMu.Unlock()
		defer func() {
			c.inodesMu.Lock()
			entry.remaining--
			if entry.remaining == 0 {
				delete(c.inodes, key)
			}
			c.inodesMu.Unlock()
		}()
		if follower {
			<-entry.done
			if entry.err != nil {
				return fmt.Errorf("hardlink source failed: %w", entry.err)
			}
			if err := checkRemote(client, remote); err != nil {
				return err
			}
			return c.timeMetadata(func() error { return publishHardlink(entry.localhardlinkpath, path, c.Durable) })
		}
		defer func() { entry.err = err; close(entry.done) }()
	}
	return c.syncIndependent(client, remote, path)
}

func (c *Client) syncIndependent(client *rpc.Client, remote FileInfo, path string) error {
	return c.syncIndependentDispatch(client, remote, path, func(work func() error) error { return work() })
}

// Checking workers hand data work to the bounded copy stage.
func (c *Client) syncIndependentDispatch(client *rpc.Client, remote FileInfo, path string, data func(func() error) error) error {
	local, err := c.localFileInfo(path)
	exists := err == nil
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if !c.Options.SendXattr {
		remote.Xattrs = nil
	}
	if remote.Mode.IsRegular() {
		sameContent := exists && local.Mode.IsRegular() && local.Size == remote.Size
		if sameContent && c.AlwaysChecksum {
			sameContent, err = c.contentMatches(client, remote, path)
			if err != nil {
				return err
			}
		}
		if sameContent && !c.AlwaysChecksum {
			sameContent = local.Mtim == remote.Mtim
		}
		if !sameContent {
			return data(func() error { return c.stageRegular(client, remote, path, local, exists) })
		}
		if err := checkRemote(client, remote); err != nil {
			return err
		}
		metadataChanged := compareMetadata(local, remote, c.Options.SendXattr) != nil
		if local.Nlink > 1 && (metadataChanged || c.conflictingLocalInode(local, remote)) {
			// The content selection policy has already accepted the local data. Detach
			// without fetching/checksumming it again, and never edit the shared inode.
			return data(func() error { return c.stageLocal(client, remote, path) })
		}
		if !metadataChanged {
			c.Perf.Add(FilesUnchanged, 1)
			return nil
		}
		err := c.applyMetadata(local, remote)
		if err == nil && c.resume != nil && c.flusher != nil {
			f, e := openNoFollow(path)
			if e != nil {
				return e
			}
			defer f.Close()
			err = f.Sync()
		}
		return err
	} else if !exists || local.Mode&os.ModeType != remote.Mode&os.ModeType ||
		local.LinkTo != remote.LinkTo || local.Rdev != remote.Rdev ||
		(local.Nlink > 1 && (compareMetadata(local, remote, c.Options.SendXattr) != nil || c.conflictingLocalInode(local, remote))) {
		tmp, err := mkdirTempNoFollow(filepath.Dir(path), ".fastsync-")
		if err != nil {
			return err
		}
		defer removeAllNoFollow(tmp)
		staged := filepath.Join(tmp, "entry")
		fi := FileInfo{Name: staged}
		if err := fi.Create(remote); err != nil {
			return err
		}
		fi, err = c.localFileInfo(staged)
		if err != nil {
			return err
		}
		if err := c.applyMetadata(fi, remote); err != nil {
			return err
		}
		if err := checkRemote(client, remote); err != nil {
			return err
		}
		return c.timeMetadata(func() error { return publish(staged, path, c.Durable) })
	}
	if err := checkRemote(client, remote); err != nil {
		return err
	}
	// Always reconcile metadata, including changes that do not affect mtime.
	if compareMetadata(local, remote, c.Options.SendXattr) == nil {
		c.Perf.Add(FilesUnchanged, 1)
		return nil
	}
	return c.applyMetadata(local, remote)
}

func (c *Client) stageRegular(client *rpc.Client, remote FileInfo, path string, local FileInfo, exists bool) (err error) {
	defer func() {
		if err == nil {
			c.Perf.Add(FilesCopied, 1)
		}
	}()
	if c.Pipeline {
		release := c.streamGate.acquire()
		defer release(0, 0)
	}

	stage, err := createTempNoFollow(filepath.Dir(path), ".fastsync-")
	if err != nil {
		return err
	}
	defer removeNoFollow(stage.Name())
	defer func() {
		if stage != nil {
			err = errors.Join(err, stage.Close())
		}
	}()
	defer func() {
		if stage != nil {
			c.finishDataFile(stage)
		}
	}()
	var previous *os.File
	if exists && local.Mode.IsRegular() {
		previous, err = openNoFollow(path)
		if err != nil {
			return err
		}
		defer previous.Close()
	}
	if err = client.Call("Server.Open", remote.Name, nil); err != nil {
		return err
	}
	opened := true
	defer func() {
		if opened {
			err = errors.Join(err, client.Call("Server.Close", remote.Name, nil))
		}
	}()
	if c.Pipeline {
		if err = c.streamRegular(client, remote, local, previous, stage); err != nil {
			return err
		}
	} else {
		buffer := make([]byte, c.BlockSize)
		var data []byte
		for offset := int64(0); offset < remote.Size; {
			length := min(int64(c.BlockSize), remote.Size-offset)
			args := GetChunkArgs{Path: remote.Name, Offset: uint64(offset), Size: uint64(length)}
			matched := false
			if previous != nil && offset+length <= local.Size {
				if _, err = previous.ReadAt(buffer[:length], offset); err != nil {
					return err
				}
				c.Perf.Add(ReadBytes, uint64(length))
				var checksum uint64
				if err = client.Call("Server.ChecksumChunk", args, &checksum); err != nil {
					return err
				}
				matched = xxhash.Sum64(buffer[:length]) == checksum
			}
			chunk := buffer[:length]
			if !matched {
				data = data[:0]
				if err = client.Call("Server.GetChunk", args, &data); err != nil {
					return err
				}
				if int64(len(data)) != length {
					return io.ErrUnexpectedEOF
				}
				chunk = data
			}
			n, writeErr := c.writeData(stage, chunk)
			if writeErr != nil {
				return writeErr
			} else if n != len(chunk) {
				return io.ErrShortWrite
			}
			// Account for all local writes, but only fetched blocks as transferred payload.
			c.Perf.Add(WrittenBytes, uint64(length))
			if !matched {
				c.Perf.Add(TransferredFileBytes, uint64(length))
			}
			offset += length
		}
	}
	if err = checkRemote(client, remote); err != nil {
		return err
	}
	err = client.Call("Server.Close", remote.Name, nil)
	opened = false
	if err != nil {
		return err
	}
	fi, err := c.localFileInfo(stage.Name())
	if err != nil {
		return err
	}
	if err = c.applyMetadata(fi, remote); err != nil {
		return err
	}
	if c.Durable {
		if err = c.timeLocalIO(stage.Sync); err != nil {
			return err
		}
	}
	c.finishDataFile(stage)
	staged := stage.Name()
	err = stage.Close()
	stage = nil
	if err != nil {
		return err
	}
	return c.timeMetadata(func() error { return publish(staged, path, c.Durable) })
}

// A changing source must never be certified as a completed copy. This detects
// ordinary mutations; callers must still use a snapshot or quiescent source.
func checkRemote(client *rpc.Client, before FileInfo) error {
	var after FileInfo
	if err := client.Call("Server.Stat", before.Name, &after); err != nil {
		return err
	}
	if before.Dev != after.Dev || before.Inode != after.Inode || before.Size != after.Size ||
		before.Mtim != after.Mtim || before.Ctim != after.Ctim || before.Mode != after.Mode ||
		before.LinkTo != after.LinkTo || before.Nlink != after.Nlink {
		return fmt.Errorf("source changed during operation: %s", before.Name)
	}
	return nil
}

func publishHardlink(source, path string, durable bool) error {
	a, err := lstatNoFollow(source)
	if err != nil {
		return err
	}
	b, err := lstatNoFollow(path)
	if err == nil && os.SameFile(a, b) {
		return nil
	}
	if os.IsNotExist(err) {
		if err := linkNoFollow(source, path); err != nil {
			return err
		}
		if durable {
			return syncParent(path)
		}
		return nil
	}
	if err != nil {
		return err
	}
	tmp, err := mkdirTempNoFollow(filepath.Dir(path), ".fastsync-")
	if err != nil {
		return err
	}
	defer removeAllNoFollow(tmp)
	staged := filepath.Join(tmp, "entry")
	if err := linkNoFollow(source, staged); err != nil {
		return err
	}
	return publish(staged, path, durable)
}

func publish(staged, path string, durable bool) error {
	// Rename replaces files atomically. Only an empty directory may be replaced;
	// never erase a populated history tree to resolve a type conflict.
	if fi, err := lstatNoFollow(path); err == nil && fi.IsDir() {
		if err := removeNoFollow(path); err != nil {
			return err
		}
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := renameNoFollow(staged, path); err != nil {
		return err
	}
	if !durable {
		return nil
	}
	return syncParent(path)
}

func syncParent(path string) error {
	dir, err := openNoFollow(filepath.Dir(path))
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func (c *Client) timeLocalIO(operation func() error) error {
	return timedIO(&c.localIO, &c.activeIO, operation)
}

func (c *Client) localFileInfo(path string) (info FileInfo, err error) {
	err = c.timeMetadata(func() error { var e error; info, e = pathToFileInfo(path, c.Options.SendXattr); return e })
	return
}
func (c *Client) applyMetadata(local, remote FileInfo) error {
	return c.timeMetadata(func() error { return local.ApplyChanges(remote) })
}

// Separate source inodes must not remain merged at the destination, even when
// their metadata/content happen to match. Correct source hardlinks share a claim.
func (c *Client) conflictingLocalInode(local, remote FileInfo) bool {
	c.inodesMu.Lock()
	defer c.inodesMu.Unlock()
	if c.localOwners == nil {
		c.localOwners = make(map[inodeKey]inodeKey)
	}
	localKey, remoteKey := inodeKey{local.Dev, local.Inode}, inodeKey{remote.Dev, remote.Inode}
	if owner, found := c.localOwners[localKey]; found {
		return owner != remoteKey
	}
	c.localOwners[localKey] = remoteKey
	return false
}

func (c *Client) contentMatches(client *rpc.Client, remote FileInfo, path string) (same bool, err error) {
	local, err := openNoFollow(path)
	if err != nil {
		return false, err
	}
	defer func() { err = errors.Join(err, local.Close()) }()
	if err = client.Call("Server.Open", remote.Name, nil); err != nil {
		return false, err
	}
	defer func() { err = errors.Join(err, client.Call("Server.Close", remote.Name, nil)) }()
	buffer := make([]byte, c.BlockSize)
	for offset := int64(0); offset < remote.Size; {
		length := min(int64(c.BlockSize), remote.Size-offset)
		if err = c.timeLocalIO(func() error { _, e := local.ReadAt(buffer[:length], offset); return e }); err != nil {
			return false, err
		}
		c.Perf.Add(ReadBytes, uint64(length))
		var remoteHash uint64
		if err = client.Call("Server.ChecksumChunk", GetChunkArgs{Path: remote.Name, Offset: uint64(offset), Size: uint64(length)}, &remoteHash); err != nil {
			return false, err
		}
		if xxhash.Sum64(buffer[:length]) != remoteHash {
			return false, nil
		}
		offset += length
	}
	return true, nil
}

func (c *Client) stageLocal(client *rpc.Client, remote FileInfo, path string) (err error) {
	defer func() {
		if err == nil {
			c.Perf.Add(FilesCopied, 1)
		}
	}()
	old, err := openNoFollow(path)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, old.Close()) }()
	staged, err := createTempNoFollow(filepath.Dir(path), ".fastsync-")
	if err != nil {
		return err
	}
	stagedPath := staged.Name()
	defer removeNoFollow(stagedPath)
	defer func() {
		if staged != nil {
			err = errors.Join(err, staged.Close())
		}
	}()
	defer func() {
		if staged != nil {
			c.finishDataFile(staged)
		}
	}()
	if err = c.timeLocalIO(func() error {
		return cloneOrCopy(staged, old, c.Perf, func(data []byte) (int, error) { return c.writeData(staged, data) })
	}); err != nil {
		return err
	}
	local, err := c.localFileInfo(stagedPath)
	if err != nil {
		return err
	}
	if err = c.applyMetadata(local, remote); err != nil {
		return err
	}
	if err = checkRemote(client, remote); err != nil {
		return err
	}
	if c.Durable {
		if err = c.timeLocalIO(staged.Sync); err != nil {
			return err
		}
	}
	c.finishDataFile(staged)
	err = staged.Close()
	staged = nil
	if err != nil {
		return err
	}
	return c.timeMetadata(func() error { return publish(stagedPath, path, c.Durable) })
}

func cloneOrCopy(dest, source *os.File, perf *performance, write ...func([]byte) (int, error)) error {
	if err := cloneFile(dest, source); err == nil {
		return nil
	} else if !errors.Is(err, ErrNotSupportedByPlatform) {
		return err
	}
	// Reflink isn't available on every filesystem. Copy locally rather than
	// violating the checksum policy by requesting remote checksums/content.
	if err := dest.Truncate(0); err != nil {
		return err
	}
	if _, err := dest.Seek(0, 0); err != nil {
		return err
	}
	if _, err := source.Seek(0, 0); err != nil {
		return err
	}
	var output io.Writer = dest
	if len(write) > 0 {
		output = writeFunc(write[0])
	}
	n, err := io.Copy(output, source)
	perf.Add(ReadBytes, uint64(n))
	perf.Add(WrittenBytes, uint64(n))
	return err
}

type writeFunc func([]byte) (int, error)

func (w writeFunc) Write(data []byte) (int, error) { return w(data) }
