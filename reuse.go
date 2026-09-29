package fastsync

import (
	"fmt"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

type reuseSeed struct {
	source FileInfo
	local  struct {
		Dev, Inode uint64
		Size       int64
		Mtim       syscall.Timespec
		LinkTo     string
		Rdev       uint64
	}
}

func (s *reuseSeed) sameLocal(local FileInfo) bool {
	return local.Dev == s.local.Dev && local.Inode == s.local.Inode && local.Size == s.local.Size &&
		local.Mtim == s.local.Mtim && local.LinkTo == s.local.LinkTo && local.Rdev == s.local.Rdev
}

// No missing-file inventory: the second walk rediscovers deferred paths.
// Only the existing source-inode cache survives the pass barrier.
func (c *Client) warmExistingPass(client *rpc.Client, root FileInfo) {
	var workers sync.WaitGroup
	for i := 0; i < c.ParallelFile; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for remote := range c.filequeue {
				c.Perf.Add(ExistingExamined, 1)
				if err := c.warmExistingFile(client, remote); err != nil {
					c.recordError("check existing %s: %v", remote.Name, err)
				}
			}
		}()
	}
	c.walkAlphabetical(client, root)
	close(c.filequeue)
	workers.Wait()
}

func (c *Client) warmExistingFile(client *rpc.Client, remote FileInfo) error {
	// Single-link files cannot seed another pathname and are checked in pass 2.
	if remote.Nlink <= 1 {
		return nil
	}
	key := inodeKey{remote.Dev, remote.Inode}
	lock := &c.reuseLocks[(remote.Dev^remote.Inode)%uint64(len(c.reuseLocks))]
	lock.Lock()
	defer lock.Unlock()
	c.inodesMu.Lock()
	cached := c.inodes[key] != nil
	c.inodesMu.Unlock()
	if cached {
		return nil
	}
	path := filepath.Join(c.BasePath, remote.Name)
	local, err := c.localFileInfo(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !c.Options.SendXattr {
		remote.Xattrs = nil
	}
	if local.Mode&os.ModeType != remote.Mode&os.ModeType || compareMetadata(local, remote, c.Options.SendXattr) != nil {
		return nil
	}
	if remote.Mode.IsRegular() {
		if local.Size != remote.Size {
			return nil
		}
		if c.AlwaysChecksum {
			match, err := c.contentMatches(client, remote, path)
			if err != nil {
				return err
			}
			if !match {
				return nil
			}
		} else if local.Mtim != remote.Mtim {
			return nil
		}
	} else if local.LinkTo != remote.LinkTo || local.Rdev != remote.Rdev {
		return nil
	}
	if err := checkRemote(client, remote); err != nil {
		return err
	}
	// Never let two different source inodes adopt the same destination inode.
	if c.conflictingLocalInode(local, remote) {
		return nil
	}
	c.inodesMu.Lock()
	defer c.inodesMu.Unlock()
	entry := c.inodes[key]
	if entry != nil && entry.localhardlinkpath <= path {
		return nil
	}
	if entry == nil {
		entry = &inodeinfo{done: make(chan struct{}), remaining: remote.Nlink}
		close(entry.done)
		c.inodes[key] = entry
		c.Perf.Add(ReuseGroups, 1)
	}
	entry.localhardlinkpath = path
	entry.seed = &reuseSeed{source: remote}
	entry.seed.local.Dev, entry.seed.local.Inode = local.Dev, local.Inode
	entry.seed.local.Size, entry.seed.local.Mtim = local.Size, local.Mtim
	entry.seed.local.LinkTo, entry.seed.local.Rdev = local.LinkTo, local.Rdev
	c.saveResumeHint(remote)
	return nil
}

// The first link using a seed revalidates it after the read-only pass. A source
// mutation or replaced destination must fail rather than publish stale data.
func (c *Client) validateReuseSeed(client *rpc.Client, entry *inodeinfo) error {
	seed := entry.seed
	var current FileInfo
	if err := client.Call("Server.Stat", seed.source.Name, &current); err != nil {
		return err
	}
	local, err := c.localFileInfo(entry.localhardlinkpath)
	if err != nil {
		return err
	}
	if !seed.sameLocal(local) || compareMetadata(local, seed.source, c.Options.SendXattr) != nil {
		return fmt.Errorf("cached destination changed: %s", entry.localhardlinkpath)
	}
	if sameRemoteState(seed.source, current) || sameSymlinkState(seed.source, current) {
		return nil
	}
	// Adding/removing other hardlinks changes ctime without changing file data.
	// Only revalidate regular files, with stable identity/metadata and fresh hashes.
	if !current.Mode.IsRegular() || current.Dev != seed.source.Dev || current.Inode != seed.source.Inode ||
		current.Mode != seed.source.Mode || compareMetadata(current, seed.source, c.Options.SendXattr) != nil {
		return fmt.Errorf("source changed during operation: %s", seed.source.Name)
	}
	var sourceHash string
	if err := client.Call("Server.Hash", current.Name, &sourceHash); err != nil {
		return err
	}
	localHash, err := hashFile(entry.localhardlinkpath)
	if err != nil {
		return err
	}
	if sourceHash != localHash {
		return fmt.Errorf("cached source content changed: %s", seed.source.Name)
	}
	if err := checkRemote(client, current); err != nil {
		return err
	}
	local, err = c.localFileInfo(entry.localhardlinkpath)
	if err != nil {
		return err
	}
	if !seed.sameLocal(local) || compareMetadata(local, current, c.Options.SendXattr) != nil {
		return fmt.Errorf("cached destination changed: %s", entry.localhardlinkpath)
	}
	return nil
}
