package fastsync

import (
	"fmt"
	"path/filepath"
)

func (c *Client) openInodeSpill() error {
	parent, err := DirectoryPathNoFollow(c.InodeSpillDirectory)
	if err != nil {
		return err
	}
	dest, err := DirectoryPathNoFollow(c.BasePath)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(dest, parent)
	if err != nil || rel == "." || filepath.IsLocal(rel) {
		return fmt.Errorf("inode spill directory must be outside the destination tree")
	}
	store, err := openInodeStore(parent)
	if err == nil {
		c.inodesMu.Lock()
		c.inodeStore = store
		c.inodesMu.Unlock()
	}
	return err
}

func (c *Client) inodePathLocked(key inodeKey) (string, error) {
	if entry := c.inodes[key]; entry != nil {
		return entry.localhardlinkpath, nil
	}
	if c.inodeStore == nil {
		return "", nil
	}
	return c.inodeStore.canonicalPath(key)
}

func (c *Client) admitInode(remote FileInfo) (*inodeinfo, bool, error) {
	c.inodesMu.Lock()
	defer c.inodesMu.Unlock()
	key := inodeKey{remote.Dev, remote.Inode}
	entry := c.inodes[key]
	if entry == nil && c.inodeStore != nil {
		var err error
		entry, err = c.inodeStore.load(key)
		if err != nil {
			return nil, false, err
		}
		if entry != nil {
			c.residentDisk++
			c.inodes[key] = entry
		}
	}
	follower := entry != nil
	if entry == nil {
		entry = &inodeinfo{localhardlinkpath: filepath.Join(c.BasePath, remote.Name), done: make(chan struct{}), remaining: remote.Nlink}
		c.inodes[key] = entry
	}
	if entry.idle != nil {
		c.inodeIdle.Remove(entry.idle)
		entry.idle = nil
	}
	entry.users++
	return entry, follower, nil
}

func (c *Client) releaseInode(remote FileInfo, entry *inodeinfo) error {
	c.inodesMu.Lock()
	defer c.inodesMu.Unlock()
	key := inodeKey{remote.Dev, remote.Inode}
	entry.users--
	entry.remaining--
	if entry.remaining == 0 {
		if entry.diskBacked {
			if err := c.inodeStore.remove(key); err != nil {
				return err
			}
			c.residentDisk--
		}
		delete(c.inodes, key)
		return nil
	}
	return c.idleInodeLocked(key, entry)
}

func (c *Client) idleInodeLocked(key inodeKey, entry *inodeinfo) error {
	if c.inodeStore == nil || entry.users != 0 {
		return nil
	}
	if entry.idle == nil {
		entry.idle = c.inodeIdle.PushBack(key)
	}
	for len(c.inodes) > c.InodeMemoryGroups && c.inodeIdle.Len() > 0 {
		oldest := c.inodeIdle.Front()
		key := oldest.Value.(inodeKey)
		entry := c.inodes[key]
		if err := c.inodeStore.put(key, entry); err != nil {
			return err
		}
		if entry.diskBacked {
			c.residentDisk--
		}
		c.inodeIdle.Remove(oldest)
		entry.idle = nil
		delete(c.inodes, key)
	}
	return nil
}
