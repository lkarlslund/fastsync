package fastsync

import (
	"errors"
	"path/filepath"
)

func topLevelDirectory(info FileInfo) bool {
	name := relativeResumePath(info.Name)
	return info.IsDir && filepath.IsLocal(name) && filepath.Dir(name) == "."
}

// The root stamp stays invalid while descendants are changed. A filesystem
// barrier at the boundary makes all descendant directory entries durable too.
func (c *Client) invalidateCompletionStamp(remote FileInfo) error {
	local, err := c.localFileInfo(filepath.Join(c.BasePath, remote.Name))
	if err != nil {
		return err
	}
	dirty := local
	dirty.Mtim.Sec, dirty.Mtim.Nsec = 0, 0
	if dirty.Mtim == remote.Mtim {
		dirty.Mtim.Sec = 1
	}
	if err := local.SetTimestamps(dirty); err != nil {
		return err
	}
	return syncCompletionStamp(local.Name)
}

func (c *Client) flushCompletionDirectory(item *dirinfo) error {
	path := filepath.Join(c.BasePath, item.name)
	if err := c.flusher.waitDirectory(path); err != nil {
		return err
	}
	if err := syncCompletionFilesystem(path); err != nil {
		return err
	}
	return c.checkDirectory(c.remoteClient, item.info)
}

func syncCompletionStamp(path string) error {
	directory, err := openNoFollow(path)
	if err != nil {
		return err
	}
	err = errors.Join(directory.Sync(), directory.Close())
	if err != nil {
		return err
	}
	return syncParent(path)
}
