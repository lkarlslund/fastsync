package fastsync

import (
	"fmt"
	"path/filepath"
	"strings"
)

type resumeSubtree struct {
	Source               FileInfo
	LocalDev, LocalInode uint64
}

func relativeResumePath(path string) string {
	return strings.TrimPrefix(filepath.Clean(path), string(filepath.Separator))
}

// Position checkpoints intentionally cover top-level subtrees, not individual
// files. This is a small set for dated backup histories, even with millions of paths.
func (c *Client) checkpointResumeDirectory(item *dirinfo) {
	if c.resume == nil || c.flusher == nil || c.runError() != nil {
		return
	}
	path := filepath.Join(c.BasePath, item.name)
	if err := c.flusher.waitDirectory(path); err != nil {
		c.recordError("checkpoint data flush: %v", err)
		return
	}
	directory, err := openNoFollow(path)
	if err != nil {
		c.recordError("checkpoint directory: %v", err)
		return
	}
	err = directory.Sync()
	directory.Close()
	if err != nil {
		c.recordError("checkpoint directory flush: %v", err)
		return
	}
	name := relativeResumePath(item.name)
	if !filepath.IsLocal(name) || filepath.Dir(name) != "." {
		return
	}
	// Persist the directory's entry in the destination root before marking done.
	if err := syncParent(path); err != nil {
		c.recordError("checkpoint parent flush: %v", err)
		return
	}
	local, err := pathToFileInfo(path, false)
	if err != nil {
		c.recordError("checkpoint directory identity: %v", err)
		return
	}
	source := item.info
	source.Xattrs = nil
	source.Atim.Sec = 0
	source.Atim.Nsec = 0
	c.resume.record(resumeRecord{Subtree: &resumeSubtree{Source: source, LocalDev: local.Dev, LocalInode: local.Inode}})
	c.resume.flush()
}
func (c *Client) skipResumeDirectory(remote FileInfo) bool {
	if c.warming || !c.ResumePosition || c.resume == nil {
		return false
	}
	name := relativeResumePath(remote.Name)
	marker, ok := c.resume.subtrees[name]
	if !ok {
		return false
	}
	s := marker.Source
	if remote.Dev != s.Dev || remote.Inode != s.Inode || remote.Mtim != s.Mtim || remote.Ctim != s.Ctim {
		return false
	}
	local, err := pathToFileInfo(filepath.Join(c.BasePath, remote.Name), false)
	if err != nil || !local.IsDir || local.Dev != marker.LocalDev || local.Inode != marker.LocalInode || local.Mtim != remote.Mtim {
		return false
	}
	c.Perf.Add(ResumeSubtreesSkipped, 1)
	Logger.Info().Msgf("Resume checkpoint: skipping completed subtree %s", remote.Name)
	return true
}

// Only wait for fastsync's files in this completed subtree. No syncfs/global
// drain; writers in other subtrees can continue while this one checkpoints.
func (m *fileFlusher) waitDirectory(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for {
		if m.err != nil {
			return m.err
		}
		pending := false
		for _, f := range m.files {
			rel, err := filepath.Rel(path, f.original.Name())
			if err == nil && filepath.IsLocal(rel) {
				if !f.closed {
					return fmt.Errorf("subtree still has an open writer")
				}
				pending = true
				m.schedule(f)
			}
		}
		if !pending {
			return nil
		}
		m.cond.Wait()
	}
}
