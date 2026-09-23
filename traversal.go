package fastsync

import (
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
)

type directoryListing struct {
	response FileListResponse
	err      error
}

// Alphabetical depth-first admission is independent of RPC completion order.
// At most ParallelDir sibling listings are prefetched. Their responses remain
// private until their turn; data copying still runs concurrently downstream.
func (c *Client) walkAlphabetical(client *rpc.Client, root FileInfo) {
	pending := []FileInfo{root}
	futures := make(map[string]chan directoryListing)
	slots := make(chan struct{}, c.ParallelDir)
	fetch := func(item FileInfo) directoryListing {
		slots <- struct{}{}
		defer func() { <-slots }()
		var result directoryListing
		result.err = client.Call("Server.List", item.Name, &result.response)
		return result
	}
	// Every launched request has a buffered result, and is joined even on errors.
	defer func() {
		for _, future := range futures {
			<-future
		}
		c.directoryPending.Store(0)
	}()
	c.directoryPending.Store(1)
	for len(pending) > 0 {
		last := len(pending) - 1
		item := pending[last]
		pending[last] = FileInfo{}
		pending = pending[:last]
		if !item.IsDir {
			c.filequeue <- item
			continue
		}
		c.directoryPending.Add(-1)
		if c.skipResumeDirectory(item) {
			c.ProcessedItemInDir(filepath.Dir(item.Name))
			continue
		}
		if c.warming {
			local, err := c.localFileInfo(filepath.Join(c.BasePath, item.Name))
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				c.recordError("check existing directory %s: %v", item.Name, err)
				continue
			}
			if !local.IsDir {
				continue
			}
		} else if item.Name != root.Name {
			if err := c.prepareDirectory(item); err != nil {
				c.recordError("prepare directory %s: %v", item.Name, err)
				c.ProcessedItemInDir(filepath.Dir(item.Name))
				continue
			}
		}
		if !c.warming {
			c.dircacheMu.Lock()
			c.dircache[item.Name] = &dirinfo{name: item.Name, info: item, remaining: -1}
			c.dircacheMu.Unlock()
		}
		var result directoryListing
		if future, ok := futures[item.Name]; ok {
			result = <-future
			delete(futures, item.Name)
		} else {
			result = fetch(item)
		}
		if result.err != nil {
			c.recordError("list directory %s: %v", item.Name, result.err)
			c.ProcessedItemInDir(item.Name)
			continue
		}
		entries := result.response.Files
		sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
		if item.Name == root.Name {
			entries, result.err = c.selectTopLevel(entries)
			if result.err != nil {
				c.recordError("select source entries: %v", result.err)
				c.ProcessedItemInDir(item.Name)
				continue
			}
		}
		var extras []string
		if c.Delete && !c.warming {
			names := make(map[string]bool, len(entries))
			for _, e := range entries {
				names[e.Name] = true
			}
			local, err := readDirNoFollow(filepath.Join(c.BasePath, item.Name))
			if err != nil {
				c.recordError("list destination directory %s: %v", item.Name, err)
			} else {
				for _, e := range local {
					if !names[filepath.Join(item.Name, e.Name())] {
						extras = append(extras, e.Name())
					}
				}
			}
		}
		if !c.warming {
			c.dircacheMu.Lock()
			c.dircache[item.Name].remaining = int64(len(entries))
			c.dircache[item.Name].extraentries = extras
			c.dircacheMu.Unlock()
		}
		if len(entries) == 0 {
			c.ProcessedItemInDir(item.Name)
		}
		// Prefetch in name order, but never enqueue files from these responses here.
		for _, e := range entries {
			if e.IsDir && len(futures) < c.ParallelDir {
				if c.ResumePosition && c.resume != nil {
					if _, ok := c.resume.subtrees[relativeResumePath(e.Name)]; ok {
						continue
					}
				}
				future := make(chan directoryListing, 1)
				futures[e.Name] = future
				go func(item FileInfo) { future <- fetch(item) }(e)
			}
		}
		for i := len(entries) - 1; i >= 0; i-- {
			pending = append(pending, entries[i])
			if entries[i].IsDir {
				c.directoryPending.Add(1)
			}
		}
		if !c.warming {
			c.Perf.Add(DirectoriesProcessed, 1)
		}
	}
}

func (c *Client) prepareDirectory(item FileInfo) error {
	path := filepath.Join(c.BasePath, item.Name)
	local, err := c.localFileInfo(path)
	if os.IsNotExist(err) {
		return c.timeMetadata(func() error { return mkdirAllNoFollow(path, 0755) })
	}
	if err != nil {
		return err
	}
	if local.IsDir {
		return nil
	}
	return c.timeMetadata(func() error {
		if err := removeNoFollow(path); err != nil {
			return err
		}
		return mkdirAllNoFollow(path, 0755)
	})
}
