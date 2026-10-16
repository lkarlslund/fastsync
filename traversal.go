package fastsync

import (
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"unsafe"
)

type directoryListing struct {
	response FileListResponse
	err      error
	retained int64
}

func (l *directoryListing) retainedBytes() int64 {
	n := int64(len(l.response.ParentDirectory)) + int64(cap(l.response.Files))*int64(unsafe.Sizeof(FileInfo{}))
	for _, file := range l.response.Files {
		n += int64(len(file.Name) + len(file.LinkTo))
		if len(file.Xattrs) > 0 {
			n += 128
			for key, value := range file.Xattrs {
				n += int64(64 + len(key) + cap(value))
			}
		}
	}
	return n
}

// Alphabetical depth-first admission is independent of RPC completion order.
// Prefetch the nearest pending directories, without letting completed ancestor
// siblings occupy the RPC slots. Cached replies have a 64 MiB estimated budget;
// at most ParallelDir in-flight replies can overshoot it.
func (c *Client) walkAlphabetical(client *rpc.Client, root FileInfo) {
	pending := []FileInfo{root}
	futures := make(map[string]chan directoryListing)
	slots := make(chan struct{}, c.ParallelDir)
	lookAhead := 64 * c.ParallelDir
	var cachedBytes atomic.Int64
	fetchAcquired := func(item FileInfo) directoryListing {
		var result directoryListing
		result.err = client.Call("Server.List", item.Name, &result.response)
		return result
	}
	prefetch := func(current FileInfo) {
		for i := len(pending); i >= 0 && len(pending)-i < lookAhead; i-- {
			if len(futures) >= lookAhead || cachedBytes.Load() >= 64<<20 {
				return
			}
			item := current
			if i < len(pending) {
				item = pending[i]
			}
			if !item.IsDir {
				continue
			}
			if _, exists := futures[item.Name]; exists {
				continue
			}
			if c.ResumePosition && c.resume != nil {
				if _, complete := c.resume.subtrees[relativeResumePath(item.Name)]; complete {
					continue
				}
			}
			select {
			case slots <- struct{}{}:
				if c.warming && i < len(pending) {
					local, err := lstatNoFollow(filepath.Join(c.BasePath, item.Name))
					if err != nil || !local.IsDir() {
						<-slots
						return
					}
				}
				future := make(chan directoryListing, 1)
				futures[item.Name] = future
				go func() {
					defer func() { <-slots }()
					result := fetchAcquired(item)
					result.retained = result.retainedBytes()
					cachedBytes.Add(result.retained)
					future <- result
				}()
			default:
				return
			}
		}
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
				if c.isReference(item.Name) {
					c.recordError("reference directory missing: %s", item.Name)
				}
				continue
			}
			if err != nil {
				c.recordError("check existing directory %s: %v", item.Name, err)
				continue
			}
			if !local.IsDir {
				if c.isReference(item.Name) {
					c.recordError("reference is not a destination directory: %s", item.Name)
				}
				continue
			}
		} else if item.Name != root.Name {
			if err := c.prepareDirectory(item); err != nil {
				c.recordError("prepare directory %s: %v", item.Name, err)
				c.processedItemInDir(filepath.Dir(item.Name), true)
				continue
			}
			if c.CompletionStamps && topLevelDirectory(item) {
				if err := c.invalidateCompletionStamp(item); err != nil {
					c.recordError("invalidate completion stamp %s: %v", item.Name, err)
					c.processedItemInDir(filepath.Dir(item.Name), true)
					continue
				}
			}
		}
		if !c.warming {
			c.dircacheMu.Lock()
			c.dircache[item.Name] = &dirinfo{name: item.Name, info: item, remaining: -1}
			c.dircacheMu.Unlock()
		}
		prefetch(item)
		var result directoryListing
		if future, ok := futures[item.Name]; ok {
			result = <-future
			cachedBytes.Add(-result.retained)
			delete(futures, item.Name)
		} else {
			slots <- struct{}{}
			result = fetchAcquired(item)
			<-slots
		}
		if result.err != nil {
			c.recordError("list directory %s: %v", item.Name, result.err)
			c.processedItemInDir(item.Name, true)
			continue
		}
		entries := result.response.Files
		sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
		if item.Name == root.Name {
			entries, result.err = c.selectTopLevel(entries)
			if result.err != nil {
				c.recordError("select source entries: %v", result.err)
				c.processedItemInDir(item.Name, true)
				continue
			}
		}
		var extras []string
		listingFailed := false
		if c.Delete && !c.warming {
			names := make(map[string]bool, len(entries))
			for _, e := range entries {
				names[e.Name] = true
			}
			local, err := readDirNoFollow(filepath.Join(c.BasePath, item.Name))
			if err != nil {
				c.recordError("list destination directory %s: %v", item.Name, err)
				listingFailed = true
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
			c.dircache[item.Name].failed = listingFailed
			c.dircacheMu.Unlock()
		}
		if len(entries) == 0 {
			c.ProcessedItemInDir(item.Name)
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
