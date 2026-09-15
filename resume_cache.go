package fastsync

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type resumeIdentity struct {
	Version                          VersionInfo
	Source                           string
	SourceRoot                       FileInfo
	Destination                      string
	DestinationDev, DestinationInode uint64
	Xattrs, Checksum                 bool
}
type resumeHint struct {
	Path       string
	Dev, Inode uint64
}
type resumeRecord struct {
	Identity *resumeIdentity `json:",omitempty"`
	Hint     *resumeHint     `json:",omitempty"`
	Subtree  *resumeSubtree  `json:",omitempty"`
	Complete bool
}

// The cache is a journal of hints, never proof that a file is complete. Every
// loaded hint goes through current source Stat and the normal reuse validation.
type resumeCache struct {
	mu         sync.Mutex
	subtrees   map[string]resumeSubtree
	file, lock *os.File
	writer     *bufio.Writer
	path, temp string
	identity   resumeIdentity
	failed     bool
	stop, done chan struct{}
}

func (c *Client) openResumeCache(root FileInfo) (*resumeCache, bool, error) {
	if c.ResumePosition && (c.ResumeCache == "" || c.flusher == nil) {
		return nil, false, fmt.Errorf("resume-position requires resume-cache, resume-id, and background file flushing")
	}
	if c.ResumeCache == "" {
		return nil, false, nil
	}
	if !c.PreserveHardlinks {
		return nil, false, fmt.Errorf("resume cache requires hardlink preservation")
	}
	if c.ResumeIdentity == "" {
		return nil, false, fmt.Errorf("resume cache requires a stable source identity")
	}
	dest, err := DirectoryPathNoFollow(c.BasePath)
	if err != nil {
		return nil, false, err
	}
	path, err := filepath.Abs(c.ResumeCache)
	if err != nil {
		return nil, false, err
	}
	parent, err := DirectoryPathNoFollow(filepath.Dir(path))
	if err != nil {
		return nil, false, err
	}
	rel, err := filepath.Rel(dest, filepath.Join(parent, filepath.Base(path)))
	if err != nil {
		return nil, false, err
	}
	if rel == "." || filepath.IsLocal(rel) {
		return nil, false, fmt.Errorf("resume cache must be outside the destination tree")
	}
	info, err := pathToFileInfo(dest, false)
	if err != nil {
		return nil, false, err
	}
	lock, err := OpenFileNoFollow(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, false, err
	}
	if err = lockResumeCache(lock); err != nil {
		lock.Close()
		return nil, false, fmt.Errorf("lock resume cache: %w", err)
	}
	// Root metadata is an additional guard; a stable snapshot identity is mandatory.
	root.Xattrs = nil
	root.Atim.Sec = 0
	root.Atim.Nsec = 0
	r := &resumeCache{subtrees: make(map[string]resumeSubtree), path: path, lock: lock, identity: resumeIdentity{Version: CurrentVersions(), Source: c.ResumeIdentity, SourceRoot: root, Destination: dest, DestinationDev: info.Dev, DestinationInode: info.Inode, Xattrs: c.Options.SendXattr, Checksum: c.AlwaysChecksum}}
	c.resume = r
	c.phase.Store(3)
	Logger.Info().Msg("Loading inode cache from disk and validating saved hints")
	loaded := c.loadResumeHints(r)
	Logger.Info().Msg("Inode cache loading finished")
	if loaded {
		r.file, err = OpenFileNoFollow(path, os.O_WRONLY|os.O_APPEND, 0)
	} else {
		r.file, err = createTempNoFollow(parent, ".fastsync-resume-")
		if err == nil {
			r.temp = r.file.Name()
		}
	}
	if err != nil {
		r.Close()
		c.resume = nil
		return nil, false, err
	}
	r.writer = bufio.NewWriterSize(r.file, 64*1024)
	if !loaded {
		r.record(resumeRecord{Identity: &r.identity})
		// Retain validated partial hints even when a full traversal is still needed.
		c.inodesMu.Lock()
		for _, entry := range c.inodes {
			if entry.seed != nil {
				c.saveResumeHint(entry.seed.source)
			}
		}
		c.inodesMu.Unlock()
		r.publish(false) // Make periodic checkpoints discoverable even during warmup.
	}
	r.stop = make(chan struct{})
	r.done = make(chan struct{})
	go func() {
		defer close(r.done)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				r.flush()
			case <-r.stop:
				return
			}
		}
	}()
	return r, loaded, nil
}

func (c *Client) loadResumeHints(r *resumeCache) bool {
	file, err := openNoFollow(r.path)
	if os.IsNotExist(err) {
		Logger.Info().Msg("Resume cache not found; scanning existing paths")
		return false
	}
	if err != nil {
		Logger.Warn().Err(err).Msg("Resume cache unavailable; scanning existing paths")
		return false
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	if !scanner.Scan() {
		Logger.Warn().Err(scanner.Err()).Msg("Resume cache header missing or unreadable; scanning existing paths")
		return false
	}
	var header resumeRecord
	if json.Unmarshal(scanner.Bytes(), &header) != nil || header.Identity == nil {
		Logger.Warn().Msg("Resume cache header invalid; scanning existing paths")
		return false
	}
	before, _ := json.Marshal(header.Identity)
	now, _ := json.Marshal(r.identity)
	if string(before) != string(now) {
		var previous, current map[string]json.RawMessage
		_ = json.Unmarshal(before, &previous)
		_ = json.Unmarshal(now, &current)
		var changed []string
		for key, value := range current {
			if string(previous[key]) != string(value) {
				changed = append(changed, key)
			}
		}
		sort.Strings(changed)
		Logger.Info().Strs("changed_fields", changed).Msg("Resume cache identity changed; scanning existing paths")
		return false
	}
	// Bound validation work, including RPCs; don't load a second full hint list.
	jobs := make(chan resumeHint, c.ParallelFile)
	var workers sync.WaitGroup
	var validationMu sync.Mutex
	valid := true
	failures := make(map[string]int)
	validated := 0
	reject := func(reason, path string, err error) {
		validationMu.Lock()
		defer validationMu.Unlock()
		valid = false
		failures[reason]++
		// One example per reason keeps large stale caches from flooding the log.
		if failures[reason] == 1 {
			Logger.Warn().Str("reason", reason).Str("path", path).Err(err).Msg("Resume cache validation failed (first example)")
		}
	}
	for i := 0; i < c.ParallelFile; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for hint := range jobs {
				var remote FileInfo
				err := c.remoteClient.Call("Server.Stat", hint.Path, &remote)
				reason := "source_stat_error"
				if err == nil && (remote.Dev != hint.Dev || remote.Inode != hint.Inode || remote.Nlink <= 1) {
					reason = "source_inode_or_link_count_changed"
					err = fmt.Errorf("expected device/inode %d/%d, got %d/%d with %d links", hint.Dev, hint.Inode, remote.Dev, remote.Inode, remote.Nlink)
				}
				if err == nil {
					reason = "destination_validation_error"
					err = c.warmExistingFile(c.remoteClient, remote)
				}
				c.inodesMu.Lock()
				present := c.inodes[inodeKey{hint.Dev, hint.Inode}] != nil
				c.inodesMu.Unlock()
				if err != nil {
					reject(reason, hint.Path, err)
				} else if !present {
					reject("destination_not_reusable", hint.Path, nil)
				} else {
					validationMu.Lock()
					validated++
					validationMu.Unlock()
				}
				c.Perf.Add(ExistingExamined, 1)
			}
		}()
	}
	complete := false
	line := 1
	for scanner.Scan() {
		line++
		var record resumeRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			reject("malformed_record", fmt.Sprintf("journal line %d", line), err)
			break
		}
		if record.Subtree != nil && record.Hint == nil && record.Identity == nil && !record.Complete {
			name := relativeResumePath(record.Subtree.Source.Name)
			if !filepath.IsLocal(name) || filepath.Dir(name) != "." {
				reject("invalid_subtree_record", fmt.Sprintf("journal line %d", line), nil)
				break
			}
			r.subtrees[name] = *record.Subtree
			continue
		}
		if record.Complete && record.Hint == nil && record.Identity == nil && record.Subtree == nil {
			complete = true
			continue
		}
		if record.Hint == nil || record.Identity != nil || record.Complete || record.Subtree != nil || !filepath.IsLocal(record.Hint.Path) {
			reject("invalid_hint_record", fmt.Sprintf("journal line %d", line), nil)
			break
		}
		jobs <- *record.Hint
	}
	close(jobs)
	workers.Wait()
	if scanner.Err() != nil {
		reject("journal_read_error", fmt.Sprintf("after journal line %d", line), scanner.Err())
	}
	if !complete {
		reject("completion_marker_missing", "", nil)
	}
	Logger.Info().Int("validated_hints", validated).Interface("failure_counts", failures).Bool("completion_marker", complete).Msg("Resume cache validation summary")
	if !valid || !complete {
		r.subtrees = make(map[string]resumeSubtree)
		Logger.Info().Msg("Resume cache incomplete or stale; scanning existing paths")
		return false
	}
	Logger.Info().Msg("Resume cache validated; skipping existing-path traversal")
	return true
}
func (c *Client) saveResumeHint(remote FileInfo) {
	if c.resume == nil || remote.Nlink <= 1 {
		return
	}
	// RPC names may have a leading slash; the cache stores relative paths only.
	path := filepath.Clean(remote.Name)
	if filepath.IsAbs(path) {
		path = path[1:]
	}
	if !filepath.IsLocal(path) {
		return
	}
	c.resume.record(resumeRecord{Hint: &resumeHint{Path: path, Dev: remote.Dev, Inode: remote.Inode}})
}
func (r *resumeCache) record(record resumeRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failed || r.writer == nil {
		return
	}
	if err := json.NewEncoder(r.writer).Encode(record); err != nil {
		r.failed = true
		Logger.Warn().Msg("Resume cache write failed; transfer continues")
	}
}
func (r *resumeCache) flush() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failed || r.writer == nil {
		return
	}
	if err := errors.Join(r.writer.Flush(), r.file.Sync()); err != nil {
		r.failed = true
		Logger.Warn().Msg("Resume cache checkpoint failed; transfer continues")
	}
}
func (r *resumeCache) publish(complete bool) {
	if complete {
		r.record(resumeRecord{Complete: true})
	}
	r.flush()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failed || r.temp == "" {
		return
	}
	if err := renameNoFollow(r.temp, r.path); err != nil {
		r.failed = true
		Logger.Warn().Msg("Resume cache publication failed; transfer continues")
		return
	}
	r.temp = ""
	if err := syncParent(r.path); err != nil {
		Logger.Warn().Msg("Resume cache directory flush failed")
	}
}
func (r *resumeCache) Close() {
	if r.stop != nil {
		close(r.stop)
		<-r.done
	}
	if r.temp != "" {
		r.publish(false)
	} else {
		r.flush()
	}
	if r.file != nil {
		r.file.Close()
	}
	if r.temp != "" {
		removeNoFollow(r.temp)
	}
	if r.lock != nil {
		r.lock.Close()
	}
}
