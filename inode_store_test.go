package fastsync

import (
	"bytes"
	"errors"
	"fmt"
	"net/rpc"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func spillFixture(t *testing.T, groups int) (string, string) {
	t.Helper()
	src, dst := t.TempDir(), t.TempDir()
	for _, generation := range []string{"a", "b", "c", "excluded"} {
		if err := os.Mkdir(filepath.Join(src, generation), 0755); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < groups; i++ {
		name := fmt.Sprintf("item-%04d", i)
		writeTestFile(t, src, "a/"+name, "test data")
		fixedFileTime(t, filepath.Join(src, "a", name))
		for _, generation := range []string{"b", "c", "excluded"} {
			if err := os.Link(filepath.Join(src, "a", name), filepath.Join(src, generation, name)); err != nil {
				t.Fatal(err)
			}
		}
	}
	runTestSync(t, src, dst, func(c *Client) { c.PreserveHardlinks = true; c.Include = []string{"a"} })
	return src, dst
}

func TestSpillPreservesReferenceLinksAndUniqueByteAccounting(t *testing.T) {
	const groups = 1200
	src, dst := spillFixture(t, groups)
	spill := t.TempDir()
	c := runTestSync(t, src, dst, func(c *Client) {
		c.PreserveHardlinks = true
		c.Include, c.Reference = []string{"b", "c"}, []string{"a"}
		c.InodeSpillDirectory, c.InodeMemoryGroups = spill, 3
		c.QueueSize, c.ParallelFile = 1, 4
	})
	if len(c.inodes) > 3 || len(c.localOwners) >= 3 {
		t.Fatalf("unbounded in-memory maps: %d groups, %d owners", len(c.inodes), len(c.localOwners))
	}
	if c.Perf.Get(WrittenBytes) != 0 || c.Perf.Get(BytesUniqueProcessed) != groups*9 || c.Perf.Get(FilesLinked) != groups*2 {
		t.Fatalf("reuse/accounting failed: writes=%d unique=%d links=%d", c.Perf.Get(WrittenBytes), c.Perf.Get(BytesUniqueProcessed), c.Perf.Get(FilesLinked))
	}
	for i := 0; i < groups; i++ {
		name := fmt.Sprintf("item-%04d", i)
		a, _ := os.Stat(filepath.Join(dst, "a", name))
		for _, generation := range []string{"b", "c"} {
			z, err := os.Stat(filepath.Join(dst, generation, name))
			if err != nil || !os.SameFile(a, z) {
				t.Fatalf("spilled reference group lost: %s/%s: %v", generation, name, err)
			}
		}
	}
	entries, err := os.ReadDir(spill)
	if err != nil || len(entries) != 0 {
		t.Fatalf("temporary inode database leaked: %v, %v", entries, err)
	}
	v := newTestClient(dst)
	v.PreserveHardlinks, v.Include = true, []string{"a", "b", "c"}
	var report bytes.Buffer
	if err := v.Verify(newTestRPCClient(t, src), &report); err != nil {
		t.Fatalf("spilled transfer did not verify: %v\n%s", err, report.String())
	}
}

func TestSpillNewGroupsCopyOnlyOnce(t *testing.T) {
	const groups = 96
	src, _ := spillFixture(t, groups)
	dst := t.TempDir()
	c := runTestSync(t, src, dst, func(c *Client) {
		c.PreserveHardlinks, c.Include = true, []string{"a", "b", "c"}
		c.InodeSpillDirectory, c.InodeMemoryGroups = t.TempDir(), 2
		c.QueueSize, c.ParallelFile = 1, 4
	})
	if c.Perf.Get(WrittenBytes) != groups*9 || c.Perf.Get(BytesUniqueProcessed) != groups*9 || c.Perf.Get(FilesLinked) != groups*2 {
		t.Fatalf("spilled new groups were recopied: writes=%d unique=%d links=%d", c.Perf.Get(WrittenBytes), c.Perf.Get(BytesUniqueProcessed), c.Perf.Get(FilesLinked))
	}
	if len(c.inodes) > 2 {
		t.Fatal("new-group cache exceeded its bound")
	}
}

func TestSpillFailureStopsCopyWithoutCompletionStamp(t *testing.T) {
	src, dst := spillFixture(t, 8)
	c := newTestClient(dst)
	c.PreserveHardlinks, c.CompletionStamps = true, true
	c.Include, c.Reference = []string{"b"}, []string{"a"}
	c.InodeSpillDirectory, c.InodeMemoryGroups = t.TempDir(), 1
	s := &spillBoundaryServer{Server: NewServer()}
	s.BasePath = src
	s.change = func() error {
		c.inodesMu.Lock()
		defer c.inodesMu.Unlock()
		if err := c.inodeStore.flush(); err != nil {
			return err
		}
		return c.inodeStore.db.Close()
	}
	registry := rpc.NewServer()
	registerTestRPCServer(t, registry, s)
	if err := c.Run(newTestRPCClientForServer(t, registry)); err == nil {
		t.Fatal("closed inode database accepted")
	}
	assertNotExists(t, filepath.Join(dst, "b/item-0000"))
	a, _ := os.Stat(filepath.Join(src, "b"))
	b, err := os.Stat(filepath.Join(dst, "b"))
	if err == nil && a.ModTime().Equal(b.ModTime()) {
		t.Fatal("failed cache produced a completion timestamp")
	}
}

func TestSpilledSeedCodecPreservesRawPaths(t *testing.T) {
	name := string([]byte{'/', 'a', '/', 0xff, '\n'})
	e := &inodeinfo{localhardlinkpath: name, remaining: 123, bytesCounted: true, seed: &reuseSeed{source: FileInfo{Name: name, LinkTo: name}}}
	e.seed.local.LinkTo = name
	b, err := encodeStoredInode(e)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeStoredInode(b)
	if err != nil || got.localhardlinkpath != name || got.seed.source.Name != name || got.seed.local.LinkTo != name || !got.bytesCounted || got.remaining != 123 {
		t.Fatalf("spilled record lost state or raw path: %v", err)
	}
	if _, err := decodeStoredInode([]byte("truncated")); err == nil {
		t.Fatal("truncated record accepted")
	}
}

func TestSpilledFailuresRemainFailures(t *testing.T) {
	e := &inodeinfo{localhardlinkpath: "/archive/generation/file", remaining: 40,
		err: errors.New("original copy failed"), seedErr: errors.New("seed failed")}
	value, err := encodeStoredInode(e)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeStoredInode(value)
	if err != nil || got.err == nil || got.err.Error() != e.err.Error() || got.seedErr == nil || got.seedErr.Error() != e.seedErr.Error() {
		t.Fatalf("lost spilled failure: %+v, %v", got, err)
	}
	got.seedOnce.Do(func() { t.Fatal("failed seed validation retried after spilling") })
}

func TestInodeStoreFindsBufferedAndFlushedRecords(t *testing.T) {
	s, err := openInodeStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	key := inodeKey{1, 2}
	e := &inodeinfo{localhardlinkpath: "/archive/generation/file", remaining: 40}
	if err := s.put(key, e); err != nil {
		t.Fatal(err)
	}
	for _, flush := range []bool{false, true} {
		if flush {
			if err := s.flush(); err != nil {
				t.Fatal(err)
			}
		}
		path, err := s.canonicalPath(key)
		if err != nil || path != e.localhardlinkpath {
			t.Fatalf("canonical path flush=%v: %q %v", flush, path, err)
		}
	}
	if err := s.remove(key); err != nil {
		t.Fatal(err)
	}
	if path, err := s.canonicalPath(key); err != nil || path != "" || s.keys != 0 {
		t.Fatalf("removed record remains: %q %v keys=%d", path, err, s.keys)
	}
}

type spillBoundaryServer struct {
	*Server
	roots  atomic.Int64
	change func() error
}

func (s *spillBoundaryServer) List(path string, reply *FileListResponse) error {
	if path == "/" && s.roots.Add(1) == 3 {
		if err := s.change(); err != nil {
			return err
		}
	}
	return s.Server.List(path, reply)
}

func TestSpilledReferenceRevalidatesBeforeUse(t *testing.T) {
	src, dst := spillFixture(t, 8)
	c := newTestClient(dst)
	c.PreserveHardlinks = true
	c.Include, c.Reference = []string{"b"}, []string{"a"}
	c.InodeSpillDirectory, c.InodeMemoryGroups = t.TempDir(), 1
	s := &spillBoundaryServer{Server: NewServer()}
	s.BasePath = src
	s.change = func() error {
		path := filepath.Join(dst, "a/item-0000")
		if err := os.Remove(path); err != nil {
			return err
		}
		return os.WriteFile(path, []byte("bad data!"), 0644)
	}
	registry := rpc.NewServer()
	registerTestRPCServer(t, registry, s)
	if err := c.Run(newTestRPCClientForServer(t, registry)); err == nil {
		t.Fatal("changed spilled reference accepted")
	}
	assertNotExists(t, filepath.Join(dst, "b/item-0000"))
}

func TestSpilledOwnersRejectIndependentInodeMerge(t *testing.T) {
	c := NewClient()
	c.InodeMemoryGroups = 2
	store, err := openInodeStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c.inodeStore = store
	defer store.close()
	for i := uint64(1); i <= 8; i++ {
		conflict, err := c.conflictingLocalInode(FileInfo{Dev: 1, Inode: i}, FileInfo{Dev: 2, Inode: i})
		if err != nil || conflict {
			t.Fatalf("initial owner: %v %v", conflict, err)
		}
	}
	conflict, err := c.conflictingLocalInode(FileInfo{Dev: 1, Inode: 1}, FileInfo{Dev: 2, Inode: 99})
	if err != nil || !conflict {
		t.Fatalf("spilled owner allowed independent source: %v %v", conflict, err)
	}
}
