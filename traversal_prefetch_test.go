package fastsync

import (
	"fmt"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type readAheadServer struct {
	*Server
	gate         <-chan struct{}
	release      func()
	delay        time.Duration
	active, peak atomic.Int64
	branchLists  atomic.Int64
}

func (s *readAheadServer) List(path string, reply *FileListResponse) error {
	if path == "/a" || strings.HasPrefix(path, "/a/") {
		s.branchLists.Add(1)
	}
	if strings.HasPrefix(path, "/a/") {
		n := s.active.Add(1)
		defer s.active.Add(-1)
		for old := s.peak.Load(); n > old; old = s.peak.Load() {
			if s.peak.CompareAndSwap(old, n) {
				break
			}
		}
		if s.gate != nil {
			if n > 1 {
				s.release()
			}
			<-s.gate
		}
		if s.delay > 0 {
			time.Sleep(s.delay)
		}
	}
	return s.Server.List(path, reply)
}

func setupReadAhead(t testing.TB, leaves, parallel int, server *readAheadServer) (*Client, *rpc.Client, FileInfo) {
	t.Helper()
	src, dst := t.TempDir(), t.TempDir()
	for _, base := range []string{src, dst} {
		for branch := 'a'; branch <= 'h'; branch++ {
			if err := os.MkdirAll(filepath.Join(base, string(branch)), 0755); err != nil {
				t.Fatal(err)
			}
		}
		for i := range leaves {
			if err := os.MkdirAll(filepath.Join(base, "a", fmt.Sprintf("leaf-%04d", i)), 0755); err != nil {
				t.Fatal(err)
			}
		}
	}
	for i := range leaves {
		if err := os.WriteFile(filepath.Join(src, "a", fmt.Sprintf("leaf-%04d", i), "file"), []byte("example"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	server.Server = NewServer()
	server.BasePath = src
	t.Cleanup(server.CloseFiles)
	registry := rpc.NewServer()
	if err := registry.RegisterName("Server", server); err != nil {
		t.Fatal(err)
	}
	r := newTestRPCClientForServer(t, registry)
	c := newTestClient(dst)
	c.ParallelDir = parallel
	c.warming = true
	c.remoteClient = r
	if err := c.hello(r); err != nil {
		t.Fatal(err)
	}
	var root FileInfo
	if err := r.Call("Server.Stat", "/", &root); err != nil {
		t.Fatal(err)
	}
	return c, r, root
}

func collectReadAhead(t testing.TB, c *Client, r *rpc.Client, root FileInfo) []string {
	t.Helper()
	c.filequeue = make(chan FileInfo, 1)
	done := make(chan struct{})
	go func() {
		c.walkAlphabetical(r, root)
		close(c.filequeue)
		close(done)
	}()
	var paths []string
	for file := range c.filequeue {
		paths = append(paths, file.Name)
	}
	<-done
	if err := c.runError(); err != nil {
		t.Fatal(err)
	}
	return paths
}

func TestReadAheadDoesNotParkAllSlotsAtAncestorSiblings(t *testing.T) {
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	s := &readAheadServer{gate: gate, release: release}
	c, r, root := setupReadAhead(t, 32, 8, s)
	timer := time.AfterFunc(time.Second, release)
	t.Cleanup(func() { timer.Stop(); release() })
	paths := collectReadAhead(t, c, r, root)
	if len(paths) != 32 {
		t.Fatalf("visited %d files, want 32", len(paths))
	}
	if peak := s.peak.Load(); peak < 2 || peak > 8 {
		t.Fatalf("deep listing concurrency = %d, want 2..8", peak)
	}
}

func TestReadAheadDoesNotListMissingWarmSubtrees(t *testing.T) {
	s := &readAheadServer{}
	c, r, root := setupReadAhead(t, 32, 8, s)
	if err := os.RemoveAll(filepath.Join(c.BasePath, "a")); err != nil {
		t.Fatal(err)
	}
	if paths := collectReadAhead(t, c, r, root); len(paths) != 0 {
		t.Fatalf("visited missing subtree: %v", paths)
	}
	if calls := s.branchLists.Load(); calls != 0 {
		t.Fatalf("listed missing warm subtree %d times", calls)
	}
}

func TestReadAheadAccountsForRetainedListingStorage(t *testing.T) {
	listing := directoryListing{response: FileListResponse{Files: make([]FileInfo, 0, 8)}}
	base := listing.retainedBytes()
	if base <= 0 {
		t.Fatal("slice capacity was not accounted")
	}
	listing.response.Files = listing.response.Files[:1]
	listing.response.Files[0] = FileInfo{Name: strings.Repeat("x", 1024), LinkTo: strings.Repeat("y", 2048),
		Xattrs: map[string][]byte{"user.example": make([]byte, 1, 1024)}}
	if got := listing.retainedBytes(); got < base+1024+2048+1024 {
		t.Fatalf("retained listing charge %d omits dynamic storage", got)
	}
}

func BenchmarkDirectoryReadAhead(b *testing.B) {
	for _, parallel := range []int{1, 8} {
		b.Run(fmt.Sprintf("parallel=%d", parallel), func(b *testing.B) {
			s := &readAheadServer{delay: 250 * time.Microsecond}
			c, r, root := setupReadAhead(b, 128, parallel, s)
			b.ResetTimer()
			for b.Loop() {
				if paths := collectReadAhead(b, c, r, root); len(paths) != 128 {
					b.Fatalf("visited %d files, want 128", len(paths))
				}
			}
		})
	}
}
