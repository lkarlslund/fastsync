package fastsync

import (
	"fmt"
	"net/rpc"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

type shuffledListingServer struct {
	*Server
	reverseDelay bool
	active, peak atomic.Int64
}

func (s *shuffledListingServer) List(path string, r *FileListResponse) error {
	n := s.active.Add(1)
	defer s.active.Add(-1)
	for old := s.peak.Load(); n > old; old = s.peak.Load() {
		if s.peak.CompareAndSwap(old, n) {
			break
		}
	}
	delay := len(path) % 3
	if s.reverseDelay {
		delay = 2 - delay
	}
	time.Sleep(time.Duration(delay) * time.Millisecond)
	if err := s.Server.List(path, r); err != nil {
		return err
	}
	slices.Reverse(r.Files)
	return nil
}
func TestAlphabeticalTraversalIgnoresListingCompletionOrder(t *testing.T) {
	src := t.TempDir()
	for _, name := range []string{"z/f", "a/sub/f", "a/sub/g", "a/a", "b", "a.txt", "202602/f", "202601/f"} {
		writeTestFile(t, src, name, name)
	}
	want := []string{"/202601/f", "/202602/f", "/a/a", "/a/sub/f", "/a/sub/g", "/a.txt", "/b", "/z/f"}
	for _, parallel := range []int{1, 2, 8} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/%v", parallel, reverse), func(t *testing.T) {
				s := &shuffledListingServer{Server: NewServer(), reverseDelay: reverse}
				s.BasePath = src
				registry := rpc.NewServer()
				registerTestRPCServer(t, registry, s)
				r := newTestRPCClientForServer(t, registry)
				c := newTestClient(t.TempDir())
				c.ParallelDir = parallel
				c.remoteClient = r
				if err := c.hello(r); err != nil {
					t.Fatal(err)
				}
				var root FileInfo
				if err := r.Call("Server.Stat", "/", &root); err != nil {
					t.Fatal(err)
				}
				c.filequeue = make(chan FileInfo, 1)
				done := make(chan struct{})
				go func() { c.walkAlphabetical(r, root); close(c.filequeue); close(done) }()
				var got []string
				for f := range c.filequeue {
					got = append(got, f.Name)
					c.ProcessedItemInDir(filepath.Dir(f.Name))
				}
				<-done
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("order %v, want %v", got, want)
				}
				if s.peak.Load() > int64(parallel) {
					t.Fatal("listing concurrency exceeded")
				}
				if err := c.runError(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestAlphabeticalResumeKeepsExistingCanonicalInode(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	writeTestFile(t, src, "a/file", "payload")
	fixedFileTime(t, filepath.Join(src, "a/file"))
	for _, d := range []string{"b", "c"} {
		if err := os.MkdirAll(filepath.Join(src, d), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(filepath.Join(src, "a/file"), filepath.Join(src, d, "file")); err != nil {
			t.Fatal(err)
		}
	}
	writeTestFile(t, dst, "a/file", "payload")
	fixedFileTime(t, filepath.Join(dst, "a/file"))
	before, err := os.Stat(filepath.Join(dst, "a/file"))
	if err != nil {
		t.Fatal(err)
	}
	for _, parallel := range []int{8, 1, 4} {
		c := runTestSync(t, src, dst, func(c *Client) { c.ParallelDir = parallel; c.PreserveHardlinks = true })
		after, err := os.Stat(filepath.Join(dst, "a/file"))
		if err != nil {
			t.Fatal(err)
		}
		if !os.SameFile(before, after) || c.Perf.Get(WrittenBytes) != 0 {
			t.Fatal("resume replaced/copied canonical inode")
		}
		if parallel != 8 && c.Perf.Get(FilesLinked) != 0 {
			t.Fatal("resume relinked existing history")
		}
	}
}

func TestCheckCompletionReorderingPreservesAdmission(t *testing.T) {
	o := orderedChecks{pending: make(map[uint64]fileCompletion)}
	var got []uint64
	finish := func(r fileCompletion) { got = append(got, r.job.checkSequence) }
	for _, n := range []uint64{3, 1, 2} {
		o.complete(fileCompletion{job: &scheduledFile{checkSequence: n}}, finish)
	}
	if len(got) != 0 {
		t.Fatal("later checks overtook first")
	}
	o.complete(fileCompletion{job: &scheduledFile{checkSequence: 0}}, finish)
	if !reflect.DeepEqual(got, []uint64{0, 1, 2, 3}) || len(o.pending) != 0 {
		t.Fatalf("order %v", got)
	}
}
