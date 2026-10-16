//go:build linux

package fastsync

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type referenceCountServer struct {
	*Server
	lists sync.Map
	stats atomic.Int64
}

func (s *referenceCountServer) List(path string, reply *FileListResponse) error {
	v, _ := s.lists.LoadOrStore(path, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
	return s.Server.List(path, reply)
}

func (s *referenceCountServer) Stat(path string, reply *FileInfo) error {
	s.stats.Add(1)
	return s.Server.Stat(path, reply)
}

func TestReferenceScannedOnceAndRetainsItsInode(t *testing.T) {
	src, dst := makeABC(t)
	// A partial older copy must adopt the completed reference's inode.
	writeTestFile(t, dst, "a/file", "shared data")
	fixedFileTime(t, filepath.Join(dst, "a/file"))
	s := &referenceCountServer{Server: NewServer()}
	s.BasePath = src
	registry := rpc.NewServer()
	registerTestRPCServer(t, registry, s)
	before, _ := os.Stat(filepath.Join(dst, "b/file"))
	dirBefore, _ := os.Stat(filepath.Join(dst, "b"))
	c := newTestClient(dst)
	c.PreserveHardlinks = true
	c.Include, c.Reference = []string{"a"}, []string{"b"}
	if err := c.Run(newTestRPCClientForServer(t, registry)); err != nil {
		t.Fatal(err)
	}
	v, ok := s.lists.Load("/b")
	if !ok || v.(*atomic.Int64).Load() != 1 {
		t.Fatal("reference traversed more than once")
	}
	for _, p := range []string{"a/file", "b/file"} {
		after, err := os.Stat(filepath.Join(dst, p))
		if err != nil || !os.SameFile(before, after) {
			t.Fatalf("%s did not reuse the reference inode: %v", p, err)
		}
	}
	dirAfter, _ := os.Stat(filepath.Join(dst, "b"))
	if !dirAfter.ModTime().Equal(dirBefore.ModTime()) || c.Perf.Get(WrittenBytes) != 0 {
		t.Fatal("reference modified or file data copied")
	}
}

func TestReferenceFailureStopsBeforeCopy(t *testing.T) {
	src, dst := makeABC(t)
	if err := os.Remove(filepath.Join(dst, "b/file")); err != nil {
		t.Fatal(err)
	}
	c := newTestClient(dst)
	c.PreserveHardlinks = true
	c.Include, c.Reference = []string{"a"}, []string{"b"}
	if err := c.Run(newTestRPCClient(t, src)); err == nil {
		t.Fatal("missing reference accepted")
	}
	assertNotExists(t, filepath.Join(dst, "a"))
}

func TestReferenceValidationAndOverlap(t *testing.T) {
	for _, ref := range []string{"../outside", ".", "a/b", "a*"} {
		c := NewClient()
		c.Include, c.Reference = []string{"copy"}, []string{ref}
		if c.validateIncludes() == nil {
			t.Fatalf("accepted %q", ref)
		}
	}
	src, dst := makeABC(t)
	c := newTestClient(dst)
	c.PreserveHardlinks = true
	c.Include, c.Reference = []string{"b"}, []string{"b"}
	if err := c.Run(newTestRPCClient(t, src)); err == nil {
		t.Fatal("overlap accepted")
	}
}

type selectiveReadFailure struct{ *Server }

func (s *selectiveReadFailure) GetChunk(args GetChunkArgs, reply *[]byte) error {
	if strings.HasPrefix(args.Path, "/bad/") {
		return errors.New("injected subtree failure")
	}
	return s.Server.GetChunk(args, reply)
}

func TestCompletionStampWaitsForFlush(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "flush-failure"}[fail], func(t *testing.T) {
			src, dst := t.TempDir(), t.TempDir()
			writeTestFile(t, src, "generation/deep/file", "data")
			writeTestFile(t, dst, "generation/deep/file", "old unfinished content")
			fixedFileTime(t, filepath.Join(src, "generation"))
			fixedFileTime(t, filepath.Join(dst, "generation"))
			remote, _ := os.Stat(filepath.Join(src, "generation"))
			c := newTestClient(dst)
			c.CompletionStamps = true
			var calls atomic.Int64
			c.checkpointOverride = func() error {
				calls.Add(1)
				local, err := os.Stat(filepath.Join(dst, "generation"))
				if err != nil || local.ModTime().Equal(remote.ModTime()) {
					return errors.New("completion timestamp published before data flush")
				}
				if fail {
					return errors.New("injected flush failure")
				}
				return nil
			}
			err := c.Run(newTestRPCClient(t, src))
			if (err != nil) != fail || calls.Load() == 0 {
				t.Fatalf("flush result: %v, calls %d", err, calls.Load())
			}
			local, _ := os.Stat(filepath.Join(dst, "generation"))
			if local.ModTime().Equal(remote.ModTime()) == fail {
				t.Fatal("completion timestamp does not reflect durability")
			}
		})
	}
}

func BenchmarkCompletedReference(b *testing.B) {
	src, dst := b.TempDir(), b.TempDir()
	for _, root := range []string{src, dst} {
		for _, generation := range []string{"anchor", "pending"} {
			if err := os.MkdirAll(filepath.Join(root, generation), 0755); err != nil {
				b.Fatal(err)
			}
		}
	}
	for i := 0; i < 256; i++ {
		name := fmt.Sprintf("file-%04d", i)
		for _, root := range []string{src, dst} {
			path := filepath.Join(root, "anchor", name)
			if err := os.WriteFile(path, nil, 0644); err != nil {
				b.Fatal(err)
			}
			if err := os.Link(path, filepath.Join(root, "pending", name)); err != nil {
				b.Fatal(err)
			}
			if err := os.Chtimes(path, time.Unix(100, 0), time.Unix(100, 0)); err != nil {
				b.Fatal(err)
			}
		}
	}
	for _, reference := range []bool{false, true} {
		b.Run(fmt.Sprint(reference), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				s := &referenceCountServer{Server: NewServer()}
				s.BasePath = src
				registry := rpc.NewServer()
				if err := registry.RegisterName("Server", s); err != nil {
					b.Fatal(err)
				}
				a, z := net.Pipe()
				done := make(chan struct{})
				go func() { registry.ServeConn(a); close(done) }()
				r := rpc.NewClient(z)
				c := NewClient()
				c.BasePath, c.FlushInterval = dst, 0
				c.Include = []string{"anchor", "pending"}
				if reference {
					c.Include, c.Reference = []string{"pending"}, []string{"anchor"}
				}
				if err := c.Run(r); err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(s.stats.Load()), "stat-rpc/op")
				_ = r.Close()
				<-done
				s.CloseFiles()
			}
		})
	}
}

func TestFailedSubtreeDoesNotPreventSiblingCompletion(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	for _, name := range []string{"bad", "good"} {
		writeTestFile(t, src, name+"/nested/file", "payload")
		fixedFileTime(t, filepath.Join(src, name))
	}
	s := NewServer()
	s.BasePath = src
	registry := rpc.NewServer()
	registerTestRPCServer(t, registry, &selectiveReadFailure{s})
	c := newTestClient(dst)
	c.CompletionStamps = true
	if err := c.Run(newTestRPCClientForServer(t, registry)); err == nil {
		t.Fatal("failed subtree returned success")
	}
	for _, name := range []string{"bad", "good"} {
		a, _ := os.Stat(filepath.Join(src, name))
		b, _ := os.Stat(filepath.Join(dst, name))
		if a.ModTime().Equal(b.ModTime()) != (name == "good") {
			t.Fatalf("incorrect completion stamp for %s", name)
		}
	}
}
