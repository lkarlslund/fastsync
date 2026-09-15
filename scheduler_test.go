package fastsync

import (
	"fmt"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type pausedCopyServer struct {
	*Server
	entered, release chan struct{}
	once             sync.Once
	fail             bool
}

func (s *pausedCopyServer) GetChunk(a GetChunkArgs, r *[]byte) error {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	if s.fail {
		return fmt.Errorf("injected copy failure")
	}
	return s.Server.GetChunk(a, r)
}

func TestCheckingContinuesPastBlockedCopyAndHardlinks(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			src, dst := t.TempDir(), t.TempDir()
			writeTestFile(t, src, "000-owner", "new payload")
			for n := 1; n <= 8; n++ {
				if err := os.Link(filepath.Join(src, "000-owner"), filepath.Join(src, fmt.Sprintf("%03d-link", n))); err != nil {
					t.Fatal(err)
				}
			}
			for n := 0; n < 12; n++ {
				name := fmt.Sprintf("z%03d-existing", n)
				writeTestFile(t, src, name, "unchanged")
				writeTestFile(t, dst, name, "unchanged")
				fixedFileTime(t, filepath.Join(src, name))
				fixedFileTime(t, filepath.Join(dst, name))
			}
			s := &pausedCopyServer{Server: NewServer(), entered: make(chan struct{}), release: make(chan struct{}), fail: fail}
			s.BasePath = src
			registry := rpc.NewServer()
			registerTestRPCServer(t, registry, s)
			c := newTestClient(dst)
			c.ParallelFile = 1
			c.QueueSize = 32
			c.PreserveHardlinks = true
			rpcClient := newTestRPCClientForServer(t, registry)
			done := make(chan error, 1)
			go func() { done <- c.Run(rpcClient) }()
			var release sync.Once
			defer release.Do(func() { close(s.release) })
			select {
			case <-s.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("copy not started")
			}
			deadline := time.Now().Add(3 * time.Second)
			for c.Perf.Get(FilesUnchanged) < 12 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if got := c.Perf.Get(FilesUnchanged); got != 12 {
				t.Fatalf("checking blocked behind copy/dependencies: %d unchanged", got)
			}
			if _, err := os.Lstat(filepath.Join(dst, "001-link")); !os.IsNotExist(err) {
				t.Fatal("published follower before successful owner")
			}
			release.Do(func() { close(s.release) })
			select {
			case err := <-done:
				if (err != nil) != fail {
					t.Fatalf("run error %v, fail=%v", err, fail)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("dependency drain deadlocked")
			}
			if peak := c.schedulingPeak.Load(); peak > 35 {
				t.Fatalf("unbounded scheduling: %d", peak)
			}
			if fail {
				if c.Perf.Get(FilesLinked) != 0 || c.Perf.Get(FilesCopied) != 0 {
					t.Fatal("failed files counted successful")
				}
			} else {
				if c.Perf.Get(FilesLinked) != 8 || c.Perf.Get(FilesCopied) != 1 {
					t.Fatalf("copy/link counts %d/%d", c.Perf.Get(FilesCopied), c.Perf.Get(FilesLinked))
				}
				if _, err := verifyTestArchive(t, src, dst); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestSchedulerBoundsAndDrainsManyDependencies(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	writeTestFile(t, src, "000", "data")
	for n := 1; n < 100; n++ {
		if err := os.Link(filepath.Join(src, "000"), filepath.Join(src, fmt.Sprintf("%03d", n))); err != nil {
			t.Fatal(err)
		}
	}
	c := runTestSync(t, src, dst, func(c *Client) { c.ParallelFile = 1; c.QueueSize = 1; c.PreserveHardlinks = true })
	if peak := c.schedulingPeak.Load(); peak > 4 {
		t.Fatalf("pending bound exceeded: %d", peak)
	}
	if c.Perf.Get(FilesLinked) != 99 {
		t.Fatalf("lost followers: %d", c.Perf.Get(FilesLinked))
	}
	c = runTestSync(t, src, dst, func(c *Client) { c.ParallelFile = 1; c.QueueSize = 1; c.PreserveHardlinks = true })
	if c.Perf.Get(FilesUnchanged) != 100 || c.Perf.Get(WrittenBytes) != 0 || c.Perf.Get(FilesLinked) != 0 {
		t.Fatal("resume was not all unchanged")
	}
}
