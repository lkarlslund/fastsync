package fastsync

import (
	"errors"
	"net/rpc"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func TestBackgroundFlushDoesNotPauseOtherFiles(t *testing.T) {
	c := NewClient()
	c.FlushBytes = 8192
	c.FlushFiles = 2
	c.FlushWorkers = 1
	c.FlushInterval = time.Hour
	c.BlockSize = 4096
	if err := c.initPipeline(); err != nil {
		t.Fatal(err)
	}
	entered, unblock := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	c.checkpointOverride = func() error {
		if calls.Add(1) == 1 {
			close(entered)
			<-unblock
		}
		return nil
	}
	c.flusher = newFileFlusher(c)
	a, err := os.CreateTemp(t.TempDir(), "a")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := os.CreateTemp(t.TempDir(), "b")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if _, err := c.writeData(a, make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	c.finishDataFile(a)
	<-entered
	wrote := make(chan error, 1)
	go func() { _, e := c.writeData(b, make([]byte, 4096)); wrote <- e }()
	select {
	case err := <-wrote:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("one file flush paused another file")
	}
	// A third block must backpressure because the configured dirty-byte budget is full.
	go func() { _, e := c.writeData(b, make([]byte, 4096)); wrote <- e }()
	select {
	case <-wrote:
		t.Fatal("exceeded pending-byte budget")
	case <-time.After(20 * time.Millisecond):
	}
	c.flusher.mu.Lock()
	pending := c.flusher.pending
	files := len(c.flusher.files)
	c.flusher.mu.Unlock()
	if pending > 8192 || files > 2 {
		t.Fatal("exceeded bounds")
	}
	close(unblock)
	select {
	case err := <-wrote:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("budget not released")
	}
	c.finishDataFile(b)
	if err := c.flusher.finish(); err != nil {
		t.Fatal(err)
	}
	if c.Tuning().FlushedBytes != 12288 {
		t.Fatalf("flushed accounting: %d", c.Tuning().FlushedBytes)
	}
}
func TestFinalFileFlushErrorFailsCopy(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	writeTestFile(t, src, "file", "data")
	c := newTestClient(dst)
	var calls atomic.Int32
	c.checkpointOverride = func() error { calls.Add(1); return errors.New("injected file flush failure") }
	if err := c.Run(newTestRPCClient(t, src)); err == nil {
		t.Fatal("flush failure reported success")
	}
	if calls.Load() < 1 {
		t.Fatal("file was never flushed")
	}
}

type slowCheckpointServer struct{ *Server }

func (s *slowCheckpointServer) GetChunk(args GetChunkArgs, data *[]byte) error {
	time.Sleep(2 * time.Millisecond)
	return s.Server.GetChunk(args, data)
}
func TestRegularCopyFlushesPeriodicallyWithoutAutotune(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	writeTestFile(t, src, "file", string(make([]byte, 65536)))
	s := NewServer()
	s.BasePath = src
	registry := rpc.NewServer()
	registerTestRPCServer(t, registry, &slowCheckpointServer{s})
	c := newTestClient(dst)
	c.BlockSize = 4096
	c.FlushInterval = 5 * time.Millisecond
	var calls atomic.Int32
	c.checkpointOverride = func() error { calls.Add(1); return nil }
	if err := c.Run(newTestRPCClientForServer(t, registry)); err != nil {
		t.Fatal(err)
	}
	if calls.Load() < 2 {
		t.Fatalf("missing periodic file flush: %d", calls.Load())
	}
	if c.Tuning().FlushedBytes != 65536 {
		t.Fatalf("regular writes not included: %d", c.Tuning().FlushedBytes)
	}
}
func TestFlushCanBeDisabledForRegularCopy(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	writeTestFile(t, src, "file", "payload")
	c := newTestClient(dst)
	c.FlushInterval = 0
	c.checkpointOverride = func() error { t.Error("disabled flush called"); return nil }
	if err := c.Run(newTestRPCClient(t, src)); err != nil {
		t.Fatal(err)
	}
	if c.Tuning().FlushCount != 0 {
		t.Fatal("unexpected flush")
	}
}
