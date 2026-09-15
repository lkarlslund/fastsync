package fastsync

import (
	"fmt"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPipelineStreamsBeyondBudgetAndPreservesHardlinks(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	// Each file is substantially larger than the entire cache budget.
	for i := 0; i < 8; i++ {
		writeTestFile(t, src, fmt.Sprint(i), strings.Repeat(fmt.Sprint(i), 1<<20))
	}
	if err := os.Link(filepath.Join(src, "0"), filepath.Join(src, "linked")); err != nil {
		t.Fatal(err)
	}
	c := runTestSync(t, src, dst, func(c *Client) {
		c.Pipeline = true
		c.PreserveHardlinks = true
		c.BlockSize = 4096
		c.BufferBytes = 32768
		c.CachedFiles = 32
		c.WriteParallel = 1
		c.ParallelFile = 16
	})
	if _, err := verifyTestArchive(t, src, dst); err != nil {
		t.Fatal(err)
	}
	if c.streamGate.peak > 2 || c.writerFiles.peak > 1 {
		t.Fatalf("exceeded limits: streams=%d writers=%d", c.streamGate.peak, c.writerFiles.peak)
	}
	if c.Tuning().BufferReserved != 0 {
		t.Fatal("leaked buffer reservation")
	}
	// Resume follows the existing metadata selection rules, with zero payload.
	c = runTestSync(t, src, dst, func(c *Client) { c.Pipeline = true; c.PreserveHardlinks = true })
	if c.Perf.Get(TransferredFileBytes) != 0 || c.Perf.Get(WrittenBytes) != 0 {
		t.Fatal("resume rewrote complete files")
	}
}
func TestPipelineReadFailureCleansStagesAndReleasesBudget(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	for i := 0; i < 8; i++ {
		writeTestFile(t, src, fmt.Sprint(i), "payload")
	}
	s := NewServer()
	s.BasePath = src
	registry := rpc.NewServer()
	registerTestRPCServer(t, registry, &brokenChunkServer{s})
	c := newTestClient(dst)
	c.Pipeline = true
	c.PreserveHardlinks = true
	c.BufferBytes = int64(c.BlockSize) * 4
	c.WriteParallel = 1
	done := make(chan error, 1)
	go func() { done <- c.Run(newTestRPCClientForServer(t, registry)) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("read failure accepted")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pipeline deadlocked")
	}
	entries, err := os.ReadDir(dst)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 || c.Tuning().BufferReserved != 0 {
		t.Fatal("failure leaked files or reservations")
	}
}
func TestPipelineDeltaReuse(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	writeTestFile(t, src, "disk", strings.Repeat("a", 65536)+"tail")
	writeTestFile(t, dst, "disk", strings.Repeat("a", 65536))
	c := runTestSync(t, src, dst, func(c *Client) { c.Pipeline = true; c.PreserveHardlinks = true; c.BlockSize = 4096 })
	if c.Perf.Get(TransferredFileBytes) != 4 {
		t.Fatalf("delta reused incorrectly: %d", c.Perf.Get(TransferredFileBytes))
	}
	if _, err := verifyTestArchive(t, src, dst); err != nil {
		t.Fatal(err)
	}
}
func TestAutotuneHandshakeAndSharedServerGate(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	writeTestFile(t, src, "file", "payload")
	s := NewServer()
	s.BasePath = src
	if err := s.ConfigureIO(8, true); err != nil {
		t.Fatal(err)
	}
	a, b := s.NewSession(), s.NewSession()
	if a.readGate != b.readGate || a.tuning != b.tuning {
		t.Fatal("limits must be shared across connections")
	}
	registry := rpc.NewServer()
	registerTestRPCServer(t, registry, a)
	c := newTestClient(dst)
	c.Pipeline = true
	c.PreserveHardlinks = true
	c.AutoTune = true
	if err := c.Run(newTestRPCClientForServer(t, registry)); err != nil {
		t.Fatal(err)
	}
	if c.Tuning().ReadLimit != 8 {
		t.Fatal("source limit not acknowledged")
	}
}
func TestTunerRejectsThroughputRegression(t *testing.T) {
	g := newIOGate(16, true)
	now := time.Now()
	feed := func(bytes uint64, latency time.Duration) {
		g.started = now.Add(-30 * time.Second)
		g.bytes = bytes
		g.calls = 100
		g.nanos = uint64(latency) * 100
		g.peak = g.limit
		g.step(now)
		now = now.Add(30 * time.Second)
	}
	feed(300<<20, time.Millisecond) // Probe 8 -> 16.
	if g.limit != 16 {
		t.Fatal(g.limit)
	}
	feed(300<<20, 2*time.Millisecond) // No gain; return to 8.
	if g.limit != 8 {
		t.Fatal("kept higher load without throughput gain")
	}
	feed(300<<20, time.Millisecond) // Probe 8 -> 4.
	if g.limit != 4 {
		t.Fatal(g.limit)
	}
	feed(295<<20, time.Millisecond) // Similar throughput: keep 4.
	if g.limit != 4 {
		t.Fatal("rejected equally fast lower concurrency")
	}
}
func TestPipelineBudgetValidation(t *testing.T) {
	c := NewClient()
	c.Pipeline = true
	c.PreserveHardlinks = true
	c.BufferBytes = int64(c.BlockSize)*4 - 1
	if err := c.initPipeline(); err == nil {
		t.Fatal("accepted unachievable budget")
	}
}

func TestAutotuneFailsClosedWithoutServerControls(t *testing.T) {
	c := newTestClient(t.TempDir())
	c.Pipeline = true
	c.AutoTune = true
	if err := c.Run(newTestRPCClient(t, t.TempDir())); err == nil {
		t.Fatal("silently ignored missing source controls")
	}
}
func TestTuningPhasesDoNotChangeBothSides(t *testing.T) {
	s := NewServer()
	if err := s.ConfigureIO(32, true); err != nil {
		t.Fatal(err)
	}
	authenticateTestSession(t, s)
	if err := s.Hello(SharedOptions{ProtocolVersion: PROTOCOLVERSION, BehaviorVersion: BEHAVIORVERSION}, nil); err != nil {
		t.Fatal(err)
	}
	for phase := 0; phase < 6; phase++ {
		var r TuneReply
		if err := s.TuneIO(struct{}{}, &r); err != nil {
			t.Fatal(err)
		}
		if r.ClientTurn != (phase == 4 || phase == 5) || r.ResetClient != (phase == 3) {
			t.Fatalf("phase %d: %+v", phase, r)
		}
	}
}
func TestPipelineWriteFailureUnblocksPrefetch(t *testing.T) {
	if _, err := os.Stat("/dev/full"); err != nil {
		t.Skip("requires /dev/full")
	}
	src := t.TempDir()
	writeTestFile(t, src, "file", strings.Repeat("x", 65536))
	rpcClient := newTestRPCClient(t, src)
	c := newTestClient(t.TempDir())
	c.Pipeline = true
	c.BlockSize = 4096
	if err := c.initPipeline(); err != nil {
		t.Fatal(err)
	}
	if err := c.hello(rpcClient); err != nil {
		t.Fatal(err)
	}
	var remote FileInfo
	if err := rpcClient.Call("Server.Stat", "/file", &remote); err != nil {
		t.Fatal(err)
	}
	if err := rpcClient.Call("Server.Open", "/file", nil); err != nil {
		t.Fatal(err)
	}
	defer rpcClient.Call("Server.Close", "/file", nil)
	full, err := os.OpenFile("/dev/full", os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer full.Close()
	done := make(chan error, 1)
	go func() { done <- c.streamRegular(rpcClient, remote, FileInfo{}, nil, full) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("write error ignored")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("prefetch stuck after write failure")
	}
}
