package fastsync

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPositionSavedWhenDisabledAndUsedOnlyWhenEnabled(t *testing.T) {
	src, dst := makeABC(t)
	cache := filepath.Join(t.TempDir(), "cache")
	// First run leaves position use off, but must save durable subtree markers.
	cachedSync(t, src, dst, cache, "synthetic-snapshot")
	again := cachedSync(t, src, dst, cache, "synthetic-snapshot")
	if again.Perf.Get(ResumeSubtreesSkipped) != 0 || again.Perf.Get(FilesProcessed) != 3 {
		t.Fatal("position used without opting in")
	}
	resumed := runTestSync(t, src, dst, func(c *Client) {
		c.PreserveHardlinks = true
		c.ResumeCache = cache
		c.ResumeIdentity = "synthetic-snapshot"
		c.ResumePosition = true
	})
	if resumed.Perf.Get(ResumeSubtreesSkipped) != 3 || resumed.Perf.Get(FilesProcessed) != 0 {
		t.Fatalf("position did not skip completed trees: %d/%d", resumed.Perf.Get(ResumeSubtreesSkipped), resumed.Perf.Get(FilesProcessed))
	}
}
func TestPositionMissingDestinationSubtreeIsRevisited(t *testing.T) {
	src, dst := makeABC(t)
	cache := filepath.Join(t.TempDir(), "cache")
	cachedSync(t, src, dst, cache, "synthetic-snapshot")
	if err := os.RemoveAll(filepath.Join(dst, "c")); err != nil {
		t.Fatal(err)
	}
	resumed := runTestSync(t, src, dst, func(c *Client) {
		c.PreserveHardlinks = true
		c.ResumeCache = cache
		c.ResumeIdentity = "synthetic-snapshot"
		c.ResumePosition = true
	})
	if resumed.Perf.Get(ResumeSubtreesSkipped) != 2 || resumed.Perf.Get(FilesLinked) != 1 {
		t.Fatalf("missing subtree not repaired: skipped=%d linked=%d", resumed.Perf.Get(ResumeSubtreesSkipped), resumed.Perf.Get(FilesLinked))
	}
}
func TestPositionRequiresCache(t *testing.T) {
	c := newTestClient(t.TempDir())
	c.ResumePosition = true
	if err := c.Run(newTestRPCClient(t, t.TempDir())); err == nil {
		t.Fatal("accepted position without cache")
	}
}

func TestPositionFlushFailureDoesNotMarkSubtree(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	writeTestFile(t, src, "tree/file", "data")
	c := newTestClient(dst)
	c.PreserveHardlinks = true
	c.ResumeCache = filepath.Join(t.TempDir(), "cache")
	c.ResumeIdentity = "synthetic-snapshot"
	c.checkpointOverride = func() error { return fmt.Errorf("injected flush failure") }
	if err := c.Run(newTestRPCClient(t, src)); err == nil {
		t.Fatal("flush error lost")
	}
	data, err := os.ReadFile(c.ResumeCache)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), `"Subtree":{`) {
		t.Fatal("checkpointed unflushed subtree")
	}
}
