package fastsync

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIncludeCopyAndVerifySelectedTopLevelEntries(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	writeTestFile(t, src, "batch-a/data", "selected")
	writeTestFile(t, src, "batch-a.marker", "complete")
	writeTestFile(t, src, "batch-a.log", "stats")
	writeTestFile(t, src, "batch-b/data", "excluded")
	writeTestFile(t, dst, "batch-b/data", "keep local copy")
	client := runTestSync(t, src, dst, func(c *Client) {
		c.Include = []string{"batch-a", "batch-a.*", "batch-a"}
		c.PreserveHardlinks = true
	})
	if got := readTestFile(t, dst, "batch-a/data"); got != "selected" {
		t.Fatalf("selected data = %q", got)
	}
	if got := readTestFile(t, dst, "batch-a.marker"); got != "complete" {
		t.Fatalf("selected marker = %q", got)
	}
	if got := readTestFile(t, dst, "batch-b/data"); got != "keep local copy" {
		t.Fatalf("excluded destination data changed: %q", got)
	}
	if got := client.Perf.Get(FilesProcessed); got != 3 {
		t.Fatalf("processed %d selected files, want 3", got)
	}
	verify := newTestClient(dst)
	verify.Include = []string{"batch-a", "batch-a.*"}
	var report bytes.Buffer
	if err := verify.Verify(newTestRPCClient(t, src), &report); err != nil {
		t.Fatalf("verify selected entries: %v", err)
	}
	if strings.Contains(report.String(), "batch-b") {
		t.Fatal("verification inspected excluded generation")
	}
}

func TestIncludeRejectsUnsafeOrUnmatchedSelection(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	writeTestFile(t, src, "available/data", "source")
	for _, patterns := range [][]string{{""}, {"../escape"}, {"["}} {
		c := newTestClient(dst)
		c.Include = patterns
		if err := c.Run(newTestRPCClient(t, src)); err == nil {
			t.Fatalf("accepted invalid patterns %q", patterns)
		}
	}
	c := newTestClient(dst)
	c.Include = []string{"missing*"}
	if err := c.Run(newTestRPCClient(t, src)); err == nil || !strings.Contains(err.Error(), "no source entry matches") {
		t.Fatalf("unmatched selection error = %v", err)
	}
	partial := newTestClient(dst)
	partial.Include = []string{"available", "missing*"}
	if err := partial.Run(newTestRPCClient(t, src)); err == nil || !strings.Contains(err.Error(), "missing*") {
		t.Fatalf("partially unmatched selection error = %v", err)
	}
	verify := newTestClient(dst)
	verify.Include = []string{"missing*"}
	if err := verify.Verify(newTestRPCClient(t, src), &bytes.Buffer{}); err == nil {
		t.Fatal("verification accepted unmatched selection")
	}
	deleteClient := newTestClient(dst)
	deleteClient.Include = []string{"available"}
	deleteClient.Delete = true
	if err := deleteClient.Run(newTestRPCClient(t, src)); err == nil {
		t.Fatal("selected copy accepted deletion")
	}
	if _, err := os.Stat(filepath.Join(dst, "available")); !os.IsNotExist(err) {
		t.Fatalf("invalid selection changed destination: %v", err)
	}
}
