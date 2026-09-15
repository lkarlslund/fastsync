package fastsync

import (
	"bytes"
	"encoding/json"
	"github.com/rs/zerolog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func cachedSync(t *testing.T, src, dst, path, id string) *Client {
	t.Helper()
	return runTestSync(t, src, dst, func(c *Client) { c.PreserveHardlinks = true; c.ResumeCache = path; c.ResumeIdentity = id })
}
func TestPersistentCacheSkipsWarmTraversalAndReuses(t *testing.T) {
	src, dst := makeABC(t)
	cache := filepath.Join(t.TempDir(), "reuse.jsonl")
	first := cachedSync(t, src, dst, cache, "synthetic-snapshot")
	if first.Perf.Get(WrittenBytes) != 0 {
		t.Fatal("initial reuse wrote data")
	}
	if err := os.Remove(filepath.Join(dst, "a/file")); err != nil {
		t.Fatal(err)
	}
	second := cachedSync(t, src, dst, cache, "synthetic-snapshot")
	if second.Perf.Get(ExistingExamined) != 1 {
		t.Fatalf("expected one hint validation, got %d", second.Perf.Get(ExistingExamined))
	}
	if second.Perf.Get(WrittenBytes) != 0 || second.Perf.Get(FilesLinked) != 1 {
		t.Fatal("did not reuse persistent hint")
	}
}
func TestPersistentCacheRecordsNewCopyGroups(t *testing.T) {
	src, _ := makeABC(t)
	dst := t.TempDir()
	cache := filepath.Join(t.TempDir(), "reuse.jsonl")
	cachedSync(t, src, dst, cache, "synthetic-snapshot")
	second := cachedSync(t, src, dst, cache, "synthetic-snapshot")
	if second.Perf.Get(ExistingExamined) != 1 || second.Perf.Get(WrittenBytes) != 0 {
		t.Fatalf("newly copied group not restored: examined=%d written=%d", second.Perf.Get(ExistingExamined), second.Perf.Get(WrittenBytes))
	}
}
func TestPersistentCacheFallsBack(t *testing.T) {
	for _, mode := range []string{"identity", "truncated", "stale-path"} {
		t.Run(mode, func(t *testing.T) {
			src, dst := makeABC(t)
			cache := filepath.Join(t.TempDir(), "reuse.jsonl")
			cachedSync(t, src, dst, cache, "synthetic-snapshot")
			id := "synthetic-snapshot"
			switch mode {
			case "identity":
				id = "different-snapshot"
			case "truncated":
				f, e := os.OpenFile(cache, os.O_APPEND|os.O_WRONLY, 0)
				if e != nil {
					t.Fatal(e)
				}
				f.WriteString("{broken")
				f.Close()
			case "stale-path":
				if e := os.Remove(filepath.Join(dst, "b/file")); e != nil {
					t.Fatal(e)
				}
			}
			c := cachedSync(t, src, dst, cache, id)
			if c.Perf.Get(ExistingExamined) <= 1 {
				t.Fatal("did not scan on invalid cache")
			}
			if c.Perf.Get(WrittenBytes) != 0 {
				t.Fatal("fallback failed to reuse remaining links")
			}
		})
	}
}
func TestPersistentCacheRejectsArchivePathAndMissingIdentity(t *testing.T) {
	for _, inside := range []bool{true, false} {
		src, dst := makeABC(t)
		c := newTestClient(dst)
		c.PreserveHardlinks = true
		c.ResumeCache = filepath.Join(t.TempDir(), "cache")
		if inside {
			c.ResumeCache = filepath.Join(dst, "cache")
			c.ResumeIdentity = "synthetic"
		}
		if err := c.Run(newTestRPCClient(t, src)); err == nil {
			t.Fatal("accepted unsafe configuration")
		}
	}
}
func TestPersistentCacheClosePublishesPartialHints(t *testing.T) {
	src, dst := makeABC(t)
	c := newTestClient(dst)
	c.PreserveHardlinks = true
	c.ResumeCache = filepath.Join(t.TempDir(), "cache")
	c.ResumeIdentity = "synthetic"
	rpc := newTestRPCClient(t, src)
	c.remoteClient = rpc
	if err := c.Handshake(rpc); err != nil {
		t.Fatal(err)
	}
	var root, remote FileInfo
	if err := rpc.Call("Server.Stat", "/", &root); err != nil {
		t.Fatal(err)
	}
	cache, _, err := c.openResumeCache(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.Call("Server.Stat", "b/file", &remote); err != nil {
		t.Fatal(err)
	}
	if err := c.warmExistingFile(rpc, remote); err != nil {
		t.Fatal(err)
	}
	cache.Close()
	c.resume = nil
	data, err := os.ReadFile(c.ResumeCache)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "b/file") || strings.Contains(string(data), `"Complete":true`) {
		t.Fatal("partial cache was not saved correctly")
	}
}

func TestResumeCacheFallbackExplainsJournalFailure(t *testing.T) {
	for _, tc := range []struct{ name, tail, reason string }{
		{"missing completion", "", "completion_marker_missing"},
		{"malformed", "{broken\n", "malformed_record"},
		{"invalid hint", "{\"Hint\":{\"Path\":\"../escape\"}}\n", "invalid_hint_record"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			old := Logger
			Logger = zerolog.New(&logs)
			defer func() { Logger = old }()
			r := &resumeCache{path: filepath.Join(t.TempDir(), "cache.jsonl")}
			header, err := json.Marshal(resumeRecord{Identity: &r.identity})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(r.path, append(append(header, '\n'), []byte(tc.tail)...), 0600); err != nil {
				t.Fatal(err)
			}
			c := NewClient()
			c.ParallelFile = 1
			if c.loadResumeHints(r) {
				t.Fatal("invalid cache accepted")
			}
			for _, want := range []string{tc.reason, "failure_counts", "validated_hints", "completion_marker"} {
				if !strings.Contains(logs.String(), want) {
					t.Fatalf("missing %q in %s", want, logs.String())
				}
			}
		})
	}
}
