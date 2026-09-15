package fastsync

import (
	"bytes"
	"errors"
	"fmt"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type brokenChunkServer struct{ *Server }

func (s *brokenChunkServer) GetChunk(args GetChunkArgs, reply *[]byte) error {
	return errors.New("injected read failure")
}

func TestFailedCopyPreservesExistingHardlinkedHistory(t *testing.T) {
	source, dest := t.TempDir(), t.TempDir()
	writeTestFile(t, source, "a", "new content")
	if err := os.Link(filepath.Join(source, "a"), filepath.Join(source, "b")); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, dest, "a", "old content")
	if err := os.Link(filepath.Join(dest, "a"), filepath.Join(dest, "b")); err != nil {
		t.Fatal(err)
	}
	server := NewServer()
	server.BasePath = source
	registry := rpc.NewServer()
	registerTestRPCServer(t, registry, &brokenChunkServer{server})
	c := newTestClient(dest)
	c.PreserveHardlinks = true
	c.AlwaysChecksum = true
	if err := c.Run(newTestRPCClientForServer(t, registry)); err == nil {
		t.Fatal("failed copy returned success")
	}
	for _, name := range []string{"a", "b"} {
		if got := readTestFile(t, dest, name); got != "old content" {
			t.Fatalf("%s changed after failure: %q", name, got)
		}
	}
	entries, err := os.ReadDir(dest)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("staging files left after handled failure: %v", entries)
	}
	if c.Perf.Get(FilesProcessed) != 0 {
		t.Fatal("failed files counted as completed")
	}
	runTestSync(t, source, dest, func(c *Client) { c.PreserveHardlinks = true; c.AlwaysChecksum = true })
	for _, name := range []string{"a", "b"} {
		if got := readTestFile(t, dest, name); got != "new content" {
			t.Fatalf("rerun %s = %q", name, got)
		}
	}
}

type delayedOpenServer struct {
	*Server
	once sync.Once
}

func (s *delayedOpenServer) Open(path string, reply *interface{}) error {
	s.once.Do(func() { time.Sleep(1200 * time.Millisecond) })
	return s.Server.Open(path, reply)
}
func TestHardlinksWaitForCompleteCopy(t *testing.T) {
	source, dest := t.TempDir(), t.TempDir()
	writeTestFile(t, source, "a", "complete")
	if err := os.Link(filepath.Join(source, "a"), filepath.Join(source, "b")); err != nil {
		t.Fatal(err)
	}
	server := NewServer()
	server.BasePath = source
	registry := rpc.NewServer()
	registerTestRPCServer(t, registry, &delayedOpenServer{Server: server})
	c := newTestClient(dest)
	c.PreserveHardlinks = true
	if err := c.Run(newTestRPCClientForServer(t, registry)); err != nil {
		t.Fatal(err)
	}
	a, err := os.Stat(filepath.Join(dest, "a"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.Stat(filepath.Join(dest, "b"))
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(a, b) || readTestFile(t, dest, "b") != "complete" {
		t.Fatal("hardlink follower did not receive completed file")
	}
}

type mutatingChunkServer struct {
	*Server
	once sync.Once
}

func (s *mutatingChunkServer) GetChunk(args GetChunkArgs, reply *[]byte) error {
	err := s.Server.GetChunk(args, reply)
	s.once.Do(func() { _ = os.WriteFile(filepath.Join(s.BasePath, "a"), []byte("changed source"), 0644) })
	return err
}
func TestCopyRejectsChangingSource(t *testing.T) {
	source, dest := t.TempDir(), t.TempDir()
	writeTestFile(t, source, "a", "original content")
	server := NewServer()
	server.BasePath = source
	registry := rpc.NewServer()
	registerTestRPCServer(t, registry, &mutatingChunkServer{Server: server})
	if err := newTestClient(dest).Run(newTestRPCClientForServer(t, registry)); err == nil {
		t.Fatal("changing source accepted")
	}
	assertNotExists(t, filepath.Join(dest, "a"))
}

func TestMetadataFailuresAreReturned(t *testing.T) {
	info := FileInfo{Name: filepath.Join(t.TempDir(), "absent")}
	remote := FileInfo{Mode: 0600, Permissions: 0600}
	if err := info.ApplyChanges(remote); err == nil {
		t.Fatal("metadata failure swallowed")
	}
}

func verifyTestArchive(t *testing.T, source, dest string) (string, error) {
	t.Helper()
	c := newTestClient(dest)
	c.PreserveHardlinks = true
	c.Options.SendXattr = true
	var report bytes.Buffer
	err := c.Verify(newTestRPCClient(t, source), &report)
	return report.String(), err
}

func TestVerifyArchiveAndContentCorruption(t *testing.T) {
	source, dest := t.TempDir(), t.TempDir()
	writeTestFile(t, source, "old/a", "shared content")
	if err := os.MkdirAll(filepath.Join(source, "new"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(source, "old/a"), filepath.Join(source, "new/a")); err != nil {
		t.Fatal(err)
	}
	runTestSync(t, source, dest, func(c *Client) { c.PreserveHardlinks = true; c.Options.SendXattr = true })
	report, err := verifyTestArchive(t, source, dest)
	if err != nil {
		t.Fatalf("valid archive: %v\n%s", err, report)
	}
	if !strings.Contains(report, `"complete":true`) || !strings.Contains(report, `"hardlink_to":`) {
		t.Fatalf("incomplete report: %s", report)
	}
	path := filepath.Join(dest, "old/a")
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("broken content"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, before.ModTime(), before.ModTime()); err != nil {
		t.Fatal(err)
	}
	report, err = verifyTestArchive(t, source, dest)
	if err == nil || !strings.Contains(report, "SHA-256 content mismatch") {
		t.Fatalf("corruption missed: %v\n%s", err, report)
	}
	if got := readTestFile(t, dest, "old/a"); got != "broken content" {
		t.Fatal("verification repaired destination")
	}
}

func TestVerifyRejectsWrongHardlinkTopology(t *testing.T) {
	for _, split := range []bool{true, false} {
		t.Run(map[bool]string{true: "missing-link", false: "extra-link"}[split], func(t *testing.T) {
			source, dest := t.TempDir(), t.TempDir()
			writeTestFile(t, source, "a", "same")
			if split {
				if err := os.Link(filepath.Join(source, "a"), filepath.Join(source, "b")); err != nil {
					t.Fatal(err)
				}
			} else {
				writeTestFile(t, source, "b", "same")
			}
			runTestSync(t, source, dest, func(c *Client) { c.PreserveHardlinks = true })
			original, err := os.Stat(filepath.Join(dest, "b"))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(dest, "b")); err != nil {
				t.Fatal(err)
			}
			if split {
				writeTestFile(t, dest, "b", "same")
			} else {
				if err := os.Link(filepath.Join(dest, "a"), filepath.Join(dest, "b")); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Chtimes(filepath.Join(dest, "b"), original.ModTime(), original.ModTime()); err != nil {
				t.Fatal(err)
			}
			report, err := verifyTestArchive(t, source, dest)
			if err == nil || (!strings.Contains(report, "hardlink relationship") && !strings.Contains(report, "independent source inodes")) {
				t.Fatalf("bad topology accepted: %v\n%s", err, report)
			}
		})
	}
}

func TestVerifyDoesNotFollowDestinationDirectorySymlinks(t *testing.T) {
	source, dest, outside := t.TempDir(), t.TempDir(), t.TempDir()
	writeTestFile(t, source, "dir/file", "source")
	writeTestFile(t, outside, "file", "outside")
	if err := os.Symlink(outside, filepath.Join(dest, "dir")); err != nil {
		t.Fatal(err)
	}
	if _, err := verifyTestArchive(t, source, dest); err == nil {
		t.Fatal("symlink substituted for directory accepted")
	}
	if readTestFile(t, outside, "file") != "outside" {
		t.Fatal("outside content changed")
	}
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("report disk full") }
func TestVerifyReportFailure(t *testing.T) {
	source := t.TempDir()
	c := newTestClient(t.TempDir())
	if err := c.Verify(newTestRPCClient(t, source), failWriter{}); err == nil {
		t.Fatal("report failure ignored")
	}
}

type countedHashServer struct {
	*Server
	hashes int
}

func (s *countedHashServer) Hash(path string, reply *string) error {
	s.hashes++
	return s.Server.Hash(path, reply)
}
func TestManyVersionsTransferAndHashOnce(t *testing.T) {
	source, dest := t.TempDir(), t.TempDir()
	writeTestFile(t, source, "version-00/file", "shared backup")
	for i := 1; i < 40; i++ {
		dir := filepath.Join(source, fmt.Sprintf("version-%02d", i))
		if err := os.Mkdir(dir, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(filepath.Join(source, "version-00/file"), filepath.Join(dir, "file")); err != nil {
			t.Fatal(err)
		}
	}
	runTestSync(t, source, dest, func(c *Client) { c.PreserveHardlinks = true; c.ParallelFile = 8; c.ParallelDir = 8; c.QueueSize = 1 })
	server := NewServer()
	server.BasePath = source
	counted := &countedHashServer{Server: server}
	registry := rpc.NewServer()
	registerTestRPCServer(t, registry, counted)
	client := newTestClient(dest)
	client.PreserveHardlinks = true
	var report bytes.Buffer
	if err := client.Verify(newTestRPCClientForServer(t, registry), &report); err != nil {
		t.Fatalf("%v\n%s", err, report.String())
	}
	if counted.hashes != 1 {
		t.Fatalf("hashed shared content %d times, want once", counted.hashes)
	}
}
