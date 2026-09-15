package fastsync

import (
	"fmt"
	"net/rpc"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pkg/xattr"
)

func TestSyncShortFileSameMtime(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	writeTestFile(t, src, "file", "complete content")
	writeTestFile(t, dst, "file", "short")
	stamp := time.Unix(1700000000, 0)
	for _, root := range []string{src, dst} {
		if err := os.Chtimes(filepath.Join(root, "file"), stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	runTestSync(t, src, dst, nil)
	if got := readTestFile(t, dst, "file"); got != "complete content" {
		t.Fatalf("silent incomplete copy: %q", got)
	}
}
func TestSyncChangedSymlink(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	if err := os.Symlink("new", filepath.Join(src, "latest")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("old", filepath.Join(dst, "latest")); err != nil {
		t.Fatal(err)
	}
	runTestSync(t, src, dst, nil)
	got, err := os.Readlink(filepath.Join(dst, "latest"))
	if err != nil {
		t.Fatal(err)
	}
	if got != "new" {
		t.Fatalf("stale link target: %q", got)
	}
}
func TestSyncHardlinkSplit(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	writeTestFile(t, src, "a", "old content")
	writeTestFile(t, src, "b", "new content")
	writeTestFile(t, dst, "a", "old content")
	if err := os.Link(filepath.Join(dst, "a"), filepath.Join(dst, "b")); err != nil {
		t.Fatal(err)
	}
	runTestSync(t, src, dst, func(c *Client) { c.PreserveHardlinks = true; c.AlwaysChecksum = true; c.ParallelFile = 1 })
	if got := readTestFile(t, dst, "a"); got != "old content" {
		t.Errorf("old version overwritten: %q", got)
	}
	if got := readTestFile(t, dst, "b"); got != "new content" {
		t.Errorf("new version incorrect: %q", got)
	}
}
func TestSyncSpecialPermissions(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	writeTestFile(t, src, "file", "data")
	if err := os.Chmod(filepath.Join(src, "file"), 0755|os.ModeSetuid); err != nil {
		t.Fatal(err)
	}
	runTestSync(t, src, dst, nil)
	fi, err := os.Stat(filepath.Join(dst, "file"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode()&os.ModeSetuid == 0 {
		t.Fatalf("setuid lost: %v", fi.Mode())
	}
}
func TestSyncXattrOnlyChange(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	stamp := time.Unix(1700000000, 0)
	for _, root := range []string{src, dst} {
		writeTestFile(t, root, "file", "data")
		if err := os.Chtimes(filepath.Join(root, "file"), stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	if err := xattr.Set(filepath.Join(src, "file"), "user.review", []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := xattr.Set(filepath.Join(dst, "file"), "user.review", []byte("old")); err != nil {
		t.Fatal(err)
	}
	runTestSync(t, src, dst, func(c *Client) { c.Options.SendXattr = true; c.AlwaysChecksum = true })
	got, err := xattr.Get(filepath.Join(dst, "file"), "user.review")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "new" {
		t.Fatalf("stale xattr: %q", got)
	}
}

func TestSyncListFailureMustFailRun(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	writeTestFile(t, src, "bad/important", "backup contents")
	rs := rpc.NewServer()
	s := NewServer()
	s.BasePath = src
	registerTestRPCServer(t, rs, &failingListServer{Server: s, failPath: "/bad"})
	c := newTestClient(dst)
	if err := c.Run(newTestRPCClientForServer(t, rs)); err == nil {
		t.Fatal("Run returned success despite omitted backup directory")
	}
}
func TestSyncCrossHistoryHardlinks(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	writeTestFile(t, src, "20250101/etc/file", "shared content")
	if err := os.MkdirAll(filepath.Join(src, "20250201/etc"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(src, "20250101/etc/file"), filepath.Join(src, "20250201/etc/file")); err != nil {
		t.Fatal(err)
	}
	runTestSync(t, src, dst, func(c *Client) { c.PreserveHardlinks = true })
	a, err := os.Stat(filepath.Join(dst, "20250101/etc/file"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.Stat(filepath.Join(dst, "20250201/etc/file"))
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(a, b) {
		t.Fatal("history hardlinks not preserved")
	}
	for _, p := range []string{"20250101/etc/file", "20250201/etc/file"} {
		if got := readTestFile(t, dst, p); got != "shared content" {
			t.Fatalf("bad content: %q", got)
		}
	}
}

func TestSyncDirectoryListingsDoNotShareXattrs(t *testing.T) {
	source, dest := t.TempDir(), t.TempDir()
	for i := 0; i < 40; i++ {
		path := fmt.Sprintf("dir-%02d/file", i)
		writeTestFile(t, source, path, "content")
		if i%2 == 0 {
			if err := xattr.Set(filepath.Join(source, path), "user.index", []byte(fmt.Sprint(i))); err != nil {
				t.Fatal(err)
			}
		}
	}
	runTestSync(t, source, dest, func(c *Client) { c.Options.SendXattr = true; c.ParallelDir = 1; c.ParallelFile = 1; c.QueueSize = 128 })
	for i := 0; i < 40; i++ {
		path := fmt.Sprintf("dir-%02d/file", i)
		sourceInfo, err := PathToFileInfo(filepath.Join(source, path))
		if err != nil {
			t.Fatal(err)
		}
		destInfo, err := PathToFileInfo(filepath.Join(dest, path))
		if err != nil {
			t.Fatal(err)
		}
		if err := compareMetadata(destInfo, sourceInfo, true); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
	}
}
