package fastsync

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func makeABC(t *testing.T) (string, string) {
	t.Helper()
	src, dst := t.TempDir(), t.TempDir()
	writeTestFile(t, src, "a/file", "shared data")
	fixedFileTime(t, filepath.Join(src, "a/file"))
	for _, d := range []string{"b", "c"} {
		if err := os.MkdirAll(filepath.Join(src, d), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(filepath.Join(src, "a/file"), filepath.Join(src, d, "file")); err != nil {
			t.Fatal(err)
		}
	}
	writeTestFile(t, dst, "b/file", "shared data")
	fixedFileTime(t, filepath.Join(dst, "b/file"))
	if err := os.MkdirAll(filepath.Join(dst, "c"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(dst, "b/file"), filepath.Join(dst, "c/file")); err != nil {
		t.Fatal(err)
	}
	return src, dst
}
func TestExistingPassReusesLaterSubtreesWithoutCopying(t *testing.T) {
	for _, checksum := range []bool{false, true} {
		t.Run(map[bool]string{false: "quick", true: "checksum"}[checksum], func(t *testing.T) {
			src, dst := makeABC(t)
			before, err := os.Stat(filepath.Join(dst, "b/file"))
			if err != nil {
				t.Fatal(err)
			}
			c, s := resumeSync(t, src, dst, checksum)
			if c.Perf.Get(WrittenBytes) != 0 || c.Perf.Get(TransferredFileBytes) != 0 || s.chunks.Load() != 0 {
				t.Fatal("copied data instead of reusing b/c")
			}
			if !checksum && (s.opens.Load() != 0 || s.checks.Load() != 0) {
				t.Fatal("quick-check read content")
			}
			if c.Perf.Get(FilesLinked) != 1 {
				t.Fatalf("linked %d, want only a", c.Perf.Get(FilesLinked))
			}
			for _, d := range []string{"a", "b", "c"} {
				after, err := os.Stat(filepath.Join(dst, d, "file"))
				if err != nil || !os.SameFile(before, after) {
					t.Fatalf("%s did not retain original inode", d)
				}
			}
			if _, err := verifyTestArchive(t, src, dst); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestWarmPassDoesNotCreateMissingPathsOrChangeMetadata(t *testing.T) {
	src, dst := makeABC(t)
	before, err := PathToFileInfo(filepath.Join(dst, "b/file"))
	if err != nil {
		t.Fatal(err)
	}
	c := newTestClient(dst)
	c.PreserveHardlinks = true
	c.warming = true
	c.filequeue = make(chan FileInfo, 1)
	r := newTestRPCClient(t, src)
	c.remoteClient = r
	if err := c.hello(r); err != nil {
		t.Fatal(err)
	}
	var root FileInfo
	if err := r.Call("Server.Stat", "/", &root); err != nil {
		t.Fatal(err)
	}
	c.warmExistingPass(r, root)
	if err := c.runError(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(dst, "a")); !os.IsNotExist(err) {
		t.Fatal("warm pass created missing directory")
	}
	after, err := PathToFileInfo(filepath.Join(dst, "b/file"))
	if err != nil {
		t.Fatal(err)
	}
	if before.Ctim != after.Ctim || before.Mtim != after.Mtim || before.Inode != after.Inode {
		t.Fatal("warm pass changed existing file")
	}
	if len(c.inodes) != 1 || c.Perf.Get(WrittenBytes) != 0 {
		t.Fatal("warm pass did not retain exactly one reusable group")
	}
}
func TestCachedDestinationReplacementFailsBeforeLinking(t *testing.T) {
	src, dst := makeABC(t)
	c := newTestClient(dst)
	r := newTestRPCClient(t, src)
	if err := c.hello(r); err != nil {
		t.Fatal(err)
	}
	var remote FileInfo
	if err := r.Call("Server.Stat", "/b/file", &remote); err != nil {
		t.Fatal(err)
	}
	if err := c.warmExistingFile(r, remote); err != nil {
		t.Fatal(err)
	}
	entry := c.inodes[inodeKey{remote.Dev, remote.Inode}]
	if entry == nil {
		t.Fatal("no cache entry")
	}
	if err := os.Remove(filepath.Join(dst, "b/file")); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, dst, "b/file", "wrong bytes")
	if err := c.validateReuseSeed(r, entry); err == nil {
		t.Fatal("accepted replaced cached path")
	}
}

// A bad earlier existing member must not prevent discovering a good later one.
func TestWarmPassSkipsMismatchedMember(t *testing.T) {
	src, dst := makeABC(t)
	writeTestFile(t, dst, "a/file", "bad")
	c := runTestSync(t, src, dst, func(c *Client) { c.PreserveHardlinks = true })
	if c.Perf.Get(WrittenBytes) != 0 {
		t.Fatal("copied despite good later member")
	}
	if _, err := verifyTestArchive(t, src, dst); err != nil {
		t.Fatal(err)
	}
}

func TestCompleteGenerationsNeedFullScopeForHardlinkRepair(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	writeTestFile(t, src, "a/file", "shared data")
	fixedFileTime(t, filepath.Join(src, "a/file"))
	for _, generation := range []string{"b", "c"} {
		if err := os.Mkdir(filepath.Join(src, generation), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(filepath.Join(src, "a/file"), filepath.Join(src, generation, "file")); err != nil {
			t.Fatal(err)
		}
	}
	for _, generation := range []string{"a", "b", "c"} {
		writeTestFile(t, dst, generation+"/file", "shared data")
		fixedFileTime(t, filepath.Join(dst, generation, "file"))
		for _, root := range []string{src, dst} {
			fixedFileTime(t, filepath.Join(root, generation))
		}
	}
	canonical, err := os.Stat(filepath.Join(dst, "a/file"))
	if err != nil {
		t.Fatal(err)
	}
	runTestSync(t, src, dst, func(c *Client) {
		c.PreserveHardlinks = true
		c.Include = []string{"a", "c"}
	})
	for _, generation := range []string{"a", "b", "c"} {
		source, err := os.Stat(filepath.Join(src, generation))
		if err != nil {
			t.Fatal(err)
		}
		dest, err := os.Stat(filepath.Join(dst, generation))
		if err != nil || !dest.ModTime().Equal(source.ModTime()) {
			t.Fatalf("generation %s does not have its completion timestamp: %v", generation, err)
		}
	}
	report, err := verifyTestArchive(t, src, dst)
	if err == nil || !strings.Contains(report, "source hardlink relationship missing at destination") {
		t.Fatalf("certified incomplete cross-generation links: error=%v, report=%s", err, report)
	}
	client, server := resumeSync(t, src, dst, false)
	if client.Perf.Get(ReadBytes) != 0 || client.Perf.Get(WrittenBytes) != 0 ||
		client.Perf.Get(TransferredFileBytes) != 0 || server.opens.Load() != 0 ||
		server.checks.Load() != 0 || server.chunks.Load() != 0 {
		t.Fatal("hardlink repair read, transferred or rewrote file data")
	}
	if client.Perf.Get(FilesLinked) != 1 {
		t.Fatalf("linked %d files, want only the excluded generation", client.Perf.Get(FilesLinked))
	}
	for _, generation := range []string{"a", "b", "c"} {
		current, err := os.Stat(filepath.Join(dst, generation, "file"))
		if err != nil || !os.SameFile(canonical, current) {
			t.Fatalf("generation %s did not reuse the canonical inode: %v", generation, err)
		}
	}
	if _, err := verifyTestArchive(t, src, dst); err != nil {
		t.Fatal(err)
	}
}
