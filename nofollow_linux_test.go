//go:build linux

package fastsync

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"golang.org/x/sys/unix"
)

func TestNoFollowArchiveSymlinkParents(t *testing.T) {
	root, out := t.TempDir(), t.TempDir()
	writeTestFile(t, out, "file", "outside")
	if err := os.Symlink(out, filepath.Join(root, "alias")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "alias", "file")
	checks := map[string]func() error{
		"open": func() error {
			f, e := openNoFollow(path)
			if f != nil {
				f.Close()
			}
			return e
		},
		"stat":  func() error { _, e := pathToFileInfo(path, true); return e },
		"hash":  func() error { _, e := hashFile(path); return e },
		"list":  func() error { _, e := readDirNoFollow(filepath.Join(root, "alias")); return e },
		"mkdir": func() error { return mkdirAllNoFollow(filepath.Join(root, "alias", "child"), 0700) },
		"create": func() error {
			f, e := createTempNoFollow(filepath.Join(root, "alias"), "temp-")
			if f != nil {
				f.Close()
			}
			return e
		},
		"remove":        func() error { return removeNoFollow(path) },
		"remove-all":    func() error { return removeAllNoFollow(path) },
		"chmod":         func() error { return chmodNoFollow(path, 0600) },
		"chown":         func() error { return chownNoFollow(path, os.Getuid(), os.Getgid()) },
		"times":         func() error { return timesNoFollow(path, []unix.Timespec{{Sec: 1}, {Sec: 1}}) },
		"xattr":         func() error { return setXattrNoFollow(path, "user.bad", []byte("bad")) },
		"link-dest":     func() error { return linkNoFollow(filepath.Join(out, "file"), filepath.Join(root, "alias", "new")) },
		"link-source":   func() error { return linkNoFollow(path, filepath.Join(root, "new")) },
		"rename-dest":   func() error { return renameNoFollow(filepath.Join(out, "file"), filepath.Join(root, "alias", "new")) },
		"rename-source": func() error { return renameNoFollow(path, filepath.Join(root, "new")) },
	}
	for n, fn := range checks {
		t.Run(n, func(t *testing.T) {
			if err := fn(); err == nil {
				t.Fatal("followed archive symlink parent")
			}
		})
	}
	if got := readTestFile(t, out, "file"); got != "outside" {
		t.Fatal("outside content changed")
	}
	entries, err := os.ReadDir(out)
	if err != nil || len(entries) != 1 {
		t.Fatal("outside directory changed")
	}
	// Exercise the old-kernel component-by-component fallback as well.
	if fd, err := walkDirectoryNoFollow(filepath.Join(root, "alias")); err == nil {
		unix.Close(fd)
		t.Fatal("fallback followed symlink")
	}
}

func TestNoFollowLeafStillCopiesSymlinkAsEntry(t *testing.T) {
	src, dst, out := t.TempDir(), t.TempDir(), t.TempDir()
	writeTestFile(t, out, "secret", "secret")
	link := filepath.Join(src, "link")
	if err := os.Symlink(filepath.Join(out, "secret"), link); err != nil {
		t.Fatal(err)
	}
	if f, err := openNoFollow(link); err == nil {
		f.Close()
		t.Fatal("opened symlink content")
	}
	if _, err := hashFile(link); err == nil {
		t.Fatal("hashed symlink target")
	}
	fi, err := pathToFileInfo(link, true)
	if err != nil || fi.LinkTo != filepath.Join(out, "secret") {
		t.Fatalf("symlink metadata: %v", err)
	}
	runTestSync(t, src, dst, nil)
	target, err := os.Readlink(filepath.Join(dst, "link"))
	if err != nil || target != fi.LinkTo {
		t.Fatal("literal symlink not preserved")
	}
	if err := linkNoFollow(link, filepath.Join(src, "second")); err != nil {
		t.Fatal(err)
	}
	a, _ := os.Lstat(link)
	b, _ := os.Lstat(filepath.Join(src, "second"))
	if !os.SameFile(a, b) {
		t.Fatal("hardlink followed final symlink")
	}
	if err := removeAllNoFollow(filepath.Join(dst, "link")); err != nil {
		t.Fatal(err)
	}
	if readTestFile(t, out, "secret") != "secret" {
		t.Fatal("removed symlink target")
	}
}

func TestServerRejectsSymlinkContentAndDirectoryRPCs(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "real/file", "data")
	if err := os.Symlink("real", filepath.Join(root, "alias")); err != nil {
		t.Fatal(err)
	}
	s := NewServer()
	s.BasePath = root
	if err := s.NewSession().SelectRoot("alias", nil); err == nil {
		t.Fatal("selected symlink root inside share")
	}
	if err := s.Hello(SharedOptions{ProtocolVersion: PROTOCOLVERSION}, nil); err != nil {
		t.Fatal(err)
	}
	var list FileListResponse
	if err := s.List("alias", &list); err == nil {
		t.Fatal("listed symlink directory")
	}
	var fi FileInfo
	if err := s.Stat("alias/file", &fi); err == nil {
		t.Fatal("stat through symlink parent")
	}
	if err := s.Open("alias/file", nil); err == nil {
		t.Fatal("read through symlink parent")
	}
	if err := s.Stat("alias", &fi); err != nil || fi.Mode&os.ModeSymlink == 0 {
		t.Fatal("cannot inspect symlink entry")
	}
}

func TestPinnedParentCannotBeRedirected(t *testing.T) {
	root, out := t.TempDir(), t.TempDir()
	writeTestFile(t, root, "parent/file", "inside")
	writeTestFile(t, out, "file", "outside")
	path := filepath.Join(root, "parent", "file")
	err := withPinnedLeaf(path, func(pinned string) error {
		if e := os.Rename(filepath.Join(root, "parent"), filepath.Join(root, "moved")); e != nil {
			return e
		}
		if e := os.Symlink(out, filepath.Join(root, "parent")); e != nil {
			return e
		}
		// The trusted procfd bridge still names the original directory.
		b, e := os.ReadFile(pinned)
		if e != nil {
			return e
		}
		if string(b) != "inside" {
			t.Fatal("parent replacement redirected pinned access")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pathToFileInfo(path, false); err == nil {
		t.Fatal("new operation accepted replaced parent")
	}
}

func TestConcurrentParentSymlinkSwapsCannotEscape(t *testing.T) {
	root, out := t.TempDir(), t.TempDir()
	writeTestFile(t, root, "parent/file", "inside")
	writeTestFile(t, out, "file", "outside")
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if os.Rename(filepath.Join(root, "parent"), filepath.Join(root, "held")) == nil {
				os.Symlink(out, filepath.Join(root, "parent"))
				os.Remove(filepath.Join(root, "parent"))
				os.Rename(filepath.Join(root, "held"), filepath.Join(root, "parent"))
			}
		}
	}()
	defer func() { close(stop); wg.Wait() }()
	for i := 0; i < 500; i++ {
		f, err := openNoFollow(filepath.Join(root, "parent", "file"))
		if err != nil {
			continue
		}
		b := make([]byte, 16)
		n, e := f.Read(b)
		f.Close()
		if e == nil && string(b[:n]) != "inside" {
			t.Fatalf("escaped to %q", b[:n])
		}
	}
}
