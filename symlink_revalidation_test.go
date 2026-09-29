package fastsync

import (
	"bytes"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
)

func TestSymlinkStateAllowsOnlyLinkMetadataChanges(t *testing.T) {
	before := FileInfo{Mode: os.ModeSymlink | 0777, Dev: 1, Inode: 2, Size: 6,
		LinkTo: "target", Nlink: 3, Mtim: syscall.Timespec{Sec: 1}, Ctim: syscall.Timespec{Sec: 2},
		Xattrs: map[string][]byte{"user.test": []byte("value")}}
	cases := []struct {
		name   string
		change func(*FileInfo)
		ok     bool
	}{
		{"unchanged", func(*FileInfo) {}, true},
		{"add-link", func(f *FileInfo) { f.Nlink++; f.Ctim.Sec++ }, true},
		{"remove-link", func(f *FileInfo) { f.Nlink--; f.Ctim.Sec++ }, true},
		{"ctime", func(f *FileInfo) { f.Ctim.Sec++ }, true},
		{"inode", func(f *FileInfo) { f.Inode++ }, false},
		{"device", func(f *FileInfo) { f.Dev++ }, false},
		{"target", func(f *FileInfo) { f.LinkTo = "other!" }, false},
		{"type", func(f *FileInfo) { f.Mode = 0777 }, false},
		{"permissions", func(f *FileInfo) { f.Mode = os.ModeSymlink | 0700 }, false},
		{"owner", func(f *FileInfo) { f.Owner++ }, false},
		{"group", func(f *FileInfo) { f.Group++ }, false},
		{"mtime", func(f *FileInfo) { f.Mtim.Sec++ }, false},
		{"size", func(f *FileInfo) { f.Size++ }, false},
		{"rdev", func(f *FileInfo) { f.Rdev++ }, false},
		{"xattr", func(f *FileInfo) { f.Xattrs = map[string][]byte{"user.test": []byte("changed")} }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			after := before
			tc.change(&after)
			if got := sameSymlinkState(before, after); got != tc.ok {
				t.Fatalf("accepted=%v, want %v", got, tc.ok)
			}
		})
	}
	regular := before
	regular.Mode = 0644
	if sameSymlinkState(regular, regular) {
		t.Fatal("relaxed regular-file content checks")
	}
}

func makeSymlinkArchive(t *testing.T) (string, string) {
	t.Helper()
	src, dst := t.TempDir(), t.TempDir()
	for _, dir := range []string{"a", "b", "excluded"} {
		if err := os.Mkdir(filepath.Join(src, dir), 0755); err != nil {
			t.Fatal(err)
		}
	}
	link := filepath.Join(src, "a/link")
	if err := os.Symlink("../target", link); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"b/link", "excluded/old-link"} {
		if err := os.Link(link, filepath.Join(src, name)); err != nil {
			t.Fatal(err)
		}
	}
	runTestSync(t, src, dst, func(c *Client) {
		c.PreserveHardlinks = true
		c.Include = []string{"a", "b"}
	})
	return src, dst
}

type symlinkChangingServer struct {
	*Server
	rootLists atomic.Int64
	linkStats atomic.Int64
	stage     string
	change    func() error
}

func (s *symlinkChangingServer) List(path string, reply *FileListResponse) error {
	if path == "/" && s.rootLists.Add(1) == 2 && s.stage == "copy" {
		if err := s.change(); err != nil {
			return err
		}
	}
	return s.Server.List(path, reply)
}

func (s *symlinkChangingServer) Stat(path string, reply *FileInfo) error {
	if path == "/a/link" && s.linkStats.Add(1) == 1 && s.stage == "verify" {
		if err := s.change(); err != nil {
			return err
		}
	}
	return s.Server.Stat(path, reply)
}

func TestCopyAndVerifySymlinksAfterUnselectedHardlinkChanges(t *testing.T) {
	for _, stage := range []string{"copy", "verify"} {
		for _, kind := range []string{"add-link", "remove-link", "replacement", "target"} {
			t.Run(stage+"/"+kind, func(t *testing.T) {
				src, dst := makeSymlinkArchive(t)
				if stage == "copy" {
					if err := os.Remove(filepath.Join(dst, "b/link")); err != nil {
						t.Fatal(err)
					}
				}
				server := &symlinkChangingServer{Server: NewServer(), stage: stage}
				server.BasePath = src
				t.Cleanup(server.CloseFiles)
				server.change = func() error {
					path := filepath.Join(src, "a/link")
					switch kind {
					case "add-link":
						return os.Link(path, filepath.Join(src, "excluded/new-link"))
					case "remove-link":
						return os.Remove(filepath.Join(src, "excluded/old-link"))
					default:
						if err := os.Remove(path); err != nil {
							return err
						}
						target := "../target"
						if kind == "target" {
							target = "../other!"
						}
						return os.Symlink(target, path)
					}
				}
				registry := rpc.NewServer()
				registerTestRPCServer(t, registry, server)
				client := newTestClient(dst)
				client.PreserveHardlinks = true
				client.Include = []string{"a", "b"}
				var report bytes.Buffer
				var err error
				if stage == "copy" {
					err = client.Run(newTestRPCClientForServer(t, registry))
				} else {
					err = client.Verify(newTestRPCClientForServer(t, registry), &report)
				}
				accepted := kind == "add-link" || kind == "remove-link"
				if (err == nil) != accepted {
					t.Fatalf("accepted=%v, error=%v\n%s", accepted, err, report.String())
				}
				if stage == "verify" && strings.Contains(report.String(), `"complete":true`) != accepted {
					t.Fatalf("incorrect verification completion: %s", report.String())
				}
				if accepted {
					a, err := os.Lstat(filepath.Join(dst, "a/link"))
					if err != nil {
						t.Fatal(err)
					}
					b, err := os.Lstat(filepath.Join(dst, "b/link"))
					if err != nil || !os.SameFile(a, b) {
						t.Fatal("destination symlink hardlinks split")
					}
					if target, err := os.Readlink(filepath.Join(dst, "b/link")); err != nil || target != "../target" {
						t.Fatalf("target=%q error=%v", target, err)
					}
				}
			})
		}
	}
}
