//go:build linux

package fastsync

import (
	"bytes"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestSpecialStateAllowsOnlyLinkMetadataChanges(t *testing.T) {
	for _, mode := range []os.FileMode{os.ModeSocket, os.ModeNamedPipe, os.ModeDevice, os.ModeDevice | os.ModeCharDevice} {
		before := FileInfo{Mode: mode | 0600, Dev: 1, Inode: 2, Size: 0, Nlink: 3,
			Mtim: syscall.Timespec{Sec: 1}, Ctim: syscall.Timespec{Sec: 2}, Rdev: 7,
			Xattrs: map[string][]byte{"user.test": []byte("value")}}
		cases := []struct {
			name   string
			change func(*FileInfo)
			ok     bool
		}{
			{"unchanged", func(*FileInfo) {}, true},
			{"remove-link", func(f *FileInfo) { f.Nlink--; f.Ctim.Sec++ }, true},
			{"inode", func(f *FileInfo) { f.Inode++ }, false},
			{"device", func(f *FileInfo) { f.Dev++ }, false},
			{"size", func(f *FileInfo) { f.Size++ }, false},
			{"mtime", func(f *FileInfo) { f.Mtim.Sec++ }, false},
			{"permissions", func(f *FileInfo) { f.Mode = mode | 0644 }, false},
			{"type", func(f *FileInfo) { f.Mode = os.ModeSymlink | 0600 }, false},
			{"owner", func(f *FileInfo) { f.Owner++ }, false},
			{"group", func(f *FileInfo) { f.Group++ }, false},
			{"rdev", func(f *FileInfo) { f.Rdev++ }, false},
			{"xattr", func(f *FileInfo) { f.Xattrs = map[string][]byte{"user.test": []byte("changed")} }, false},
		}
		for _, tc := range cases {
			t.Run(mode.String()+"/"+tc.name, func(t *testing.T) {
				after := before
				tc.change(&after)
				if got := sameStableNonRegularState(before, after); got != tc.ok {
					t.Fatalf("accepted=%v, want %v", got, tc.ok)
				}
			})
		}
	}
}

func makeSocketArchive(t *testing.T) (string, string) {
	t.Helper()
	src, dst := t.TempDir(), t.TempDir()
	for _, dir := range []string{"a", "b", "excluded"} {
		if err := os.Mkdir(filepath.Join(src, dir), 0755); err != nil {
			t.Fatal(err)
		}
	}
	link := filepath.Join(src, "a/link")
	if err := syscall.Mknod(link, syscall.S_IFSOCK|0600, 0); err != nil {
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

func TestCopyAndVerifySocketsAfterUnselectedHardlinkChanges(t *testing.T) {
	for _, stage := range []string{"copy", "verify"} {
		for _, kind := range []string{"add-link", "remove-link", "replacement", "permissions"} {
			t.Run(stage+"/"+kind, func(t *testing.T) {
				src, dst := makeSocketArchive(t)
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
					case "permissions":
						return os.Chmod(path, 0644)
					default:
						if err := os.Remove(path); err != nil {
							return err
						}
						return syscall.Mknod(path, syscall.S_IFSOCK|0600, 0)
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
					if err != nil || !os.SameFile(a, b) || a.Mode()&os.ModeSocket == 0 {
						t.Fatal("destination socket hardlinks split or changed type")
					}
				}
			})
		}
	}
}
