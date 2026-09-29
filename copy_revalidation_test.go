package fastsync

import (
	"bytes"
	"net/rpc"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
)

func TestRegularRevalidationMetadata(t *testing.T) {
	before := FileInfo{Mode: 0644, Dev: 1, Inode: 2, Size: 6, Nlink: 3,
		Mtim: syscall.Timespec{Sec: 1}, Ctim: syscall.Timespec{Sec: 2},
		Xattrs: map[string][]byte{"user.test": []byte("value")}}
	cases := []struct {
		name   string
		change func(*FileInfo)
		ok     bool
	}{
		{"unchanged", func(*FileInfo) {}, true},
		{"link-metadata", func(f *FileInfo) { f.Nlink--; f.Ctim.Sec++ }, true},
		{"inode", func(f *FileInfo) { f.Inode++ }, false},
		{"device", func(f *FileInfo) { f.Dev++ }, false},
		{"size", func(f *FileInfo) { f.Size++ }, false},
		{"mtime", func(f *FileInfo) { f.Mtim.Sec++ }, false},
		{"mode", func(f *FileInfo) { f.Mode = 0600 }, false},
		{"type", func(f *FileInfo) { f.Mode |= os.ModeSymlink }, false},
		{"owner", func(f *FileInfo) { f.Owner++ }, false},
		{"group", func(f *FileInfo) { f.Group++ }, false},
		{"target", func(f *FileInfo) { f.LinkTo = "target" }, false},
		{"rdev", func(f *FileInfo) { f.Rdev++ }, false},
		{"xattrs", func(f *FileInfo) { f.Xattrs = nil }, false},
		{"xattr-value", func(f *FileInfo) { f.Xattrs = map[string][]byte{"user.test": []byte("other")} }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			after := before
			tc.change(&after)
			if got := sameRegularMetadata(before, after, true); got != tc.ok {
				t.Fatalf("accepted=%v, want %v", got, tc.ok)
			}
		})
	}
	withoutAttrs := before
	withoutAttrs.Xattrs = nil
	if sameRegularMetadata(withoutAttrs, before, true) {
		t.Fatal("accepted adding xattrs to an empty set")
	}
	if !sameRegularMetadata(withoutAttrs, before, false) {
		t.Fatal("checked unrequested xattrs")
	}
}

type copyChangingServer struct {
	*Server
	rootLists atomic.Int64
	fileStats atomic.Int64
	hashes    atomic.Int64
	mode      string
	change    func() error
	again     func() error
}

func (s *copyChangingServer) List(path string, reply *FileListResponse) error {
	if path == "/" {
		s.rootLists.Add(1)
	}
	return s.Server.List(path, reply)
}

func (s *copyChangingServer) Stat(path string, reply *FileInfo) error {
	phase, count, target := int64(2), int64(1), "/a/file"
	if s.mode == "warm" {
		phase = 1
	} else if s.mode == "existing" {
		phase = 1
	} else if s.mode == "follower" {
		target = "/b/file"
	} else if s.mode == "detach" {
		phase = 1
		count = 2
	}
	if path == target && s.rootLists.Load() == phase && s.fileStats.Add(1) == count {
		if err := s.change(); err != nil {
			return err
		}
	}
	return s.Server.Stat(path, reply)
}

func (s *copyChangingServer) Hash(path string, reply *string) error {
	if err := s.Server.Hash(path, reply); err != nil {
		return err
	}
	if s.hashes.Add(1) == 1 && s.again != nil {
		return s.again()
	}
	return nil
}

func TestCopyRevalidatesQueuedFilesAfterHardlinkChanges(t *testing.T) {
	for _, mode := range []string{"warm", "existing", "download", "pipeline", "detach", "follower"} {
		for _, kind := range []string{"unchanged", "add-link", "remove-link", "content", "destination-content", "permissions", "replacement", "changing-again"} {
			t.Run(mode+"/"+kind, func(t *testing.T) {
				src, dst := t.TempDir(), t.TempDir()
				writeTestFile(t, src, "a/file", "shared data")
				fixedFileTime(t, filepath.Join(src, "a/file"))
				for _, dir := range []string{"b", "excluded"} {
					if err := os.Mkdir(filepath.Join(src, dir), 0755); err != nil {
						t.Fatal(err)
					}
					if err := os.Link(filepath.Join(src, "a/file"), filepath.Join(src, dir, "file")); err != nil {
						t.Fatal(err)
					}
				}
				configure := func(c *Client) {
					c.PreserveHardlinks = true
					c.Include = []string{"a", "b"}
				}
				if mode != "download" && mode != "pipeline" {
					runTestSync(t, src, dst, configure)
				}
				if mode == "follower" {
					if err := os.Remove(filepath.Join(dst, "b/file")); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "detach" {
					if err := os.Chmod(filepath.Join(dst, "a/file"), 0600); err != nil {
						t.Fatal(err)
					}
				}
				path := filepath.Join(src, "a/file")
				if mode == "follower" {
					path = filepath.Join(src, "b/file")
				}
				before, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				server := &copyChangingServer{Server: NewServer(), mode: mode}
				server.BasePath = src
				t.Cleanup(server.CloseFiles)
				server.change = func() error {
					switch kind {
					case "unchanged":
						return nil
					case "add-link", "changing-again":
						return os.Link(path, filepath.Join(src, "excluded/new-link"))
					case "remove-link":
						return os.Remove(filepath.Join(src, "excluded/file"))
					case "content":
						if err := os.WriteFile(path, []byte("changed bad"), 0644); err != nil {
							return err
						}
						return os.Chtimes(path, before.ModTime(), before.ModTime())
					case "destination-content":
						if err := os.Link(path, filepath.Join(src, "excluded/new-link")); err != nil {
							return err
						}
						local := filepath.Join(dst, "a/file")
						if mode == "download" || mode == "pipeline" || mode == "detach" {
							stages, err := filepath.Glob(filepath.Join(dst, "a/.fastsync-*"))
							if err != nil {
								return err
							}
							if len(stages) != 1 {
								return os.ErrNotExist
							}
							local = stages[0]
						}
						if err := os.WriteFile(local, []byte("changed bad"), 0644); err != nil {
							return err
						}
						return os.Chtimes(local, before.ModTime(), before.ModTime())
					case "permissions":
						return os.Chmod(path, 0600)
					case "replacement":
						if err := os.Remove(path); err != nil {
							return err
						}
						if err := os.WriteFile(path, []byte("shared data"), 0644); err != nil {
							return err
						}
						return os.Chtimes(path, before.ModTime(), before.ModTime())
					default:
						panic("unknown mutation")
					}
				}
				if kind == "changing-again" {
					server.again = func() error {
						return os.Link(path, filepath.Join(src, "excluded/another-link"))
					}
				}
				registry := rpc.NewServer()
				registerTestRPCServer(t, registry, server)
				client := newTestClient(dst)
				configure(client)
				client.ParallelFile, client.ParallelDir = 1, 1
				client.Pipeline = mode == "pipeline"
				if mode == "existing" || mode == "detach" {
					client.PreserveHardlinks = false
				}
				err = client.Run(newTestRPCClientForServer(t, registry))
				accepted := kind == "unchanged" || kind == "add-link" || kind == "remove-link"
				if (err == nil) != accepted {
					t.Fatalf("accepted=%v, error=%v", accepted, err)
				}
				if server.fileStats.Load() == 0 {
					t.Fatal("mutation hook was not reached")
				}
				if accepted {
					if kind == "unchanged" && server.hashes.Load() != 0 {
						t.Fatal("unchanged copy read full-file hashes")
					}
					if kind != "unchanged" && server.hashes.Load() == 0 {
						t.Fatal("accepted drift without hashing")
					}
					if client.PreserveHardlinks {
						verifier := newTestClient(dst)
						configure(verifier)
						var report bytes.Buffer
						if err := verifier.Verify(newTestRPCClient(t, src), &report); err != nil {
							t.Fatal(err)
						}
					}
				}
			})
		}
	}
}
