package fastsync

import (
	"net/rpc"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

type linkChangingServer struct {
	*Server
	rootLists atomic.Int64
}

func (s *linkChangingServer) List(path string, reply *FileListResponse) error {
	if path == "/" && s.rootLists.Add(1) == 2 {
		if err := os.Link(filepath.Join(s.BasePath, "a/file"), filepath.Join(s.BasePath, "excluded/new-link")); err != nil {
			return err
		}
	}
	return s.Server.List(path, reply)
}

func TestCopyReusesSeedAfterUnselectedHardlinkCreation(t *testing.T) {
	src, dst := makeABC(t)
	if err := os.Mkdir(filepath.Join(src, "excluded"), 0755); err != nil {
		t.Fatal(err)
	}
	server := &linkChangingServer{Server: NewServer()}
	server.BasePath = src
	t.Cleanup(server.CloseFiles)
	registry := rpc.NewServer()
	registerTestRPCServer(t, registry, server)
	c := newTestClient(dst)
	c.PreserveHardlinks = true
	c.Include = []string{"a", "b", "c"}
	if err := c.Run(newTestRPCClientForServer(t, registry)); err != nil {
		t.Fatal(err)
	}
	if c.Perf.Get(TransferredFileBytes) != 0 || c.Perf.Get(WrittenBytes) != 0 {
		t.Fatal("copied data rather than reusing the existing inode")
	}
	a, err := os.Stat(filepath.Join(dst, "a/file"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.Stat(filepath.Join(dst, "b/file"))
	if err != nil || !os.SameFile(a, b) {
		t.Fatal("new path does not share the baseline inode")
	}
	if _, err := os.Stat(filepath.Join(dst, "excluded")); !os.IsNotExist(err) {
		t.Fatal("copied excluded directory")
	}
}

func TestReuseSeedRevalidatesLinkMetadataWithContent(t *testing.T) {
	for _, kind := range []string{"add-link", "remove-link", "content", "permissions", "replacement", "destination-content"} {
		t.Run(kind, func(t *testing.T) {
			src, dst := makeABC(t)
			c := newTestClient(dst)
			c.warming = true
			c.filequeue = make(chan FileInfo, 1)
			rpc := newTestRPCClient(t, src)
			if err := c.hello(rpc); err != nil {
				t.Fatal(err)
			}
			var root FileInfo
			if err := rpc.Call("Server.Stat", "/", &root); err != nil {
				t.Fatal(err)
			}
			c.warmExistingPass(rpc, root)
			if err := c.runError(); err != nil {
				t.Fatal(err)
			}
			var entry *inodeinfo
			for _, value := range c.inodes {
				entry = value
			}
			if entry == nil || entry.seed == nil {
				t.Fatal("missing reusable seed")
			}
			path := filepath.Join(src, entry.seed.source.Name)
			before, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			must := func(err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
			}
			switch kind {
			case "add-link":
				must(os.Link(path, filepath.Join(src, "additional-link")))
			case "remove-link":
				must(os.Remove(filepath.Join(src, "a/file")))
			case "content":
				must(os.WriteFile(path, []byte("changed bad"), 0644))
				must(os.Chtimes(path, before.ModTime(), before.ModTime()))
			case "permissions":
				must(os.Chmod(path, 0600))
			case "replacement":
				must(os.Remove(path))
				must(os.WriteFile(path, []byte("shared data"), 0644))
				must(os.Chtimes(path, before.ModTime(), before.ModTime()))
			case "destination-content":
				must(os.Link(path, filepath.Join(src, "additional-link")))
				must(os.WriteFile(entry.localhardlinkpath, []byte("changed bad"), 0644))
				must(os.Chtimes(entry.localhardlinkpath, before.ModTime(), before.ModTime()))
			}
			err = c.validateReuseSeed(rpc, entry)
			if kind == "add-link" || kind == "remove-link" {
				if err != nil {
					t.Fatalf("harmless hardlink change rejected: %v", err)
				}
			} else if err == nil {
				t.Fatal("accepted changed data, identity or permissions")
			}
		})
	}
}
