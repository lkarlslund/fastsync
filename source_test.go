package fastsync

import (
	"bytes"
	"errors"
	"net/rpc"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

type siblingChangingServer struct {
	*Server
	rootLists atomic.Int64
}

func (s *siblingChangingServer) List(path string, reply *FileListResponse) error {
	if path == "/" && s.rootLists.Add(1) == 2 {
		if err := os.Mkdir(filepath.Join(s.BasePath, "excluded-new"), 0755); err != nil {
			return err
		}
	}
	return s.Server.List(path, reply)
}

func TestIncludedCopyAndVerifyIgnoreExcludedSiblingTimestamps(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	writeTestFile(t, src, "selected/data", "stable archive")
	server := &siblingChangingServer{Server: NewServer()}
	server.BasePath = src
	t.Cleanup(server.CloseFiles)
	registry := rpc.NewServer()
	registerTestRPCServer(t, registry, server)
	c := newTestClient(dst)
	c.PreserveHardlinks = true
	c.Include = []string{"selected"}
	if err := c.Run(newTestRPCClientForServer(t, registry)); err != nil {
		t.Fatalf("excluded sibling creation failed copy: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "excluded-new")); !os.IsNotExist(err) {
		t.Fatal("copied excluded sibling")
	}
	if err := os.Mkdir(filepath.Join(src, "excluded-later"), 0755); err != nil {
		t.Fatal(err)
	}
	verify := func() error {
		client := newTestClient(dst)
		client.Include = c.Include
		var report bytes.Buffer
		return client.Verify(newTestRPCClient(t, src), &report)
	}
	if err := verify(); err != nil {
		t.Fatalf("excluded sibling creation failed verification: %v", err)
	}
	original, err := os.Stat(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(src, original.Mode().Perm()^0100); err != nil {
		t.Fatal(err)
	}
	if err := verify(); err == nil {
		t.Fatal("ignored source-root permission mismatch")
	}
	if err := os.Chmod(src, original.Mode().Perm()); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, src, "selected/new-data", "must be verified")
	if err := verify(); err == nil {
		t.Fatal("ignored mutation inside selected directory")
	}
}

func TestSelectedSourceCopyAndVerify(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	writeTestFile(t, src, "server-a/old/data", "archive")
	writeTestFile(t, src, "server-a/new/data", "not selected")
	writeTestFile(t, src, "other/data", "other server")
	runTestSync(t, src, dst, func(c *Client) {
		c.SourcePath = "server-a"
		c.Include = []string{"old"}
	})
	if got := readTestFile(t, dst, "old/data"); got != "archive" {
		t.Fatal(got)
	}
	if _, err := os.Stat(filepath.Join(dst, "other")); !os.IsNotExist(err) {
		t.Fatal("copied sibling")
	}
	if _, err := os.Stat(filepath.Join(dst, "new")); !os.IsNotExist(err) {
		t.Fatal("copied excluded child")
	}
	c := newTestClient(dst)
	c.SourcePath = "server-a"
	c.Include = []string{"old"}
	var report bytes.Buffer
	if err := c.Verify(newTestRPCClient(t, src), &report); err != nil {
		t.Fatal(err)
	}
}
func TestSelectRootConfinementAndSessionIsolation(t *testing.T) {
	base := t.TempDir()
	if err := os.Mkdir(filepath.Join(base, "server-a"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(base, "escape")); err != nil {
		t.Fatal(err)
	}
	s := NewServer()
	s.BasePath = base
	if err := s.PinRoot(); err != nil {
		t.Fatal(err)
	}
	defer s.CloseFiles()
	for _, path := range []string{"../", base, "escape", "missing"} {
		session := s.NewSession()
		authenticateTestSession(t, session)
		if err := session.SelectRoot(path, nil); err == nil {
			t.Fatalf("accepted %q", path)
		}
	}
	session := s.NewSession()
	authenticateTestSession(t, session)
	if err := session.SelectRoot("server-a", nil); err != nil {
		t.Fatal(err)
	}
	selectedRoot := session.root
	if selectedRoot == nil {
		t.Fatal("selected root was not pinned")
	}
	defer session.CloseFiles()
	if s.BasePath != base || s.NewSession().BasePath != base {
		t.Fatal("changed listener root")
	}
	if err := session.Hello(SharedOptions{ProtocolVersion: PROTOCOLVERSION, BehaviorVersion: BEHAVIORVERSION}, nil); err != nil {
		t.Fatal(err)
	}
	if err := session.SelectRoot(".", nil); err == nil {
		t.Fatal("changed root after Hello")
	}
}

func TestServerRootHandleLifetime(t *testing.T) {
	s := NewServer()
	s.BasePath = t.TempDir()
	if err := s.PinRoot(); err != nil {
		t.Fatal(err)
	}
	if err := s.PinRoot(); err == nil {
		t.Fatal("pinned source root twice")
	}
	root := s.root
	if _, err := root.Stat(); err != nil {
		t.Fatalf("pinned root is closed: %v", err)
	}
	session := s.NewSession()
	authenticateTestSession(t, session)
	if err := session.SelectRoot(".", nil); err != nil {
		t.Fatal(err)
	}
	selected := session.root
	s.CloseFiles()
	if _, err := root.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("listener root remains open: %v", err)
	}
	if _, err := selected.Stat(); err != nil {
		t.Fatalf("session root closed with listener: %v", err)
	}
	session.CloseFiles()
	if _, err := selected.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("session root remains open: %v", err)
	}
}

func TestPinRootRejectsNonDirectoryAndSymlink(t *testing.T) {
	base := t.TempDir()
	file := filepath.Join(base, "file")
	if err := os.WriteFile(file, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "link")
	if err := os.Symlink(base, link); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{file, link, filepath.Join(base, "missing")} {
		s := NewServer()
		s.BasePath = path
		if err := s.PinRoot(); err == nil {
			t.Fatalf("accepted invalid source root %q", path)
		}
		if s.root != nil {
			t.Fatalf("pinned invalid source root %q", path)
		}
	}
}
