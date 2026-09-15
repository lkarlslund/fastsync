package fastsync

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestSelectedSourceCopyAndVerify(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	writeTestFile(t, src, "server-a/old/data", "archive")
	writeTestFile(t, src, "other/data", "other server")
	runTestSync(t, src, dst, func(c *Client) { c.SourcePath = "server-a" })
	if got := readTestFile(t, dst, "old/data"); got != "archive" {
		t.Fatal(got)
	}
	if _, err := os.Stat(filepath.Join(dst, "other")); !os.IsNotExist(err) {
		t.Fatal("copied sibling")
	}
	c := newTestClient(dst)
	c.SourcePath = "server-a"
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
	for _, path := range []string{"../", base, "escape", "missing"} {
		if err := s.NewSession().SelectRoot(path, nil); err == nil {
			t.Fatalf("accepted %q", path)
		}
	}
	session := s.NewSession()
	if err := session.SelectRoot("server-a", nil); err != nil {
		t.Fatal(err)
	}
	if s.BasePath != base || s.NewSession().BasePath != base {
		t.Fatal("changed listener root")
	}
	if err := session.Hello(SharedOptions{ProtocolVersion: PROTOCOLVERSION}, nil); err != nil {
		t.Fatal(err)
	}
	if err := session.SelectRoot(".", nil); err == nil {
		t.Fatal("changed root after Hello")
	}
}
