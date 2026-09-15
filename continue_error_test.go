package fastsync

import (
	"fmt"
	"net/rpc"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

type warmFailureServer struct {
	*Server
	calls     atomic.Int32
	permanent bool
}

func (s *warmFailureServer) List(path string, r *FileListResponse) error {
	if path == "/bad" && (s.calls.Add(1) == 1 || s.permanent) {
		return fmt.Errorf("injected listing failure")
	}
	return s.Server.List(path, r)
}
func TestWarmupErrorContinuesCopyButFailsRun(t *testing.T) {
	for _, permanent := range []bool{false, true} {
		t.Run(fmt.Sprint(permanent), func(t *testing.T) {
			src, dst := t.TempDir(), t.TempDir()
			writeTestFile(t, src, "bad/file", "retry me")
			writeTestFile(t, src, "good/file", "copy me")
			if err := os.MkdirAll(filepath.Join(dst, "bad"), 0755); err != nil {
				t.Fatal(err)
			}
			// A reusable group found after the error must survive into pass 2.
			writeTestFile(t, src, "b/shared", "shared")
			fixedFileTime(t, filepath.Join(src, "b/shared"))
			if err := os.MkdirAll(filepath.Join(src, "a"), 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.Link(filepath.Join(src, "b/shared"), filepath.Join(src, "a/shared")); err != nil {
				t.Fatal(err)
			}
			writeTestFile(t, dst, "b/shared", "shared")
			fixedFileTime(t, filepath.Join(dst, "b/shared"))
			before, err := os.Stat(filepath.Join(dst, "b/shared"))
			if err != nil {
				t.Fatal(err)
			}
			s := &warmFailureServer{Server: NewServer(), permanent: permanent}
			s.BasePath = src
			registry := rpc.NewServer()
			registerTestRPCServer(t, registry, s)
			c := newTestClient(dst)
			c.PreserveHardlinks = true
			if err := c.Run(newTestRPCClientForServer(t, registry)); err == nil {
				t.Fatal("warmup error was forgotten")
			}
			if got := readTestFile(t, dst, "good/file"); got != "copy me" {
				t.Fatal("unrelated copy aborted")
			}
			after, err := os.Stat(filepath.Join(dst, "a/shared"))
			if err != nil || !os.SameFile(before, after) {
				t.Fatal("warm cache not used after error")
			}
			if !permanent && readTestFile(t, dst, "bad/file") != "retry me" {
				t.Fatal("second pass did not retry failed directory")
			}
			if s.calls.Load() < 2 {
				t.Fatal("copy pass never revisited failed listing")
			}
		})
	}
}
