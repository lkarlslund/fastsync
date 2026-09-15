package fastsync

import (
	"net/rpc"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

type resumeServer struct {
	*Server
	opens, checks, chunks atomic.Int64
}

func (s *resumeServer) Open(path string, reply *interface{}) error {
	s.opens.Add(1)
	return s.Server.Open(path, reply)
}
func (s *resumeServer) ChecksumChunk(args GetChunkArgs, reply *uint64) error {
	s.checks.Add(1)
	return s.Server.ChecksumChunk(args, reply)
}
func (s *resumeServer) GetChunk(args GetChunkArgs, reply *[]byte) error {
	s.chunks.Add(1)
	return s.Server.GetChunk(args, reply)
}
func resumeSync(t *testing.T, source, dest string, checksum bool) (*Client, *resumeServer) {
	t.Helper()
	server := &resumeServer{Server: NewServer()}
	server.BasePath = source
	registry := rpc.NewServer()
	registerTestRPCServer(t, registry, server)
	client := newTestClient(dest)
	client.PreserveHardlinks = true
	client.AlwaysChecksum = checksum
	client.Options.SendXattr = true
	if err := client.Run(newTestRPCClientForServer(t, registry)); err != nil {
		t.Fatal(err)
	}
	return client, server
}
func fixedFileTime(t *testing.T, path string) {
	t.Helper()
	stamp := time.Unix(1700000000, 0)
	if err := os.Chtimes(path, stamp, stamp); err != nil {
		t.Fatal(err)
	}
}
func TestMatchingHardlinksRespectChecksumPolicyWithoutRewriting(t *testing.T) {
	for _, checksum := range []bool{false, true} {
		t.Run(map[bool]string{false: "quick-check", true: "checksum"}[checksum], func(t *testing.T) {
			source, dest := t.TempDir(), t.TempDir()
			writeTestFile(t, source, "a", "correct contents")
			fixedFileTime(t, filepath.Join(source, "a"))
			if err := os.Link(filepath.Join(source, "a"), filepath.Join(source, "b")); err != nil {
				t.Fatal(err)
			}
			resumeSync(t, source, dest, false)
			before, err := os.Stat(filepath.Join(dest, "a"))
			if err != nil {
				t.Fatal(err)
			}
			client, server := resumeSync(t, source, dest, checksum)
			for _, name := range []string{"a", "b"} {
				after, err := os.Stat(filepath.Join(dest, name))
				if err != nil {
					t.Fatal(err)
				}
				if !os.SameFile(before, after) {
					t.Fatalf("unchanged inode replaced: %s", name)
				}
			}
			if client.Perf.Get(WrittenBytes) != 0 || server.chunks.Load() != 0 {
				t.Fatal("matching data rewritten or fetched")
			}
			if !checksum && (server.opens.Load() != 0 || server.checks.Load() != 0 || client.Perf.Get(ReadBytes) != 0) {
				t.Fatal("default resume read/checked file content")
			}
			if checksum && (server.checks.Load() == 0 || server.opens.Load() != 1) {
				t.Fatal("checksum did not validate shared content once")
			}
		})
	}
}
func TestHardlinkCorruptionOnlyCheckedWhenRequested(t *testing.T) {
	source, dest := t.TempDir(), t.TempDir()
	writeTestFile(t, source, "a", "correct")
	fixedFileTime(t, filepath.Join(source, "a"))
	if err := os.Link(filepath.Join(source, "a"), filepath.Join(source, "b")); err != nil {
		t.Fatal(err)
	}
	resumeSync(t, source, dest, false)
	if err := os.WriteFile(filepath.Join(dest, "a"), []byte("corrupt"), 0644); err != nil {
		t.Fatal(err)
	}
	fixedFileTime(t, filepath.Join(dest, "a"))
	_, server := resumeSync(t, source, dest, false)
	if server.checks.Load() != 0 || readTestFile(t, dest, "a") != "corrupt" {
		t.Fatal("quick-check policy overridden")
	}
	_, server = resumeSync(t, source, dest, true)
	if server.checks.Load() == 0 {
		t.Fatal("checksum not used")
	}
	for _, name := range []string{"a", "b"} {
		if readTestFile(t, dest, name) != "correct" {
			t.Fatal("checksum failed to repair contents")
		}
	}
}
func TestIndependentSourceInodesAreSplitWithoutRemoteData(t *testing.T) {
	source, dest := t.TempDir(), t.TempDir()
	for _, name := range []string{"a", "b"} {
		writeTestFile(t, source, name, "same")
		fixedFileTime(t, filepath.Join(source, name))
	}
	writeTestFile(t, dest, "a", "same")
	fixedFileTime(t, filepath.Join(dest, "a"))
	if err := os.Link(filepath.Join(dest, "a"), filepath.Join(dest, "b")); err != nil {
		t.Fatal(err)
	}
	_, server := resumeSync(t, source, dest, false)
	a, err := os.Stat(filepath.Join(dest, "a"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.Stat(filepath.Join(dest, "b"))
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(a, b) {
		t.Fatal("independent source files remain merged")
	}
	if server.opens.Load() != 0 || server.checks.Load() != 0 || server.chunks.Load() != 0 {
		t.Fatal("topology repair fetched/checked remote data")
	}
}
func TestSharedMetadataChangeDoesNotAlterIndependentHistory(t *testing.T) {
	source, dest := t.TempDir(), t.TempDir()
	for _, name := range []string{"a", "b"} {
		writeTestFile(t, source, name, "same")
		fixedFileTime(t, filepath.Join(source, name))
	}
	if err := os.Chmod(filepath.Join(source, "a"), 0755); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, dest, "a", "same")
	fixedFileTime(t, filepath.Join(dest, "a"))
	if err := os.Link(filepath.Join(dest, "a"), filepath.Join(dest, "b")); err != nil {
		t.Fatal(err)
	}
	_, server := resumeSync(t, source, dest, false)
	for name, mode := range map[string]os.FileMode{"a": 0755, "b": 0644} {
		fi, err := os.Stat(filepath.Join(dest, name))
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode().Perm() != mode {
			t.Fatalf("%s permissions changed through shared inode", name)
		}
	}
	if server.opens.Load() != 0 || server.checks.Load() != 0 {
		t.Fatal("metadata repair fetched/checked remote data")
	}
}
