package fastsync

import (
	"bytes"
	"encoding/json"
	"io"
	"net/rpc"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

type verificationChangingServer struct {
	*Server
	hashes      atomic.Int64
	stage       string
	change      func() error
	changeAgain func() error
}

func (s *verificationChangingServer) Hash(path string, reply *string) error {
	if err := s.Server.Hash(path, reply); err != nil {
		return err
	}
	count := s.hashes.Add(1)
	if count == 1 && s.stage == "after-hash" {
		return s.change()
	}
	if count == 2 && s.changeAgain != nil {
		return s.changeAgain()
	}
	return nil
}

func (s *verificationChangingServer) List(path string, reply *FileListResponse) error {
	if path == "/b" && s.stage == "before-follower" && s.hashes.Load() > 0 {
		if err := s.change(); err != nil {
			return err
		}
	}
	return s.Server.List(path, reply)
}

func TestVerificationRevalidatesChangingSourceHardlinks(t *testing.T) {
	for _, stage := range []string{"after-hash", "before-follower"} {
		for _, kind := range []string{"add-link", "remove-link", "content", "permissions", "replacement", "changing-again"} {
			t.Run(stage+"/"+kind, func(t *testing.T) {
				src, dst := makeABC(t)
				excluded := filepath.Join(src, "excluded")
				if err := os.Mkdir(excluded, 0755); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(src, "a/file")
				if err := os.Link(path, filepath.Join(excluded, "old-link")); err != nil {
					t.Fatal(err)
				}
				configure := func(c *Client) {
					c.PreserveHardlinks = true
					c.Include = []string{"a", "b"}
				}
				runTestSync(t, src, dst, configure)
				if stage == "before-follower" {
					path = filepath.Join(src, "b/file")
				}
				before, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				server := &verificationChangingServer{Server: NewServer(), stage: stage}
				server.BasePath = src
				t.Cleanup(server.CloseFiles)
				if kind == "changing-again" {
					server.changeAgain = func() error {
						return os.Link(path, filepath.Join(excluded, "another-link"))
					}
				}
				server.change = func() error {
					switch kind {
					case "add-link", "changing-again":
						return os.Link(path, filepath.Join(excluded, "new-link"))
					case "remove-link":
						return os.Remove(filepath.Join(excluded, "old-link"))
					case "content":
						if err := os.WriteFile(path, []byte("changed bad"), 0644); err != nil {
							return err
						}
						return os.Chtimes(path, before.ModTime(), before.ModTime())
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
						panic("unknown test mutation")
					}
				}
				registry := rpc.NewServer()
				registerTestRPCServer(t, registry, server)
				client := newTestClient(dst)
				configure(client)
				var report bytes.Buffer
				err = client.Verify(newTestRPCClientForServer(t, registry), &report)
				accepted := kind == "add-link" || kind == "remove-link"
				if (err == nil) != accepted {
					t.Fatalf("accepted=%v, error=%v\n%s", accepted, err, report.String())
				}
				var summary VerificationRecord
				linked := false
				decoder := json.NewDecoder(&report)
				for {
					var record VerificationRecord
					if err := decoder.Decode(&record); err == io.EOF {
						break
					} else if err != nil {
						t.Fatal(err)
					}
					if record.Type == "summary" {
						summary = record
					}
					linked = linked || record.HardlinkTo != ""
				}
				if summary.Complete != accepted || (summary.Errors == 0) != accepted {
					t.Fatalf("incorrect completion: %+v", summary)
				}
				if accepted && (!linked || server.hashes.Load() != 2) {
					t.Fatalf("linked=%v hashes=%d, want linked and two hashes", linked, server.hashes.Load())
				}
			})
		}
	}
}
