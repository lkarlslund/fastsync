package fastsync

import (
	"errors"
	"io"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/ugorji/go/codec"
)

func TestMain(m *testing.M) {
	Logger = zerolog.New(io.Discard)
	os.Exit(m.Run())
}

func newTestRPCClient(t *testing.T, sourceDir string) *rpc.Client {
	t.Helper()

	rpcServer := rpc.NewServer()
	server := NewServer()
	server.BasePath = sourceDir
	registerTestRPCServer(t, rpcServer, server)

	return newTestRPCClientForServer(t, rpcServer)
}

func registerTestRPCServer(t *testing.T, rpcServer *rpc.Server, server any) {
	t.Helper()

	if err := rpcServer.RegisterName("Server", server); err != nil {
		t.Fatalf("register server: %v", err)
	}
}

func newTestRPCClientForServer(t *testing.T, rpcServer *rpc.Server) *rpc.Client {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("close listener: %v", err)
		}
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				var h codec.MsgpackHandle
				rpcServer.ServeCodec(codec.GoRpc.ServerCodec(CompressedReadWriteCloser(conn), &h))
			}(conn)
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-done
	})

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial server: %v", err)
	}
	var h codec.MsgpackHandle
	client := rpc.NewClientWithCodec(codec.GoRpc.ClientCodec(CompressedReadWriteCloser(conn), &h))
	t.Cleanup(func() {
		if err := client.Close(); err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.ErrClosedPipe) {
			t.Errorf("close rpc client: %v", err)
		}
	})

	return client
}

func newTestClient(destDir string) *Client {
	client := NewClient()
	client.BasePath = destDir
	client.ParallelDir = 2
	client.ParallelFile = 2
	client.QueueSize = 8
	client.BlockSize = 4
	client.PreserveHardlinks = false
	client.Options.SendXattr = false
	return client
}

func runTestSync(t *testing.T, sourceDir, destDir string, configure func(*Client)) *Client {
	t.Helper()

	rpcClient := newTestRPCClient(t, sourceDir)
	client := newTestClient(destDir)
	if configure != nil {
		configure(client)
	}
	if err := client.Run(rpcClient); err != nil {
		t.Fatalf("client run: %v", err)
	}
	return client
}

func writeTestFile(t *testing.T, root, name, contents string) {
	t.Helper()

	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func readTestFile(t *testing.T, root, name string) string {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(root, name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(data)
}

func assertNotExists(t *testing.T, path string) {
	t.Helper()

	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("path %s exists or returned unexpected error: %v", path, err)
	}
}

func TestClientServerSyncCreatesTree(t *testing.T) {
	source := t.TempDir()
	dest := t.TempDir()
	writeTestFile(t, source, "root.txt", "root file")
	writeTestFile(t, source, "nested/a.txt", "alpha")
	writeTestFile(t, source, "nested/deeper/b.txt", "bravo")
	if err := os.MkdirAll(filepath.Join(source, "empty"), 0o755); err != nil {
		t.Fatalf("mkdir empty source dir: %v", err)
	}

	runTestSync(t, source, dest, nil)

	if got, want := readTestFile(t, dest, "root.txt"), "root file"; got != want {
		t.Fatalf("root.txt = %q, want %q", got, want)
	}
	if got, want := readTestFile(t, dest, "nested/a.txt"), "alpha"; got != want {
		t.Fatalf("nested/a.txt = %q, want %q", got, want)
	}
	if got, want := readTestFile(t, dest, "nested/deeper/b.txt"), "bravo"; got != want {
		t.Fatalf("nested/deeper/b.txt = %q, want %q", got, want)
	}
	if info, err := os.Stat(filepath.Join(dest, "empty")); err != nil || !info.IsDir() {
		t.Fatalf("empty directory stat = %v, %v; want directory", info, err)
	}
}

func TestClientServerSyncUpdatesExistingFileByChunks(t *testing.T) {
	source := t.TempDir()
	dest := t.TempDir()
	writeTestFile(t, source, "chunked.txt", "aaaabbbbcccc")
	writeTestFile(t, dest, "chunked.txt", "aaaaXXXXcccc")

	client := runTestSync(t, source, dest, func(client *Client) {
		client.AlwaysChecksum = true
		client.BlockSize = 4
	})

	if got, want := readTestFile(t, dest, "chunked.txt"), "aaaabbbbcccc"; got != want {
		t.Fatalf("chunked.txt = %q, want %q", got, want)
	}
	if got, wantLessThan := client.Perf.Get(WrittenBytes), uint64(len("aaaabbbbcccc")); got >= wantLessThan {
		t.Fatalf("written bytes = %d, want less than %d to prove matching chunks were skipped", got, wantLessThan)
	}
}

func TestClientServerSyncDeleteRemovesExtraLocalEntries(t *testing.T) {
	source := t.TempDir()
	dest := t.TempDir()
	writeTestFile(t, source, "keep.txt", "keep")
	writeTestFile(t, dest, "keep.txt", "stale")
	writeTestFile(t, dest, "extra-file.txt", "delete me")
	writeTestFile(t, dest, "extra-dir/extra.txt", "delete me too")

	runTestSync(t, source, dest, func(client *Client) {
		client.Delete = true
	})

	if got, want := readTestFile(t, dest, "keep.txt"), "keep"; got != want {
		t.Fatalf("keep.txt = %q, want %q", got, want)
	}
	assertNotExists(t, filepath.Join(dest, "extra-file.txt"))
	assertNotExists(t, filepath.Join(dest, "extra-dir"))
}

func TestClientServerSyncCopiesSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation requires additional privileges on some Windows systems")
	}

	source := t.TempDir()
	dest := t.TempDir()
	writeTestFile(t, source, "target.txt", "target")
	if err := os.Symlink("target.txt", filepath.Join(source, "link.txt")); err != nil {
		t.Skipf("symlink not supported in this environment: %v", err)
	}

	runTestSync(t, source, dest, nil)

	got, err := os.Readlink(filepath.Join(dest, "link.txt"))
	if err != nil {
		t.Fatalf("readlink: %v", err)
	}
	if got != "target.txt" {
		t.Fatalf("symlink target = %q, want %q", got, "target.txt")
	}
}

func TestClientServerSyncPreservesHardlinks(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("hardlink metadata differs on Windows")
	}

	source := t.TempDir()
	dest := t.TempDir()
	writeTestFile(t, source, "a.txt", "shared")
	if err := os.Link(filepath.Join(source, "a.txt"), filepath.Join(source, "b.txt")); err != nil {
		t.Skipf("hardlinks not supported in this environment: %v", err)
	}

	runTestSync(t, source, dest, func(client *Client) {
		client.PreserveHardlinks = true
	})

	a, err := PathToFileInfo(filepath.Join(dest, "a.txt"))
	if err != nil {
		t.Fatalf("stat destination a.txt: %v", err)
	}
	b, err := PathToFileInfo(filepath.Join(dest, "b.txt"))
	if err != nil {
		t.Fatalf("stat destination b.txt: %v", err)
	}
	if a.Dev != b.Dev || a.Inode != b.Inode {
		t.Fatalf("destination files are not hardlinked: a=%d/%d b=%d/%d", a.Dev, a.Inode, b.Dev, b.Inode)
	}
}

type failingListServer struct {
	*Server
	failPath string
}

func (s *failingListServer) List(path string, reply *FileListResponse) error {
	if path == s.failPath {
		return errors.New("forced list failure")
	}
	return s.Server.List(path, reply)
}

func TestClientServerSyncListErrorDoesNotHang(t *testing.T) {
	source := t.TempDir()
	dest := t.TempDir()
	if err := os.MkdirAll(filepath.Join(source, "bad"), 0o755); err != nil {
		t.Fatalf("mkdir bad source dir: %v", err)
	}

	rpcServer := rpc.NewServer()
	server := NewServer()
	server.BasePath = source
	registerTestRPCServer(t, rpcServer, &failingListServer{
		Server:   server,
		failPath: "/bad",
	})
	rpcClient := newTestRPCClientForServer(t, rpcServer)

	client := newTestClient(dest)
	done := make(chan error, 1)
	go func() {
		done <- client.Run(rpcClient)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("client run returned unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client run did not finish after a child directory list failure")
	}

	if _, directories, _, stack := client.Stats(); directories != 0 || stack != 0 {
		t.Fatalf("client queues after failed list: directories=%d stack=%d, want both 0", directories, stack)
	}
}

func TestClientRejectsInvalidQueueSettings(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*Client)
	}{
		{name: "no directory workers", configure: func(client *Client) { client.ParallelDir = 0 }},
		{name: "no file workers", configure: func(client *Client) { client.ParallelFile = 0 }},
		{name: "negative queue size", configure: func(client *Client) { client.QueueSize = -1 }},
		{name: "zero block size", configure: func(client *Client) { client.BlockSize = 0 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newTestClient(t.TempDir())
			tt.configure(client)
			if err := client.Run(nil); err == nil {
				t.Fatal("Client.Run succeeded with invalid queue settings, want error")
			}
		})
	}
}

func TestWaitForFileInfoIsBoundedWhenHardlinkTargetIsMissing(t *testing.T) {
	started := time.Now()
	_, err := waitForFileInfo(filepath.Join(t.TempDir(), "missing"), 3, time.Millisecond)
	if err == nil {
		t.Fatal("waitForFileInfo succeeded for missing path")
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("waitForFileInfo took %v, want bounded retry", elapsed)
	}
}
