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
	if err := rpcServer.Register(server); err != nil {
		t.Fatalf("register server: %v", err)
	}

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
