package fastsync

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/cespare/xxhash/v2"
)

func TestServerLocalPath(t *testing.T) {
	base := t.TempDir()
	s := &Server{BasePath: base}

	tests := []struct {
		name    string
		path    string
		want    string
		wantErr error
	}{
		{name: "root slash", path: "/", want: base},
		{name: "root dot", path: ".", want: base},
		{name: "relative", path: "dir/file", want: filepath.Join(base, "dir", "file")},
		{name: "protocol absolute", path: "/dir/file", want: filepath.Join(base, "dir", "file")},
		{name: "parent escape", path: "../secret", wantErr: ErrInvalidPath},
		{name: "nested parent escape", path: "dir/../../secret", wantErr: ErrInvalidPath},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.localPath(tt.path)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("localPath(%q) error = %v, want %v", tt.path, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("localPath(%q) unexpected error: %v", tt.path, err)
			}
			if got != tt.want {
				t.Fatalf("localPath(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

func TestServerRequiresHello(t *testing.T) {
	s := NewServer()
	s.BasePath = t.TempDir()

	var listReply FileListResponse
	if err := s.List("/", &listReply); !errors.Is(err, ErrPleaseSayHello) {
		t.Fatalf("List before Hello error = %v, want %v", err, ErrPleaseSayHello)
	}

	var statReply FileInfo
	if err := s.Stat("/", &statReply); !errors.Is(err, ErrPleaseSayHello) {
		t.Fatalf("Stat before Hello error = %v, want %v", err, ErrPleaseSayHello)
	}

	var openReply interface{}
	if err := s.Open("file.txt", &openReply); !errors.Is(err, ErrPleaseSayHello) {
		t.Fatalf("Open before Hello error = %v, want %v", err, ErrPleaseSayHello)
	}

	var chunk []byte
	if err := s.GetChunk(GetChunkArgs{Path: "file.txt", Size: 1}, &chunk); !errors.Is(err, ErrPleaseSayHello) {
		t.Fatalf("GetChunk before Hello error = %v, want %v", err, ErrPleaseSayHello)
	}

	var checksum uint64
	if err := s.ChecksumChunk(GetChunkArgs{Path: "file.txt", Size: 1}, &checksum); !errors.Is(err, ErrPleaseSayHello) {
		t.Fatalf("ChecksumChunk before Hello error = %v, want %v", err, ErrPleaseSayHello)
	}

	if err := s.Close("file.txt", &openReply); !errors.Is(err, ErrPleaseSayHello) {
		t.Fatalf("Close before Hello error = %v, want %v", err, ErrPleaseSayHello)
	}

	var shutdownReply any
	if err := s.Shutdown(nil, &shutdownReply); !errors.Is(err, ErrPleaseSayHello) {
		t.Fatalf("Shutdown before Hello error = %v, want %v", err, ErrPleaseSayHello)
	}
}

func TestServerHelloRejectsProtocolMismatch(t *testing.T) {
	s := NewServer()

	var reply any
	err := s.Hello(SharedOptions{ProtocolVersion: PROTOCOLVERSION + 1}, &reply)
	if err == nil {
		t.Fatal("Hello with protocol mismatch succeeded, want error")
	}
}

func TestServerChunkErrorsAndClose(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "file.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	s := NewServer()
	s.BasePath = dir
	var reply any
	if err := s.Hello(SharedOptions{ProtocolVersion: PROTOCOLVERSION}, &reply); err != nil {
		t.Fatalf("hello: %v", err)
	}

	var data []byte
	err := s.GetChunk(GetChunkArgs{Path: "missing.txt", Size: 1}, &data)
	if err == nil {
		t.Fatal("GetChunk on unopened file succeeded, want error")
	}

	if err := s.Open("file.txt", nil); err != nil {
		t.Fatalf("open: %v", err)
	}
	err = s.GetChunk(GetChunkArgs{Path: "file.txt", Offset: 4, Size: 2}, &data)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("GetChunk past EOF error = %v, want %v", err, io.EOF)
	}

	var checksum uint64
	if err := s.ChecksumChunk(GetChunkArgs{Path: "file.txt", Offset: 1, Size: 3}, &checksum); err != nil {
		t.Fatalf("checksum chunk: %v", err)
	}
	if want := xxhash.Sum64([]byte("ell")); checksum != want {
		t.Fatalf("checksum = %x, want %x", checksum, want)
	}

	if err := s.Close("file.txt", nil); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := s.Close("file.txt", nil); err == nil {
		t.Fatal("second Close succeeded, want error")
	}
}
