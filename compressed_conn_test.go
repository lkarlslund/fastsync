package fastsync

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type blockedCompressionConn struct {
	entered, closed chan struct{}
	enter, close    sync.Once
	closeCalls      atomic.Int64
}

func (c *blockedCompressionConn) Read([]byte) (int, error) {
	<-c.closed
	return 0, io.EOF
}
func (c *blockedCompressionConn) Write([]byte) (int, error) {
	c.enter.Do(func() { close(c.entered) })
	<-c.closed
	return 0, io.ErrClosedPipe
}
func (c *blockedCompressionConn) Close() error {
	c.closeCalls.Add(1)
	c.close.Do(func() { close(c.closed) })
	return nil
}

func TestCompressedCloseUnblocksConcurrentWrite(t *testing.T) {
	transport := &blockedCompressionConn{entered: make(chan struct{}), closed: make(chan struct{})}
	conn := CompressedReadWriteCloser(transport)
	t.Cleanup(func() { _ = transport.Close() })
	written := make(chan error, 1)
	go func() { _, err := conn.Write([]byte("synthetic payload")); written <- err }()
	select {
	case <-transport.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("compressed write did not reach transport")
	}
	closed := make(chan error, 1)
	go func() { closed <- conn.Close() }()
	select {
	case err := <-closed:
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("close error = %v, want closed pipe", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("close deadlocked behind a blocked write")
	}
	if err := <-written; err != nil && !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("write error = %v, want accepted async write or closed pipe", err)
	}
	if _, err := conn.Write([]byte("after close")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("write after close = %v", err)
	}
	if err := conn.Close(); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("repeated close = %v", err)
	}
	if calls := transport.closeCalls.Load(); calls != 1 {
		t.Fatalf("transport close calls = %d, want 1", calls)
	}
}

type discardCompressionConn struct{ closes atomic.Int64 }

func (*discardCompressionConn) Read([]byte) (int, error)    { return 0, io.EOF }
func (*discardCompressionConn) Write(p []byte) (int, error) { return len(p), nil }
func (c *discardCompressionConn) Close() error              { c.closes.Add(1); return nil }

func TestCompressedCloseAfterSuccessfulWrite(t *testing.T) {
	transport := &discardCompressionConn{}
	conn := CompressedReadWriteCloser(transport)
	if _, err := conn.Write([]byte("synthetic payload")); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if calls := transport.closes.Load(); calls != 1 {
		t.Fatalf("transport close calls = %d, want 1", calls)
	}
}
