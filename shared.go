package main

import (
	"io"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/snappy"
)

type compressedConn struct {
	*s2.Reader
	w *s2.Writer
	io.Closer
}

func CompressedReadWriteCloser(rwc io.ReadWriteCloser) io.ReadWriteCloser {
	r := snappy.NewReader(rwc)
	w := snappy.NewBufferedWriter(rwc)
	return &compressedConn{
		r,
		w,
		rwc,
	}
}

func (c *compressedConn) Write(p []byte) (n int, err error) {
	n, err = c.w.Write(p)
	c.w.Flush()
	return
}

func (c *compressedConn) Close() (err error) {
	c.w.Close()
	err = c.Closer.Close()
	return
}
