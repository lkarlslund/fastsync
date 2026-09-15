package fastsync

import (
	"errors"
	"io"
	"net/rpc"
	"os"

	"github.com/cespare/xxhash/v2"
)

type streamChunk struct {
	data    []byte
	fetched bool
}

// streamRegular uses two circulating payload buffers and one delta scratch
// buffer. Admission reserves four blocks, allowing a block of decoder slack.
// Reservations precede allocation and remain held until producer and consumer exit.
func (c *Client) streamRegular(client *rpc.Client, remote FileInfo, local FileInfo, previous, stage *os.File) (err error) {
	bufferSize := int(min(int64(c.BlockSize), remote.Size))
	pool := make(chan []byte, 2)
	for i := 0; i < 2; i++ {
		pool <- make([]byte, 0, bufferSize)
	}
	chunks := make(chan streamChunk, 1)
	stopped := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		var result error
		defer func() { close(chunks); done <- result }()
		var scratch []byte
		if previous != nil {
			scratch = make([]byte, bufferSize)
		}
		for offset := int64(0); offset < remote.Size; {
			var data []byte
			select {
			case <-stopped:
				return
			case data = <-pool:
			}
			length := min(int64(c.BlockSize), remote.Size-offset)
			args := GetChunkArgs{Path: remote.Name, Offset: uint64(offset), Size: uint64(length)}
			matched := false
			if previous != nil && offset+length <= local.Size {
				if _, result = previous.ReadAt(scratch[:length], offset); result != nil {
					return
				}
				c.Perf.Add(ReadBytes, uint64(length))
				var checksum uint64
				if result = client.Call("Server.ChecksumChunk", args, &checksum); result != nil {
					return
				}
				matched = xxhash.Sum64(scratch[:length]) == checksum
			}
			if matched {
				data = append(data[:0], scratch[:length]...)
			} else {
				data = data[:0]
				if result = client.Call("Server.GetChunk", args, &data); result != nil {
					return
				}
				if int64(len(data)) != length {
					result = io.ErrUnexpectedEOF
					return
				}
			}
			select {
			case <-stopped:
				return
			case chunks <- streamChunk{data: data, fetched: !matched}:
			}
			offset += length
		}
	}()
	defer func() { close(stopped); err = errors.Join(err, <-done) }()
	// A small set of files is written sequentially; additional admitted files can
	// prefetch, but cannot create an unbounded backlog or take write permits while
	// waiting for their canonical hardlink owner.
	releaseFile := c.writerFiles.acquire()
	defer releaseFile(0, 0)
	for chunk := range chunks {
		n, writeErr := c.writeData(stage, chunk.data)
		if writeErr != nil {
			return writeErr
		}
		if n != len(chunk.data) {
			return io.ErrShortWrite
		}
		c.Perf.Add(WrittenBytes, uint64(n))
		if chunk.fetched {
			c.Perf.Add(TransferredFileBytes, uint64(n))
		}
		pool <- chunk.data[:0]
	}
	return nil
}
