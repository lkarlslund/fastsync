package fastsync

import (
	"fmt"
	"os"
	"sync"
	"time"
)

type pendingFile struct {
	original, handle *os.File
	ioMu             sync.Mutex
	dirty            uint64
	closed, queued   bool
}
type fileFlusher struct {
	mu               sync.Mutex
	cond             *sync.Cond
	files            map[*os.File]*pendingFile
	jobs             chan *pendingFile
	slots            chan struct{}
	stop             chan struct{}
	timerDone        chan struct{}
	workers          sync.WaitGroup
	pending, maximum uint64
	threshold        uint64
	err              error
	client           *Client
}

func newFileFlusher(c *Client) *fileFlusher {
	m := &fileFlusher{files: make(map[*os.File]*pendingFile), jobs: make(chan *pendingFile, c.FlushFiles), slots: make(chan struct{}, c.FlushFiles), stop: make(chan struct{}), timerDone: make(chan struct{}), maximum: uint64(c.FlushBytes), client: c}
	m.threshold = min(uint64(16<<20), m.maximum/4)
	m.cond = sync.NewCond(&m.mu)
	for i := 0; i < c.FlushWorkers; i++ {
		m.workers.Add(1)
		go m.worker()
	}
	go func() {
		defer close(m.timerDone)
		timer := time.NewTicker(c.FlushInterval)
		defer timer.Stop()
		for {
			select {
			case <-m.stop:
				return
			case <-timer.C:
				m.mu.Lock()
				m.scheduleAll()
				m.mu.Unlock()
			}
		}
	}()
	return m
}
func (m *fileFlusher) schedule(f *pendingFile) {
	if !f.queued && (f.dirty > 0 || f.closed) {
		f.queued = true
		m.jobs <- f
	}
}
func (m *fileFlusher) scheduleAll() {
	for _, f := range m.files {
		m.schedule(f)
	}
}
func (m *fileFlusher) get(file *os.File) (*pendingFile, error) {
	m.mu.Lock()
	f := m.files[file]
	err := m.err
	m.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if f != nil {
		return f, nil
	}
	// One transfer worker owns each original file descriptor.
	m.slots <- struct{}{}
	handle, err := duplicateFlushFile(file)
	if err != nil {
		<-m.slots
		return nil, err
	}
	f = &pendingFile{original: file, handle: handle}
	m.mu.Lock()
	m.files[file] = f
	m.mu.Unlock()
	return f, nil
}
func (m *fileFlusher) reserve(n uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n > m.maximum {
		return fmt.Errorf("write block exceeds pending-flush byte budget")
	}
	for m.pending+n > m.maximum && m.err == nil {
		m.scheduleAll()
		m.cond.Wait()
	}
	if m.err != nil {
		return m.err
	}
	m.pending += n
	return nil
}
func (m *fileFlusher) finishFile(file *os.File) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if f := m.files[file]; f != nil {
		f.closed = true
		m.schedule(f)
	}
}
func (m *fileFlusher) worker() {
	defer m.workers.Done()
	for f := range m.jobs {
		// Serialize only this file with its writers. Other files keep copying.
		f.ioMu.Lock()
		m.mu.Lock()
		bytes := f.dirty
		m.mu.Unlock()
		release := m.client.writeGate.acquire()
		started := time.Now()
		var err error
		if m.client.checkpointOverride != nil {
			err = m.client.timeLocalIO(m.client.checkpointOverride)
		} else {
			err = m.client.timeLocalIO(f.handle.Sync)
		}
		elapsed := time.Since(started)
		m.mu.Lock()
		f.dirty -= bytes
		m.pending -= bytes
		f.queued = false
		if err != nil && m.err == nil {
			m.err = fmt.Errorf("flush %s: %w", f.handle.Name(), err)
		}
		if f.closed {
			if e := f.handle.Close(); e != nil && m.err == nil {
				m.err = e
			}
			delete(m.files, f.original)
			<-m.slots
		}
		m.cond.Broadcast()
		m.mu.Unlock()
		f.ioMu.Unlock()
		confirmed := uint64(0)
		if err == nil {
			confirmed = bytes
		}
		release(confirmed, elapsed)
		if err == nil {
			c := m.client
			c.tuningMu.Lock()
			c.tuningState.FlushedBytes += bytes
			c.tuningState.FlushCount++
			c.tuningState.LastFlush = elapsed
			c.tuningMu.Unlock()
		}
	}
}
func (m *fileFlusher) finish() error {
	close(m.stop)
	<-m.timerDone
	m.mu.Lock()
	for _, f := range m.files {
		f.closed = true
		m.schedule(f)
	}
	for len(m.files) > 0 {
		m.cond.Wait()
	}
	err := m.err
	m.mu.Unlock()
	close(m.jobs)
	m.workers.Wait()
	return err
}
func (c *Client) finishDataFile(file *os.File) {
	if c.flusher != nil {
		if c.resume != nil {
			if _, err := c.flusher.get(file); err != nil {
				c.recordError("register checkpoint file flush: %v", err)
				return
			}
		}
		c.flusher.finishFile(file)
	}
}
func (c *Client) writeData(file *os.File, data []byte) (n int, err error) {
	var f *pendingFile
	if c.flusher != nil {
		f, err = c.flusher.get(file)
		if err != nil {
			return 0, err
		}
		if err = c.flusher.reserve(uint64(len(data))); err != nil {
			return 0, err
		}
		f.ioMu.Lock()
		defer f.ioMu.Unlock()
	}
	var release func(uint64, time.Duration)
	if c.writeGate != nil {
		release = c.writeGate.acquire()
	}
	started := time.Now()
	err = c.timeLocalIO(func() error { var e error; n, e = file.Write(data); return e })
	if n > 0 {
		c.writtenTotal.Add(uint64(n))
	}
	if f != nil {
		m := c.flusher
		m.mu.Lock()
		m.pending -= uint64(len(data) - n)
		f.dirty += uint64(n)
		if f.dirty >= m.threshold {
			m.schedule(f)
		}
		m.cond.Broadcast()
		m.mu.Unlock()
	}
	if release != nil {
		bytes := uint64(n)
		if f != nil {
			bytes = 0
		} // Adaptive throughput uses completed flushes.
		release(bytes, time.Since(started))
	}
	return
}

func (c *Client) flushState() {
	c.tuningMu.Lock()
	m := c.flusher
	c.tuningMu.Unlock()
	if m == nil {
		return
	}
	m.mu.Lock()
	pending, files := m.pending, len(m.files)
	m.mu.Unlock()
	c.tuningMu.Lock()
	now := time.Now()
	if !c.lastCheckpoint.IsZero() && now.Sub(c.lastCheckpoint) < time.Second {
		c.tuningState.PendingFlushBytes = pending
		c.tuningState.PendingFlushFiles = files
		c.tuningMu.Unlock()
		return
	}
	if !c.lastCheckpoint.IsZero() {
		rate := float64(c.tuningState.FlushedBytes-c.lastCheckpointBytes) / now.Sub(c.lastCheckpoint).Seconds()
		c.tuningState.FlushedBytesPerSecond = 0.8*c.tuningState.FlushedBytesPerSecond + 0.2*rate
	}
	c.lastCheckpoint = now
	c.lastCheckpointBytes = c.tuningState.FlushedBytes
	c.tuningState.PendingFlushBytes = pending
	c.tuningState.PendingFlushFiles = files
	c.tuningMu.Unlock()
}
