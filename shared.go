package fastsync

import (
	"io"
	"sync/atomic"

	"github.com/klauspost/compress/s2"
)

const PROTOCOLVERSION = 1

type SharedOptions struct {
	ProtocolVersion int
	SendXattr       bool
}

type compressedConn struct {
	r *s2.Reader
	w *s2.Writer
	c io.Closer
}

func CompressedReadWriteCloser(rwc io.ReadWriteCloser) io.ReadWriteCloser {
	r := s2.NewReader(rwc)
	w := s2.NewWriter(rwc, s2.WriterFlushOnWrite())
	return &compressedConn{
		r,
		w,
		rwc,
	}
}

func (c *compressedConn) Read(p []byte) (n int, err error) {
	n, err = c.r.Read(p)
	return
}

func (c *compressedConn) Write(p []byte) (n int, err error) {
	n, err = c.w.Write(p)
	return
}

func (c *compressedConn) Close() (err error) {
	c.w.Close()
	err = c.c.Close()
	return
}

// performance related stuff
type PerformanceCounterType int

const (
	SentOverWire PerformanceCounterType = iota
	RecievedOverWire
	SentBytes
	RecievedBytes
	WrittenBytes
	ReadBytes
	BytesProcessed
	FilesProcessed
	DirectoriesProcessed
	EntriesDeleted
	FileQueue
	FolderQueue
	maxperformancecountertype
)

type AtomicAdder func(uint64)

type PerformanceEntry struct {
	counters [maxperformancecountertype]uint64
}

func (pe PerformanceEntry) Add(pe2 PerformanceEntry) PerformanceEntry {
	var result PerformanceEntry
	for i := 0; i < int(maxperformancecountertype); i++ {
		result.counters[i] = pe.counters[i] + pe2.counters[i]
	}
	return result
}

func (pe PerformanceEntry) Get(pc PerformanceCounterType) uint64 {
	return pe.counters[pc]
}

type performance struct {
	current    atomic.Pointer[PerformanceEntry]
	maxhistory int
	entries    []PerformanceEntry
}

func NewPerformance() *performance {
	p := performance{}
	p.current.Store(&PerformanceEntry{})
	p.maxhistory = 300
	return &p
}

func (p *performance) GetAtomicAdder(ct PerformanceCounterType) AtomicAdder {
	return func(v uint64) {
		p.Add(ct, v)
	}
}

func (p *performance) Add(ct PerformanceCounterType, v uint64) {
	pc := p.current.Load()
	atomic.AddUint64(&pc.counters[ct], v)
}

func (p *performance) Get(ct PerformanceCounterType) uint64 {
	pc := p.current.Load()
	return atomic.LoadUint64(&pc.counters[ct])
}

func (p *performance) NextHistory() PerformanceEntry {
	newhistory := PerformanceEntry{}
	oldhistory := p.current.Swap(&newhistory)
	if len(p.entries) > p.maxhistory {
		copy(p.entries, p.entries[1:])
		p.entries[len(p.entries)-1] = *oldhistory
	} else {
		p.entries = append(p.entries, *oldhistory)
	}
	return *oldhistory
}

type PerformanceWrapperReadWriteCloser struct {
	onWrite, onRead AtomicAdder
	rwc             io.ReadWriteCloser
}

func NewPerformanceWrapper(rwc io.ReadWriteCloser, onRead, onWrite AtomicAdder) *PerformanceWrapperReadWriteCloser {
	return &PerformanceWrapperReadWriteCloser{onRead, onWrite, rwc}
}

func (pw *PerformanceWrapperReadWriteCloser) Write(b []byte) (int, error) {
	n, err := pw.rwc.Write(b)
	pw.onWrite(uint64(n))
	return n, err
}

func (pw *PerformanceWrapperReadWriteCloser) Read(b []byte) (int, error) {
	n, err := pw.rwc.Read(b)
	pw.onRead(uint64(n))
	return n, err
}

func (pw *PerformanceWrapperReadWriteCloser) Close() error {
	return pw.rwc.Close()
}
