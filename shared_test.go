package fastsync

import (
	"bytes"
	"io"
	"net"
	"sort"
	"sync"
	"testing"
)

func TestCompressedReadWriteCloserRoundTrip(t *testing.T) {
	left, right := net.Pipe()
	leftCompressed := CompressedReadWriteCloser(left)
	rightCompressed := CompressedReadWriteCloser(right)

	want := []byte("compressed payload")
	errCh := make(chan error, 1)
	go func() {
		_, err := leftCompressed.Write(want)
		errCh <- err
	}()

	got := make([]byte, len(want))
	if _, err := io.ReadFull(rightCompressed, got); err != nil {
		t.Fatalf("read compressed payload: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("write compressed payload: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("payload = %q, want %q", got, want)
	}
	if err := leftCompressed.Close(); err != nil {
		t.Fatalf("close left compressed conn: %v", err)
	}
	if err := rightCompressed.Close(); err != nil {
		t.Fatalf("close right compressed conn: %v", err)
	}
}

func TestPerformanceHistoryDoesNotLoseConcurrentAdds(t *testing.T) {
	p := NewPerformance()
	const goroutines = 8
	const additions = 1000

	var workers sync.WaitGroup
	workers.Add(goroutines)
	for range goroutines {
		go func() {
			defer workers.Done()
			for range additions {
				p.Add(WrittenBytes, 1)
			}
		}()
	}

	var total PerformanceEntry
	done := make(chan struct{})
	go func() {
		workers.Wait()
		close(done)
	}()
	for {
		total = total.Add(p.NextHistory())
		select {
		case <-done:
			total = total.Add(p.NextHistory())
			if got, want := total.Get(WrittenBytes), uint64(goroutines*additions); got != want {
				t.Fatalf("collected WrittenBytes = %d, want %d", got, want)
			}
			return
		default:
		}
	}
}

func TestPerformanceHistoryBound(t *testing.T) {
	p := NewPerformance()
	p.maxhistory = 2

	for i := 0; i < 5; i++ {
		p.Add(FilesProcessed, uint64(i+1))
		p.NextHistory()
	}

	if got, want := len(p.entries), p.maxhistory; got != want {
		t.Fatalf("history length = %d, want %d", got, want)
	}
}

func TestPerformanceCounters(t *testing.T) {
	p := NewPerformance()
	p.Add(FilesProcessed, 2)
	p.GetAtomicAdder(FilesProcessed)(3)
	p.Add(WrittenBytes, 7)

	if got, want := p.Get(FilesProcessed), uint64(5); got != want {
		t.Fatalf("FilesProcessed = %d, want %d", got, want)
	}
	if got, want := p.Get(WrittenBytes), uint64(7); got != want {
		t.Fatalf("WrittenBytes = %d, want %d", got, want)
	}

	history := p.NextHistory()
	if got, want := history.Get(FilesProcessed), uint64(5); got != want {
		t.Fatalf("history FilesProcessed = %d, want %d", got, want)
	}
	if got := p.Get(FilesProcessed); got != 0 {
		t.Fatalf("current FilesProcessed after NextHistory = %d, want 0", got)
	}

	total := history.Add(PerformanceEntry{})
	if got, want := total.Get(WrittenBytes), uint64(7); got != want {
		t.Fatalf("added WrittenBytes = %d, want %d", got, want)
	}
}

func TestStackEmitsSubmittedItemsAndCloses(t *testing.T) {
	stack, out, in := NewStack[int](8, 2)
	for i := 0; i < 10; i++ {
		in <- i
	}
	if got := stack.Len(); got == 0 {
		t.Fatal("stack length = 0 while items are queued, want non-zero")
	}
	stack.Close()

	var got []int
	for item := range out {
		got = append(got, item)
	}
	sort.Ints(got)

	if len(got) != 10 {
		t.Fatalf("got %d items, want 10: %v", len(got), got)
	}
	for i, item := range got {
		if item != i {
			t.Fatalf("sorted item %d = %d, want %d; all items %v", i, item, i, got)
		}
	}
}
