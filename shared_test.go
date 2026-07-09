package fastsync

import "testing"

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
