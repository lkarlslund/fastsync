package main

import (
	"github.com/lkarlslund/fastsync"
	"github.com/mum4k/termdash/private/canvas"
	"image"
	"testing"
	"time"
)

func TestHistoryRightEdgeAndStack(t *testing.T) {
	client := fastsync.NewClient()
	for _, k := range []fastsync.PerformanceCounterType{fastsync.DirectoriesProcessed, fastsync.FilesLinked, fastsync.FilesUnchanged, fastsync.FilesCopied, fastsync.WrittenBytes} {
		client.Perf.Add(k, 20)
	}
	s := stats{elapsed: time.Second, interval: 2 * time.Second, performance: client.Perf.NextHistory()}
	if got := sampleRate(s, fastsync.WrittenBytes); got != 10 {
		t.Fatalf("rate %v", got)
	}
	for _, stacked := range []bool{false, true} {
		for _, width := range []int{12, 40, 60, 120} {
			h := historyChart{stacked: stacked}
			h.add(s)
			c, err := canvas.New(image.Rect(0, 0, width, 10))
			if err != nil {
				t.Fatal(err)
			}
			if err := h.Draw(c, nil); err != nil {
				t.Fatal(err)
			}
			occupied := 0
			for y := 1; y < 9; y++ {
				v, err := c.Cell(image.Pt(width-1, y))
				if err != nil {
					t.Fatal(err)
				}
				if v.Rune == '█' || v.Rune == '•' {
					occupied++
				}
			}
			if occupied == 0 {
				t.Fatalf("blank newest column: width=%d stacked=%v", width, stacked)
			}
			if stacked && occupied != 8 {
				t.Fatalf("stack height=%d want 8", occupied)
			}
		}
	}
}
func TestHistoryExpiresByTime(t *testing.T) {
	h := historyChart{}
	h.add(stats{elapsed: time.Second})
	h.add(stats{elapsed: 62 * time.Second})
	if len(h.samples) != 1 {
		t.Fatal("aged sample retained")
	}
}
