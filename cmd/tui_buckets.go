package main

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/lkarlslund/fastsync"
	"github.com/mum4k/termdash/terminal/terminalapi"
)

var chartIntervals = [...]time.Duration{time.Second, 5 * time.Second, 15 * time.Second, time.Minute}

const maxChartBuckets = 2048

type chartView struct {
	selected    atomic.Int32
	logarithmic atomic.Bool
}

func (v *chartView) selection() int {
	if v == nil {
		return 0
	}
	return int(v.selected.Load())
}
func (v *chartView) key(k *terminalapi.Keyboard) {
	if v == nil || k == nil {
		return
	}
	switch k.Key {
	case '1':
		v.selected.Store(0)
	case '2':
		v.selected.Store(1)
	case '3':
		v.selected.Store(2)
	case '4':
		v.selected.Store(3)
	}
}

type historyBucket struct {
	index   int64
	seconds float64
	values  [4]float64 // Integrated counts, divided by observed seconds at display time.
}

func (h *historyChart) add(s stats) {
	h.mu.Lock()
	defer h.mu.Unlock()
	interval := s.interval
	if interval <= 0 {
		interval = time.Second
	}
	end := s.elapsed
	if end <= h.latest {
		return
	}
	start := max(time.Duration(0), end-interval)
	if end <= start {
		return
	}
	counters := []fastsync.PerformanceCounterType{fastsync.ReadBytes, fastsync.WrittenBytes, fastsync.SentOverWire, fastsync.RecievedOverWire}
	if h.stacked {
		counters = []fastsync.PerformanceCounterType{fastsync.DirectoriesProcessed, fastsync.FilesLinked, fastsync.FilesUnchanged, fastsync.FilesCopied}
	}
	h.latest = end
	for resolution, width := range chartIntervals {
		last := int64((end - 1) / width)
		first := max(int64(start/width), last-maxChartBuckets+1)
		buckets := h.buckets[resolution]
		for index := first; index <= last; index++ {
			overlap := min(end, time.Duration(index+1)*width) - max(start, time.Duration(index)*width)
			if overlap <= 0 {
				continue
			}
			if len(buckets) == 0 || buckets[len(buckets)-1].index != index {
				buckets = append(buckets, historyBucket{index: index})
			}
			b := &buckets[len(buckets)-1]
			b.seconds += overlap.Seconds()
			for i, k := range counters {
				b.values[i] += sampleRate(s, k) * overlap.Seconds()
			}
		}
		keep := 0
		for keep < len(buckets) && buckets[keep].index <= last-maxChartBuckets {
			keep++
		}
		h.buckets[resolution] = buckets[keep:]
	}
}

// One fixed time bucket per terminal cell, anchored to transfer start. Neither
// viewport width nor a delayed sample redistributes already collected history.
// Caller holds h.mu.
func (h *historyChart) columns(width int, resolution int) ([][4]float64, []float64) {
	values := make([][4]float64, width)
	observed := make([]float64, width)
	newest := int64((h.latest - 1) / chartIntervals[resolution])
	for _, b := range h.buckets[resolution] {
		x := int(b.index-newest) + width - 1
		if x < 0 || x >= width || b.seconds == 0 {
			continue
		}
		observed[x] = b.seconds
		for i, v := range b.values {
			values[x][i] = v / b.seconds
		}
	}
	return values, observed
}
func chartSpan(width int, step time.Duration) string {
	duration := time.Duration(width) * step
	if duration%time.Hour == 0 {
		return fmt.Sprintf("-%dh", duration/time.Hour)
	}
	if duration%time.Minute == 0 {
		return fmt.Sprintf("-%dm", duration/time.Minute)
	}
	return "-" + duration.String()
}
