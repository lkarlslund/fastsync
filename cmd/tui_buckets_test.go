package main

import (
	"math"
	"testing"
	"time"

	"github.com/lkarlslund/fastsync"
	"github.com/mum4k/termdash/keyboard"
	"github.com/mum4k/termdash/terminal/terminalapi"
)

func bucketSample(end, interval time.Duration, bytes uint64) stats {
	c := fastsync.NewClient()
	c.Perf.Add(fastsync.WrittenBytes, bytes)
	return stats{elapsed: end, interval: interval, performance: c.Perf.NextHistory()}
}
func TestBucketsScrollOneColumnWithoutRedistribution(t *testing.T) {
	h := historyChart{}
	for i := 1; i <= 5; i++ {
		h.add(bucketSample(time.Duration(i)*time.Second, time.Second, uint64(i*10)))
	}
	before, _ := h.columns(7, 0)
	h.add(bucketSample(6*time.Second, time.Second, 60))
	after, _ := h.columns(7, 0)
	for i := 0; i < 6; i++ {
		if before[i+1] != after[i] {
			t.Fatalf("column %d changed during scroll", i)
		}
	}
	wider, _ := h.columns(20, 0)
	for i := range after {
		if after[i] != wider[13+i] {
			t.Fatal("resize rebucketed history")
		}
	}
}
func TestBucketAveragesWeightIntervalsAndKeepGaps(t *testing.T) {
	h := historyChart{}
	h.add(bucketSample(2*time.Second, 2*time.Second, 20))
	h.add(bucketSample(5*time.Second, 3*time.Second, 60))
	values, _ := h.columns(4, 1)
	if values[3][1] != 16 {
		t.Fatalf("5s average=%v", values[3][1])
	}
	h.add(bucketSample(8*time.Second, time.Second, 40))
	values, observed := h.columns(4, 0)
	if observed[1] != 0 || observed[2] != 0 || values[3][1] != 40 {
		t.Fatal("missing intervals were filled")
	}
}
func TestJitterDoesNotShiftOldBuckets(t *testing.T) {
	h := historyChart{}
	h.add(bucketSample(1001*time.Millisecond, 1001*time.Millisecond, 1001))
	h.add(bucketSample(1999*time.Millisecond, 998*time.Millisecond, 998))
	h.add(bucketSample(3002*time.Millisecond, 1003*time.Millisecond, 1003))
	for _, b := range h.buckets[0] {
		if math.Abs(b.values[1]/b.seconds-1000) > 1e-8 {
			t.Fatal("jitter changed constant rate")
		}
	}
}
func TestScaleKeysAreSharedAndIdempotent(t *testing.T) {
	view := &chartView{}
	a, b := historyChart{view: view}, historyChart{view: view}
	for i, k := range []keyboard.Key{'1', '2', '3', '4'} {
		event := &terminalapi.Keyboard{Key: k}
		a.Keyboard(event, nil)
		b.Keyboard(event, nil)
		if a.view.selection() != i || b.view.selection() != i {
			t.Fatal("graphs disagree on resolution")
		}
	}
}
func TestHistoryRetentionIsBounded(t *testing.T) {
	h := historyChart{}
	h.add(bucketSample(100*time.Hour, 100*time.Hour, 1_000_000))
	for _, b := range h.buckets {
		if len(b) > maxChartBuckets {
			t.Fatal("unbounded history")
		}
	}
}

func TestLogToggleAndZeroSafeScale(t *testing.T) {
	view := &chartView{}
	a, b := historyChart{view: view}, historyChart{view: view, stacked: true}
	key := &terminalapi.Keyboard{Key: 'l'}
	a.Keyboard(key, nil)
	b.Keyboard(key, nil)
	if !view.logarithmic.Load() {
		t.Fatal("log toggled twice")
	}
	a.Keyboard(key, nil)
	if view.logarithmic.Load() {
		t.Fatal("did not return to linear")
	}
	scale := historyScale{maximum: 1e9, divisor: 1e6, logarithmic: true, knee: 1e3}
	last := -1.0
	for _, v := range []float64{0, 1, 1e3, 1e6, 1e9} {
		n := scale.normalize(v)
		if math.IsNaN(n) || math.IsInf(n, 0) || n < last {
			t.Fatal("invalid logarithmic mapping")
		}
		if math.Abs(scale.value(n)-v) > math.Max(1, v)*1e-10 {
			t.Fatal("axis and data disagree")
		}
		last = n
	}
	if scale.normalize(0) != 0 {
		t.Fatal("zero has no baseline")
	}
}
