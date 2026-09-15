package main

import (
	"github.com/lkarlslund/fastsync"
	"math"
	"strings"
	"testing"
	"time"
)

func TestRollingRatesWeightIntervalsAndExpire(t *testing.T) {
	c := fastsync.NewClient()
	var h rateHistory
	var total fastsync.PerformanceEntry
	sample := func(end, interval time.Duration, n uint64) metricRates {
		c.Perf.Add(fastsync.WrittenBytes, n)
		p := c.Perf.NextHistory()
		total = total.Add(p)
		return h.collect(stats{elapsed: end, interval: interval, performance: p, total: total})[3]
	}
	r := sample(30*time.Second, 30*time.Second, 300)
	if r.instant != 10 || r.minute != 10 || r.fiveMinute != 10 {
		t.Fatalf("startup %+v", r)
	}
	r = sample(90*time.Second, 60*time.Second, 1200)
	// Last minute contains only the new interval; startup 5m uses 90s.
	if r.minute != 20 || math.Abs(r.fiveMinute-1500.0/90) > 1e-9 || r.total != 1500 {
		t.Fatalf("weighted %+v", r)
	}
	r = sample(120*time.Second, 30*time.Second, 0)
	if r.instant != 0 || r.minute != 10 || r.fiveMinute != 12.5 {
		t.Fatalf("idle %+v", r)
	}
	r = sample(420*time.Second, 300*time.Second, 0)
	if r.minute != 0 || r.fiveMinute != 0 || len(h.samples) != 1 {
		t.Fatalf("expired %+v", r)
	}
}
func TestFlushRatesUseCompletedByteCounter(t *testing.T) {
	var h rateHistory
	h.collect(stats{elapsed: time.Second, interval: time.Second, tuning: fastsync.TransferTuning{FlushedBytes: 100}})
	r := h.collect(stats{elapsed: 3 * time.Second, interval: 2 * time.Second, tuning: fastsync.TransferTuning{FlushedBytes: 160}})[4]
	if r.instant != 30 || r.total != 160 {
		t.Fatalf("flush %+v", r)
	}
	if s := formatStats(stats{}); strings.Contains(s, "Completed*") {
		t.Fatal(s)
	}
}
