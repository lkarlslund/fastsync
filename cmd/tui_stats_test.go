package main

import (
	"testing"
	"time"

	"github.com/lkarlslund/fastsync"
)

func TestStatsCollectorDrainsFinalCounters(t *testing.T) {
	client := fastsync.NewClient()
	collector := startStatsCollector(client, time.Hour)
	client.Perf.Add(fastsync.WrittenBytes, 42)

	total := collector.Stop()
	if got, want := total.Get(fastsync.WrittenBytes), uint64(42); got != want {
		t.Fatalf("total written bytes = %d, want %d", got, want)
	}
	if _, ok := <-collector.samples; ok {
		t.Fatal("samples channel remains open after collector stop")
	}
}
