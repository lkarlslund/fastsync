package main

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lkarlslund/fastsync"
	"github.com/mum4k/termdash/widgets/text"
	"github.com/rs/zerolog"
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

func TestDashboardLogWriterAcceptsConcurrentEvents(t *testing.T) {
	view, err := text.New(text.RollContent(), text.MaxTextCells(4096))
	if err != nil {
		t.Fatalf("new text widget: %v", err)
	}
	writer := &dashboardLogWriter{
		view:      view,
		formatter: zerolog.ConsoleWriter{NoColor: true},
	}
	logger := zerolog.New(writer)

	var workers sync.WaitGroup
	for i := range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			logger.Info().Int("worker", i).Msg(fmt.Sprintf("event %d", i))
		}()
	}
	workers.Wait()
}

func TestTerminalMode(t *testing.T) {
	tests := []struct {
		name                string
		stdinTTY, stdoutTTY bool
		term                string
		want                bool
	}{
		{name: "interactive", stdinTTY: true, stdoutTTY: true, term: "xterm-256color", want: true},
		{name: "redirected input", stdoutTTY: true, term: "xterm-256color"},
		{name: "redirected output", stdinTTY: true, term: "xterm-256color"},
		{name: "dumb terminal", stdinTTY: true, stdoutTTY: true, term: "dumb"},
		{name: "missing terminal", stdinTTY: true, stdoutTTY: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := terminalMode(tt.stdinTTY, tt.stdoutTTY, tt.term); got != tt.want {
				t.Fatalf("terminalMode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFormatStatsIncludesQueuesAndTotals(t *testing.T) {
	var current, total fastsync.PerformanceEntry
	// Build entries through the public performance API because counters are intentionally opaque.
	client := fastsync.NewClient()
	client.Perf.Add(fastsync.WrittenBytes, 1024)
	client.Perf.Add(fastsync.FilesProcessed, 2)
	current = client.Perf.NextHistory()
	total = total.Add(current)
	got := formatStats(stats{
		performance: current, total: total, elapsed: 2 * time.Second,
		inodecache: 3, directorycache: 4, files: 5, stack: 6,
	})
	for _, want := range []string{"Local write  1.0 kB/s", "Files total  2", "File queue   5", "Dir stack    6", "Inode cache  3", "Dir cache    4"} {
		if !strings.Contains(got, want) {
			t.Fatalf("formatStats() missing %q:\n%s", want, got)
		}
	}
}
