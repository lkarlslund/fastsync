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
