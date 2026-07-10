package main

import (
	"fmt"
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
