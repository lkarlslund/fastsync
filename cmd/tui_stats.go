package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"

	"github.com/dustin/go-humanize"
	"github.com/lkarlslund/fastsync"
	"github.com/mum4k/termdash"
	"github.com/mum4k/termdash/cell"
	"github.com/mum4k/termdash/container"
	"github.com/mum4k/termdash/linestyle"
	"github.com/mum4k/termdash/terminal/tcell"
	"github.com/mum4k/termdash/terminal/terminalapi"

	"github.com/mum4k/termdash/widgets/text"
	"github.com/rs/zerolog"
)

type dashboardReady struct {
	logWriter   zerolog.LevelWriter
	interrupted <-chan struct{}
	err         error
}

type dashboardLogWriter struct {
	mu        sync.Mutex
	view      *text.Text
	formatter zerolog.ConsoleWriter
}

func (w *dashboardLogWriter) Write(p []byte) (int, error) {
	return w.WriteLevel(zerolog.NoLevel, p)
}

func (w *dashboardLogWriter) WriteLevel(level zerolog.Level, p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var formatted bytes.Buffer
	formatter := w.formatter
	formatter.Out = &formatted
	if _, err := formatter.Write(p); err != nil {
		return 0, err
	}
	color := cell.ColorWhite
	switch level {
	case zerolog.TraceLevel, zerolog.DebugLevel:
		color = cell.ColorNumber(245)
	case zerolog.WarnLevel:
		color = cell.ColorYellow
	case zerolog.ErrorLevel, zerolog.FatalLevel, zerolog.PanicLevel:
		color = cell.ColorRed
	}
	if err := w.view.Write(sanitizeDashboardText(formatted.String()), text.WriteCellOpts(cell.FgColor(color))); err != nil {
		return 0, err
	}
	return len(p), nil
}

func sanitizeDashboardText(value string) string {
	var sanitized strings.Builder
	sanitized.Grow(len(value))
	for _, r := range value {
		switch {
		case r == '\n' || r == ' ':
			sanitized.WriteRune(r)
		case r == '\t':
			sanitized.WriteString("    ")
		case unicode.IsControl(r):
			fmt.Fprintf(&sanitized, "\\u%04x", r)
		case unicode.IsSpace(r):
			sanitized.WriteByte(' ')
		default:
			sanitized.WriteRune(r)
		}
	}
	return sanitized.String()
}

type stats struct {
	rates                                    []metricRates
	tuning                                   fastsync.TransferTuning
	bottleneck                               fastsync.BottleneckStatus
	elapsed                                  time.Duration
	interval                                 time.Duration
	performance                              fastsync.PerformanceEntry
	total                                    fastsync.PerformanceEntry
	inodecache, directorycache, files, stack int
}

type statsCollector struct {
	stop    chan struct{}
	done    chan fastsync.PerformanceEntry
	samples chan stats
}

func startStatsCollector(client *fastsync.Client, interval time.Duration) *statsCollector {
	collector := &statsCollector{
		stop:    make(chan struct{}),
		done:    make(chan fastsync.PerformanceEntry, 1),
		samples: make(chan stats, 10),
	}
	go func() {
		defer close(collector.samples)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		var total fastsync.PerformanceEntry
		var rolling rateHistory
		started := time.Now()
		previous := started
		collect := func(publish bool) {
			now := time.Now()
			interval := now.Sub(previous)
			previous = now
			history := client.Perf.NextHistory()
			total = total.Add(history)
			if !publish {
				return
			}
			inodes, directories, files, stack := client.Stats()
			sample := stats{
				elapsed:        now.Sub(started),
				interval:       interval,
				bottleneck:     client.Diagnostics(),
				tuning:         client.Tuning(),
				performance:    history,
				total:          total,
				inodecache:     inodes,
				directorycache: directories,
				files:          files,
				stack:          stack,
			}
			sample.rates = rolling.collect(sample)
			select {
			case collector.samples <- sample:
			default:
			}
		}
		for {
			select {
			case <-ticker.C:
				collect(true)
			case <-collector.stop:
				collect(false)
				collector.done <- total
				return
			}
		}
	}()
	return collector
}

func formatStats(sample stats) string {
	current := sample.performance
	total := sample.total
	var out strings.Builder
	fmt.Fprintf(&out, "Status       Running\n")
	if sample.tuning.Phase == 1 {
		fmt.Fprintln(&out, "Pass         Existing files")
	} else if sample.tuning.Phase == 2 {
		fmt.Fprintln(&out, "Pass         Link/copy")
	}
	fmt.Fprintf(&out, "Reuse groups %d\n", total.Get(fastsync.ReuseGroups))
	label := sample.bottleneck.Label
	if label == "" {
		label = "Unknown"
	}
	fmt.Fprintf(&out, "Bottleneck   %s\n", label)
	if sample.tuning.FileLimit > 0 {
		fmt.Fprintf(&out, "Read limit   %d\nWrite limit  %d (%d active)\nStream files %d/%d\nBuffer bound %s/%s\n", sample.tuning.ReadLimit, sample.tuning.WriteLimit, sample.tuning.ActiveWrites, sample.tuning.ActiveFiles, sample.tuning.FileLimit, humanize.Bytes(uint64(sample.tuning.BufferReserved)), humanize.Bytes(uint64(sample.tuning.BufferLimit)))
	}
	if sample.tuning.FlushCount > 0 || sample.tuning.PendingFlushFiles > 0 {
		fmt.Fprintf(&out, "Flush queue  %d files / %s\nFile flush   %s\n", sample.tuning.PendingFlushFiles, humanize.Bytes(sample.tuning.PendingFlushBytes), sample.tuning.LastFlush.Round(time.Millisecond))
	}

	fmt.Fprintf(&out, "Elapsed      %s\n\n", sample.elapsed.Round(time.Second))
	out.WriteString(formatRateTable(sample))
	fmt.Fprintln(&out)
	fmt.Fprintf(&out, "Check/copy/link %d/%d/%d\nDependencies %d\n", sample.tuning.CheckQueue, sample.tuning.CopyQueue, sample.tuning.LinkQueue, sample.tuning.Dependencies)
	if n := current.Get(fastsync.QueueDispatches); n > 0 {
		fmt.Fprintf(&out, "Queue wait   %s avg\n", (time.Duration(current.Get(fastsync.QueueWaitNanos) / n)).Round(time.Millisecond))
	}
	fmt.Fprintf(&out, "File queue   %d\n", sample.files)
	fmt.Fprintf(&out, "Dir stack    %d\n", sample.stack)
	fmt.Fprintf(&out, "Inode cache  %d\n", sample.inodecache)
	fmt.Fprintf(&out, "Dir cache    %d\n", sample.directorycache)
	return out.String()
}

func writeStats(view *text.Text, sample stats) error {
	view.Reset()
	legend := []struct {
		label string
		color cell.Color
	}{
		{label: "[] Local read", color: cell.ColorGreen},
		{label: "[] Local write", color: cell.ColorBlue},
		{label: "[] Wire", color: cell.ColorYellow},
		{label: "[] Dir", color: cell.ColorCyan},
		{label: "[] Link", color: cell.ColorYellow},
		{label: "[] Same", color: cell.ColorGreen},
		{label: "[] Copied", color: cell.ColorMagenta},
	}
	for _, item := range legend {
		if err := view.Write(item.label+"\n", text.WriteCellOpts(cell.FgColor(item.color))); err != nil {
			return err
		}
	}
	return view.Write("\n" + formatStats(sample))
}

func (c *statsCollector) Stop() fastsync.PerformanceEntry {
	close(c.stop)
	return <-c.done
}

// showStatsTUI displays transfer activity until statsCh is closed.
func showStatsTUI(statsCh <-chan stats, ready chan<- dashboardReady) (retErr error) {
	defer func() {
		if retErr != nil {
			select {
			case ready <- dashboardReady{err: retErr}:
			default:
			}
		}
	}()
	terminal, err := tcell.New(tcell.ColorMode(terminalapi.ColorMode256))
	if err != nil {
		return fmt.Errorf("open terminal: %w", err)
	}
	defer terminal.Close()

	chart := &historyChart{}
	activity := &historyChart{stacked: true}
	statsView, err := text.New()
	if err != nil {
		return fmt.Errorf("create statistics view: %w", err)
	}
	if err := statsView.Write("Waiting for statistics..."); err != nil {
		return fmt.Errorf("initialize statistics view: %w", err)
	}
	logView, err := text.New(text.RollContent(), text.WrapAtWords(), text.MaxTextCells(64*1024))
	if err != nil {
		return fmt.Errorf("create log view: %w", err)
	}

	root, err := container.New(terminal,
		container.SplitVertical(
			container.Left(container.SplitHorizontal(
				container.Top(container.SplitVertical(
					container.Left(container.Border(linestyle.Light), container.BorderTitle("Throughput (bytes/s)"), container.PlaceWidget(chart)),
					container.Right(container.Border(linestyle.Light), container.BorderTitle("Activity (entries/s)"), container.PlaceWidget(activity)),
					container.SplitPercent(50),
				)),
				container.Bottom(container.Border(linestyle.Light), container.BorderTitle("Log"), container.PlaceWidget(logView)),
				container.SplitPercent(75),
			)),
			container.Right(container.Border(linestyle.Light), container.BorderTitle("Statistics"), container.PlaceWidget(statsView)),
			container.SplitPercent(60),
		),
	)
	if err != nil {
		return fmt.Errorf("create dashboard layout: %w", err)
	}
	signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	ready <- dashboardReady{logWriter: &dashboardLogWriter{
		view: logView,
		formatter: zerolog.ConsoleWriter{
			TimeFormat: time.RFC3339,
			NoColor:    true,
		},
	}, interrupted: signalCtx.Done()}

	ctx, cancel := context.WithCancel(signalCtx)
	defer cancel()
	go func() {
		for sample := range statsCh {
			if err := writeStats(statsView, sample); err != nil {
				fastsync.Logger.Error().Msgf("Update dashboard statistics: %v", err)
			}
			chart.add(sample)
			activity.add(sample)
		}
		cancel()
	}()

	if err := termdash.Run(ctx, terminal, root, termdash.RedrawInterval(250*time.Millisecond)); err != nil {
		return fmt.Errorf("run dashboard: %w", err)
	}
	return nil
}
