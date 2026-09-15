package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
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
	logWriter zerolog.LevelWriter
	err       error
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
	var out strings.Builder
	phase := "STARTING"
	if sample.tuning.Phase == 1 {
		phase = "WARMING EXISTING FILES"
	} else if sample.tuning.Phase == 2 {
		phase = "COPYING / LINKING"
	}
	switch sample.tuning.Phase {
	case 3:
		phase = "LOADING INODE CACHE"
	case 4:
		phase = "FLUSHING DATA"
	case 5:
		phase = "SAVING RESUME CACHE"
	case 6:
		phase = "SHUTTING DOWN"
	}
	if sample.tuning.Phase == 3 {
		fmt.Fprintf(&out, "Validated hints %s\nLoaded inodes   %s\n", humanize.Comma(int64(sample.total.Get(fastsync.ExistingExamined))), humanize.Comma(int64(sample.inodecache)))
	}
	fmt.Fprintf(&out, "● %s  ·  %s\n", phase, sample.elapsed.Round(time.Second))
	label := sample.bottleneck.Label
	if label == "" {
		label = "Unknown"
	}
	fmt.Fprintf(&out, "Bottleneck   %s\n\n", label)
	fmt.Fprintf(&out, "Processed    %s  (all paths)\nUnique data  %s  (once per inode)\n\n", humanize.Bytes(sample.total.Get(fastsync.BytesProcessed)), humanize.Bytes(sample.total.Get(fastsync.BytesUniqueProcessed)))
	if n := sample.total.Get(fastsync.ResumeSubtreesSkipped); n > 0 {
		fmt.Fprintf(&out, "Skipped trees %d (prior checkpoints)\n", n)
	}
	out.WriteString(formatRateTable(sample))
	fmt.Fprintln(&out, "Graphs  1:1s  2:5s  3:15s  4:1m  L:log")
	fmt.Fprintln(&out, "\nRESOURCES")
	t := sample.tuning
	if t.FileLimit > 0 {
		fmt.Fprintf(&out, "Buffers      %s %s / %s\n", usageMeter(t.BufferReserved, t.BufferLimit), humanize.Bytes(uint64(t.BufferReserved)), humanize.Bytes(uint64(t.BufferLimit)))
		fmt.Fprintf(&out, "Streams      %s %d / %d\n", usageMeter(int64(t.ActiveFiles), int64(t.FileLimit)), t.ActiveFiles, t.FileLimit)
		fmt.Fprintf(&out, "Read / write %d / %d  ·  %d writing\n", t.ReadLimit, t.WriteLimit, t.ActiveWrites)
	}
	if t.FlushCount > 0 || t.PendingFlushFiles > 0 {
		fmt.Fprintf(&out, "Flush queue  %d files · %s\nLast flush   %s\n", t.PendingFlushFiles, humanize.Bytes(t.PendingFlushBytes), t.LastFlush.Round(time.Millisecond))
	}
	fmt.Fprintf(&out, "Check/copy/link %d/%d/%d\nDependencies %d\n", t.CheckQueue, t.CopyQueue, t.LinkQueue, t.Dependencies)
	if n := sample.performance.Get(fastsync.QueueDispatches); n > 0 {
		fmt.Fprintf(&out, "Queue wait   %s avg\n", (time.Duration(sample.performance.Get(fastsync.QueueWaitNanos) / n)).Round(time.Millisecond))
	}
	fmt.Fprintf(&out, "File queue   %d   ·   Dir stack    %d\n", sample.files, sample.stack)
	fmt.Fprintf(&out, "Inode cache  %d   ·   Dir cache    %d\n", sample.inodecache, sample.directorycache)
	fmt.Fprintf(&out, "Reuse groups %d\n", sample.total.Get(fastsync.ReuseGroups))
	return out.String()
}

func writeStats(view *text.Text, sample stats) error {
	view.Reset()
	for _, line := range strings.Split(strings.TrimSuffix(formatStats(sample), "\n"), "\n") {
		color := dashboardInk
		fields := strings.Fields(line)
		if len(fields) > 0 {
			color = metricColor(fields[0])
		}
		if strings.HasPrefix(line, "●") {
			color = dashboardSame
		}
		if strings.HasPrefix(line, "Bottleneck") {
			color = dashboardWire
		}
		if line == "RESOURCES" || strings.HasPrefix(line, "/s") {
			color = dashboardMuted
		}
		if err := view.Write(line+"\n", text.WriteCellOpts(cell.FgColor(color))); err != nil {
			return err
		}
	}
	return nil
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

	view := &chartView{}
	chart := &historyChart{view: view}
	activity := &historyChart{stacked: true, view: view}
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

	root, err := dashboardLayout(terminal, chart, activity, statsView, logView)
	if err != nil {
		return fmt.Errorf("create dashboard layout: %w", err)
	}
	ready <- dashboardReady{logWriter: &dashboardLogWriter{
		view: logView,
		formatter: zerolog.ConsoleWriter{
			TimeFormat: time.RFC3339,
			NoColor:    true,
		},
	}}

	ctx, cancel := context.WithCancel(context.Background())
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

func dashboardLayout(terminal terminalapi.Terminal, chart, activity *historyChart, statsView, logView *text.Text) (*container.Container, error) {
	return container.New(terminal,
		container.SplitVertical(
			container.Left(container.SplitHorizontal(
				container.Top(container.SplitVertical(
					container.Left(container.Border(linestyle.Light), container.BorderColor(dashboardBorder), container.BorderTitle(" THROUGHPUT "), container.PlaceWidget(chart)),
					container.Right(container.Border(linestyle.Light), container.BorderColor(dashboardBorder), container.BorderTitle(" FILE ACTIVITY "), container.PlaceWidget(activity)),
					container.SplitPercent(50),
				)),
				container.Bottom(container.Border(linestyle.Light), container.BorderColor(dashboardBorder), container.BorderTitle(" EVENTS "), container.PlaceWidget(logView)),
				container.SplitPercent(60),
			)),
			container.Right(container.Border(linestyle.Light), container.BorderColor(dashboardBorder), container.BorderTitle(" FASTSYNC "), container.PlaceWidget(statsView)),
			container.SplitFixedFromEnd(52),
		),
	)
}
