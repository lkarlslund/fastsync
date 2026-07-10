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
	"github.com/mum4k/termdash/widgets/linechart"
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
	elapsed                                  time.Duration
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
		started := time.Now()
		collect := func(publish bool) {
			history := client.Perf.NextHistory()
			total = total.Add(history)
			if !publish {
				return
			}
			inodes, directories, files, stack := client.Stats()
			sample := stats{
				elapsed:        time.Since(started),
				performance:    history,
				total:          total,
				inodecache:     inodes,
				directorycache: directories,
				files:          files,
				stack:          stack,
			}
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

const maxGraphSamples = 60

type graphSeries struct {
	localRead, localWrite, receivedOverWire, processed []float64
}

func appendGraphSample(series graphSeries, sample stats, capacity int) graphSeries {
	limit := maxGraphSamples
	if capacity > 0 && capacity < limit {
		limit = capacity
	}
	appendValue := func(values []float64, value uint64) []float64 {
		values = append(values, float64(value))
		if len(values) > limit {
			values = values[len(values)-limit:]
		}
		return values
	}
	series.localRead = appendValue(series.localRead, sample.performance.Get(fastsync.ReadBytes))
	series.localWrite = appendValue(series.localWrite, sample.performance.Get(fastsync.WrittenBytes))
	series.receivedOverWire = appendValue(series.receivedOverWire, sample.performance.Get(fastsync.RecievedOverWire))
	series.processed = appendValue(series.processed, sample.performance.Get(fastsync.BytesProcessed))
	return series
}

func formatStats(sample stats) string {
	current := sample.performance
	total := sample.total
	var out strings.Builder
	fmt.Fprintf(&out, "Status       Running\n")
	fmt.Fprintf(&out, "Elapsed      %s\n\n", sample.elapsed.Round(time.Second))
	fmt.Fprintf(&out, "Wire         %s/s\n", humanize.Bytes(current.Get(fastsync.SentOverWire)+current.Get(fastsync.RecievedOverWire)))
	fmt.Fprintf(&out, "Payload      %s/s\n", humanize.Bytes(current.Get(fastsync.SentBytes)+current.Get(fastsync.RecievedBytes)))
	fmt.Fprintf(&out, "Local read   %s/s\n", humanize.Bytes(current.Get(fastsync.ReadBytes)))
	fmt.Fprintf(&out, "Local write  %s/s\n", humanize.Bytes(current.Get(fastsync.WrittenBytes)))
	fmt.Fprintf(&out, "Processed    %s/s\n", humanize.Bytes(current.Get(fastsync.BytesProcessed)))
	fmt.Fprintf(&out, "Files        %d/s\n", current.Get(fastsync.FilesProcessed))
	fmt.Fprintf(&out, "Directories  %d/s\n\n", current.Get(fastsync.DirectoriesProcessed))
	fmt.Fprintf(&out, "Transferred  %s\n", humanize.Bytes(total.Get(fastsync.SentBytes)+total.Get(fastsync.RecievedBytes)))
	fmt.Fprintf(&out, "Files total  %d\n", total.Get(fastsync.FilesProcessed))
	fmt.Fprintf(&out, "Dirs total   %d\n\n", total.Get(fastsync.DirectoriesProcessed))
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
		{label: "[] Wire receive", color: cell.ColorYellow},
		{label: "[] Processed", color: cell.ColorMagenta},
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

	chart, err := linechart.New(
		linechart.YAxisFormattedValues(func(value float64) string {
			return humanize.Bytes(uint64(value)) + "/s"
		}),
		linechart.XAxisUnscaled(),
	)
	if err != nil {
		return fmt.Errorf("create throughput chart: %w", err)
	}
	statsView, err := text.New(text.WrapAtWords())
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
		container.SplitHorizontal(
			container.Top(container.SplitVertical(
				container.Left(container.Border(linestyle.Light), container.BorderTitle("Throughput"), container.PlaceWidget(chart)),
				container.Right(container.Border(linestyle.Light), container.BorderTitle("Statistics"), container.PlaceWidget(statsView)),
				container.SplitPercent(75),
			)),
			container.Bottom(container.Border(linestyle.Light), container.BorderTitle("Log"), container.PlaceWidget(logView)),
			container.SplitPercent(65),
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
		var series graphSeries
		for sample := range statsCh {
			if err := writeStats(statsView, sample); err != nil {
				fastsync.Logger.Error().Msgf("Update dashboard statistics: %v", err)
			}
			series = appendGraphSample(series, sample, chart.ValueCapacity())
			updates := []struct {
				name   string
				values []float64
				color  cell.Color
			}{
				{name: "Local read", values: series.localRead, color: cell.ColorGreen},
				{name: "Local write", values: series.localWrite, color: cell.ColorBlue},
				{name: "Wire receive", values: series.receivedOverWire, color: cell.ColorYellow},
				{name: "Processed", values: series.processed, color: cell.ColorMagenta},
			}
			for _, update := range updates {
				if err := chart.Series(update.name, update.values, linechart.SeriesCellOpts(cell.FgColor(update.color))); err != nil {
					fastsync.Logger.Error().Msgf("Update dashboard graph: %v", err)
				}
			}
		}
		cancel()
	}()

	if err := termdash.Run(ctx, terminal, root, termdash.RedrawInterval(250*time.Millisecond)); err != nil {
		return fmt.Errorf("run dashboard: %w", err)
	}
	return nil
}
