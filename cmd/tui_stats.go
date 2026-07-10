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
	if err := w.view.Write(formatted.String(), text.WriteCellOpts(cell.FgColor(color))); err != nil {
		return 0, err
	}
	return len(p), nil
}

type stats struct {
	startTime                                time.Time
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
				startTime:      time.Now(),
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
			return fmt.Sprintf("%.0f", value)
		}),
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
		series := map[string][]float64{
			"read":      {},
			"written":   {},
			"wire":      {},
			"processed": {},
		}
		colors := map[string]cell.Color{
			"read": cell.ColorGreen, "written": cell.ColorBlue,
			"wire": cell.ColorYellow, "processed": cell.ColorMagenta,
		}
		for sample := range statsCh {
			statsView.Reset()
			_ = statsView.Write(formatStats(sample))
			values := map[string]uint64{
				"read": sample.performance.Get(fastsync.ReadBytes), "written": sample.performance.Get(fastsync.WrittenBytes),
				"wire": sample.performance.Get(fastsync.RecievedOverWire), "processed": sample.performance.Get(fastsync.BytesProcessed),
			}
			for name, value := range values {
				series[name] = append(series[name], float64(value))
				if len(series[name]) > 60 {
					series[name] = series[name][len(series[name])-60:]
				}
				_ = chart.Series(name, series[name], linechart.SeriesCellOpts(cell.FgColor(colors[name])))
			}
		}
		cancel()
	}()

	if err := termdash.Run(ctx, terminal, root, termdash.RedrawInterval(250*time.Millisecond)); err != nil {
		return fmt.Errorf("run dashboard: %w", err)
	}
	return nil
}
