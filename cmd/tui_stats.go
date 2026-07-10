package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/NimbleMarkets/ntcharts/linechart/timeserieslinechart"
	"github.com/charmbracelet/lipgloss"
	"github.com/dustin/go-humanize"
	"github.com/lkarlslund/fastsync"
	"golang.org/x/term"
)

type stats struct {
	startTime                                time.Time
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
		collect := func(publish bool) {
			history := client.Perf.NextHistory()
			total = total.Add(history)
			if !publish {
				return
			}
			inodes, directories, files, stack := client.Stats()
			collector.samples <- stats{
				startTime:      time.Now(),
				performance:    history,
				total:          total,
				inodecache:     inodes,
				directorycache: directories,
				files:          files,
				stack:          stack,
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

func (c *statsCollector) Stop() fastsync.PerformanceEntry {
	close(c.stop)
	return <-c.done
}

// ShowStatsTUI displays a TUI timeseries chart of byte values from the provided channel.
func showStatsTUI(statsCh <-chan stats) {
	getTermSize := func() (int, int) {
		w, h, err := term.GetSize(int(os.Stdout.Fd()))
		if err != nil {
			return 80, 24 // fallback
		}
		return w, h
	}

	width, height := getTermSize()
	chartHeight := height / 2

	tslc := timeserieslinechart.New(width, chartHeight)
	tslc.XLabelFormatter = timeserieslinechart.HourTimeLabelFormatter()
	tslc.UpdateHandler = timeserieslinechart.SecondUpdateHandler(1)

	tslc.YLabelFormatter = func(i int, v float64) string {
		return humanize.Bytes(uint64(v)) + "/s"
	}

	tslc.SetDataSetStyle("localRead",
		lipgloss.NewStyle().
			Foreground(lipgloss.Color("10")), // green
	)
	tslc.SetDataSetStyle("localWritten",
		lipgloss.NewStyle().
			Foreground(lipgloss.Color("12")), // blue
	)
	tslc.SetDataSetStyle("recievedOverWire",
		lipgloss.NewStyle().
			Foreground(lipgloss.Color("11")), // yellow
	)
	tslc.SetDataSetStyle("processed",
		lipgloss.NewStyle().
			Foreground(lipgloss.Color("13")), // magenta
	)

	tslc.DrawXYAxisAndLabel()

	resizeCh := make(chan os.Signal, 1)
	signal.Notify(resizeCh, syscall.SIGWINCH)

	clearAndDraw := func(view string) {
		// Move cursor to top-left and clear only the chart area
		_, _ = os.Stdout.Write([]byte("\033[H"))
		for i := 0; i < chartHeight+2; i++ {
			_, _ = os.Stdout.Write([]byte("\033[2K\r")) // clear line
		}
		_, _ = os.Stdout.Write([]byte("\033[H"))
		_, _ = os.Stdout.Write([]byte(view))
	}

	for {
		select {
		case <-resizeCh:
			w, h := getTermSize()
			chartHeight = h / 2
			tslc.Resize(w, chartHeight)
		case curStat, ok := <-statsCh:
			if !ok {
				return
			}

			lasthistory := curStat.performance
			fastsync.Logger.Warn().Msgf("Wired %v/sec, transferred %v/sec, local read %v/sec, local write %v/sec, processed %v/sec, %v files/sec, %v dirs/sec",
				humanize.Bytes((lasthistory.Get(fastsync.SentOverWire) + lasthistory.Get(fastsync.RecievedOverWire))),
				humanize.Bytes((lasthistory.Get(fastsync.SentBytes) + lasthistory.Get(fastsync.RecievedBytes))),
				humanize.Bytes(lasthistory.Get(fastsync.ReadBytes)), humanize.Bytes((lasthistory.Get(fastsync.WrittenBytes))),
				humanize.Bytes((lasthistory.Get(fastsync.BytesProcessed))),
				(lasthistory.Get(fastsync.FilesProcessed)),
				(lasthistory.Get(fastsync.DirectoriesProcessed)))

			// tslc.Push(timeserieslinechart.TimePoint{Time: curStat.startTime, Value: float64(curStat.performance.Get(fastsync.ReadBytes))})
			tslc.PushDataSet("localRead", timeserieslinechart.TimePoint{Time: curStat.startTime, Value: float64(curStat.performance.Get(fastsync.ReadBytes))})
			tslc.PushDataSet("localWritten", timeserieslinechart.TimePoint{Time: curStat.startTime, Value: float64(curStat.performance.Get(fastsync.WrittenBytes))})
			tslc.PushDataSet("recievedOverWire", timeserieslinechart.TimePoint{Time: curStat.startTime, Value: float64(curStat.performance.Get(fastsync.RecievedOverWire))})
			tslc.PushDataSet("processed", timeserieslinechart.TimePoint{Time: curStat.startTime, Value: float64(curStat.performance.Get(fastsync.BytesProcessed))})
			tslc.SetViewTimeRange(curStat.startTime.Add(-60*time.Second), curStat.startTime)

			tslc.DrawBrailleAll()
			view := tslc.View()
			clearAndDraw(view)
		}
	}
}
