package main

import (
	"fmt"
	"html"
	"image"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lkarlslund/fastsync"
	"github.com/mum4k/termdash"
	"github.com/mum4k/termdash/cell"
	"github.com/mum4k/termdash/private/event/eventqueue"
	"github.com/mum4k/termdash/private/faketerm"
	"github.com/mum4k/termdash/widgets/text"
)

// Optional rendering preview, using synthetic counters and the real layout.
func TestDashboardPreview(t *testing.T) {
	output := os.Getenv("FASTSYNC_TUI_PREVIEW")
	if output == "" {
		t.Skip("set FASTSYNC_TUI_PREVIEW to an SVG output path")
	}
	terminal := faketerm.MustNew(image.Pt(150, 42), faketerm.WithEventQueue(eventqueue.New()))
	view := &chartView{}
	chart, activity := &historyChart{view: view}, &historyChart{stacked: true, view: view}
	statsView, _ := text.New()
	logView, _ := text.New(text.RollContent())
	c := fastsync.NewClient()
	var total fastsync.PerformanceEntry
	var history rateHistory
	var s stats
	for second := 1; second <= 60; second++ {
		wave := 1 + 0.35*math.Sin(float64(second)/5)
		for k, v := range map[fastsync.PerformanceCounterType]uint64{
			fastsync.ReadBytes: uint64(25e6 * wave), fastsync.WrittenBytes: uint64(82e6 * wave), fastsync.RecievedOverWire: uint64(56e6 * wave), fastsync.RecievedBytes: uint64(78e6 * wave),
			fastsync.DirectoriesProcessed: uint64(85 * wave), fastsync.FilesLinked: uint64(240 * wave), fastsync.FilesUnchanged: uint64(760 * wave), fastsync.FilesCopied: uint64(180 * wave), fastsync.FilesProcessed: uint64(1180 * wave),
		} {
			c.Perf.Add(k, v)
		}
		p := c.Perf.NextHistory()
		total = total.Add(p)
		s = stats{elapsed: time.Duration(second) * time.Second, interval: time.Second, performance: p, total: total, inodecache: 284109, directorycache: 128, files: 720, stack: 24, tuning: fastsync.TransferTuning{Phase: 2, ReadLimit: 64, WriteLimit: 64, ActiveWrites: 28, ActiveFiles: 42, FileLimit: 64, BufferReserved: 840 << 20, BufferLimit: 1280 << 20, FlushedBytes: uint64(second) * 70e6, FlushCount: 20, PendingFlushFiles: 12, PendingFlushBytes: 64 << 20, LastFlush: 180 * time.Millisecond, CheckQueue: 128, CopyQueue: 32, LinkQueue: 90, Dependencies: 240}, bottleneck: fastsync.BottleneckStatus{Label: "Client IO"}}
		s.rates = history.collect(s)
		chart.add(s)
		activity.add(s)
	}
	if err := writeStats(statsView, s); err != nil {
		t.Fatal(err)
	}
	logView.Write("12:00:00 INF Copying from source.example.com/server-a to /archive/server-a\n12:00:01 INF Existing-file pass complete; reuse cache ready\n12:00:01 INF Copying files and preserving hardlink groups\n", text.WriteCellOpts(cell.FgColor(dashboardMuted)))
	root, err := dashboardLayout(terminal, chart, activity, statsView, logView)
	if err != nil {
		t.Fatal(err)
	}
	controller, err := termdash.NewController(terminal, root)
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if err := controller.Redraw(); err != nil {
		t.Fatal(err)
	}
	var svg strings.Builder
	fmt.Fprint(&svg, `<svg xmlns="http://www.w3.org/2000/svg" width="1532" height="788" viewBox="0 0 1532 788"><rect width="100%" height="100%" fill="#11151c"/><g font-family="DejaVu Sans Mono" font-size="15">`)
	color := func(c cell.Color) string {
		n := int(c) - 1
		if n >= 232 {
			v := 8 + (n-232)*10
			return fmt.Sprintf("#%02x%02x%02x", v, v, v)
		}
		if n >= 16 {
			n -= 16
			levels := []int{0, 95, 135, 175, 215, 255}
			return fmt.Sprintf("#%02x%02x%02x", levels[n/36], levels[n/6%6], levels[n%6])
		}
		return "#d0d0d0"
	}
	for x, column := range terminal.BackBuffer() {
		for y, v := range column {
			if v.Rune == 0 || v.Rune == ' ' {
				continue
			}
			fmt.Fprintf(&svg, `<text x="%d" y="%d" fill="%s">%s</text>`, 16+x*10, 30+y*18, color(v.Opts.FgColor), html.EscapeString(string(v.Rune)))
		}
	}
	fmt.Fprint(&svg, "</g></svg>")
	if err := os.WriteFile(output, []byte(svg.String()), 0600); err != nil {
		t.Fatal(err)
	}
}
