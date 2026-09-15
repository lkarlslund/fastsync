package main

import (
	"fmt"
	"image"
	"math"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/lkarlslund/fastsync"
	"github.com/mum4k/termdash/cell"
	"github.com/mum4k/termdash/private/canvas"
	"github.com/mum4k/termdash/terminal/terminalapi"
	"github.com/mum4k/termdash/widgetapi"
)

// historyChart places the latest observation at the right edge rather than
// extending the domain to the next rounded axis tick. Both charts use real time.
type historyChart struct {
	mu      sync.Mutex
	stacked bool
	samples []stats
}

func (h *historyChart) add(s stats) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.samples = append(h.samples, s)
	for len(h.samples) > 1 && h.samples[0].elapsed <= s.elapsed-60*time.Second {
		h.samples = h.samples[1:]
	}
}
func sampleRate(s stats, counter fastsync.PerformanceCounterType) float64 {
	seconds := s.interval.Seconds()
	if seconds <= 0 {
		seconds = 1
	}
	return float64(s.performance.Get(counter)) / seconds
}
func (h *historyChart) Options() widgetapi.Options {
	return widgetapi.Options{MinimumSize: image.Pt(12, 4)}
}
func (h *historyChart) Keyboard(*terminalapi.Keyboard, *widgetapi.EventMeta) error { return nil }
func (h *historyChart) Mouse(*terminalapi.Mouse, *widgetapi.EventMeta) error       { return nil }
func (h *historyChart) Draw(c *canvas.Canvas, _ *widgetapi.Meta) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	w, height := c.Size().X, c.Size().Y
	if w < 12 || height < 4 {
		return nil
	}
	put := func(x, y int, r rune, color cell.Color) error {
		_, err := c.SetCell(image.Pt(x, y), r, cell.FgColor(color))
		return err
	}
	label := func(x, y int, s string) error {
		for _, r := range s {
			if x >= w {
				break
			}
			if err := put(x, y, r, cell.ColorWhite); err != nil {
				return err
			}
			x++
		}
		return nil
	}
	if len(h.samples) == 0 {
		return label(0, 0, "Waiting for samples")
	}
	// Each screen column represents a time bucket; narrow windows average rates
	// instead of summing them, and wide windows repeat the one-second bar.
	columns := make([][4]float64, w)
	counts := make([]int, w)
	latest := h.samples[len(h.samples)-1].elapsed
	counters := []fastsync.PerformanceCounterType{fastsync.ReadBytes, fastsync.WrittenBytes, fastsync.SentOverWire, fastsync.RecievedOverWire}
	colors := []cell.Color{cell.ColorGreen, cell.ColorBlue, cell.ColorYellow, cell.ColorYellow}
	if h.stacked {
		counters = []fastsync.PerformanceCounterType{fastsync.DirectoriesProcessed, fastsync.FilesLinked, fastsync.FilesUnchanged, fastsync.FilesCopied}
		colors = []cell.Color{cell.ColorCyan, cell.ColorYellow, cell.ColorGreen, cell.ColorMagenta}
	}
	for _, s := range h.samples {
		age := (latest - s.elapsed).Seconds()
		if age >= 60 {
			continue
		}
		left := max(0, int(math.Floor((59-age)*float64(w)/60)))
		right := min(w-1, int(math.Ceil((60-age)*float64(w)/60))-1)
		for x := left; x <= right; x++ {
			counts[x]++
			for i, k := range counters {
				columns[x][i] += sampleRate(s, k)
			}
		}
	}
	peak := float64(1)
	for x := range columns {
		if counts[x] == 0 {
			continue
		}
		for i := range columns[x] {
			columns[x][i] /= float64(counts[x])
		}
		if !h.stacked {
			columns[x][2] += columns[x][3]
			columns[x][3] = 0
		}
		sum := float64(0)
		for _, v := range columns[x] {
			if h.stacked {
				sum += v
			} else {
				sum = math.Max(sum, v)
			}
		}
		peak = math.Max(peak, sum)
	}
	scale := humanize.Bytes(uint64(peak)) + "/s"
	if h.stacked {
		scale = fmt.Sprintf("%.1f entries/s", peak)
	}
	if err := label(0, 0, scale); err != nil {
		return err
	}
	rows := height - 2
	for x, values := range columns {
		if h.stacked {
			cumulative := float64(0)
			bottom := 0
			for i, v := range values {
				cumulative += v
				top := int(math.Round(cumulative / peak * float64(rows)))
				for y := bottom; y < top; y++ {
					if err := put(x, height-2-y, '█', colors[i]); err != nil {
						return err
					}
				}
				bottom = top
			}
		} else {
			for i, v := range values[:3] {
				if v <= 0 {
					continue
				}
				y := height - 2 - int(math.Round(v/peak*float64(rows-1)))
				if err := put(x, y, '•', colors[i]); err != nil {
					return err
				}
			}
		}
	}
	if err := label(0, height-1, "-60s"); err != nil {
		return err
	}
	return label(w-3, height-1, "now")
}
