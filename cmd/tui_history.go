package main

import (
	"fmt"
	"image"
	"math"
	"sync"
	"time"

	"github.com/lkarlslund/fastsync"
	"github.com/mum4k/termdash/cell"
	"github.com/mum4k/termdash/private/canvas"
	"github.com/mum4k/termdash/terminal/terminalapi"
	"github.com/mum4k/termdash/widgetapi"
)

// historyChart draws stable time buckets with the latest bucket at the right edge.
type historyChart struct {
	mu      sync.Mutex
	stacked bool
	buckets [4][]historyBucket
	latest  time.Duration
	view    *chartView
}

func sampleRate(s stats, counter fastsync.PerformanceCounterType) float64 {
	seconds := s.interval.Seconds()
	if seconds <= 0 {
		seconds = 1
	}
	return float64(s.performance.Get(counter)) / seconds
}
func (h *historyChart) Options() widgetapi.Options {
	scope := widgetapi.KeyScopeGlobal
	if h.stacked {
		scope = widgetapi.KeyScopeNone
	}
	return widgetapi.Options{MinimumSize: image.Pt(12, 4), WantKeyboard: scope}
}
func (h *historyChart) Keyboard(k *terminalapi.Keyboard, _ *widgetapi.EventMeta) error {
	if h.view != nil && k != nil && (k.Key == 'l' || k.Key == 'L') {
		if !h.stacked {
			h.view.logarithmic.Store(!h.view.logarithmic.Load())
		}
	} else {
		h.view.key(k)
	}
	return nil
}
func (h *historyChart) Mouse(*terminalapi.Mouse, *widgetapi.EventMeta) error { return nil }
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
			if err := put(x, y, r, dashboardMuted); err != nil {
				return err
			}
			x++
		}
		return nil
	}
	if h.latest == 0 {
		return label(0, 0, "Waiting for samples")
	}
	const plotLeft = 7 // Numeric Y labels and the axis.
	plotWidth := w - plotLeft
	resolution := h.view.selection()
	step := chartIntervals[resolution]
	columns, counts := h.columns(plotWidth, resolution)
	colors := []cell.Color{dashboardRead, dashboardWrite, dashboardWire, dashboardWire}
	if h.stacked {
		colors = []cell.Color{dashboardDir, dashboardLink, dashboardSame, dashboardCopy}
	}
	peak := float64(1)
	for x := range columns {
		if counts[x] == 0 {
			continue
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
	scale := newHistoryScale(peak, h.stacked)
	mode := "linear"
	if h.view != nil && h.view.logarithmic.Load() {
		mode = "log"
		scale.logarithmic = true
		smallest := peak
		for _, column := range columns {
			for _, v := range column {
				if v > 0 {
					smallest = math.Min(smallest, v)
				}
			}
		}
		scale.knee = math.Pow(10, math.Floor(math.Log10(math.Max(smallest, 1e-12))))
	}
	if err := label(0, 0, fmt.Sprintf("%s · %s/col · %s [L]", scale.unit, step, mode)); err != nil {
		return err
	}
	rows := height - 3
	names := []string{"Read", "Write", "Wire"}
	if h.stacked {
		names = []string{"Dir", "Link", "Same", "Copied"}
	}
	lx := 0
	for i, name := range names {
		if lx+len(name)+2 > w {
			break
		}
		for _, r := range "▪" + name + " " {
			if err := put(lx, 1, r, colors[i]); err != nil {
				return err
			}
			lx++
		}
	}
	// Ticks share the exact coordinate mapping used for plotted values.
	for y := 2; y <= height-2; y++ {
		if err := put(plotLeft-1, y, '│', dashboardBorder); err != nil {
			return err
		}
	}
	ticks := min(4, rows-1)
	if ticks < 1 {
		ticks = 1
	}
	seen := map[int]bool{}
	for i := 0; i <= ticks; i++ {
		offset := int(math.Round(float64(i) * float64(rows-1) / float64(ticks)))
		y := height - 2 - offset
		if seen[y] {
			continue
		}
		seen[y] = true
		value := float64(0)
		if rows > 1 {
			value = scale.value(float64(offset) / float64(rows-1))
		}
		if err := label(0, y, fmt.Sprintf("%5s", scale.tick(value))); err != nil {
			return err
		}
		if err := put(plotLeft-1, y, '┤', dashboardBorder); err != nil {
			return err
		}
		for x := plotLeft; x < w; x++ {
			if err := put(x, y, '┄', dashboardGrid); err != nil {
				return err
			}
		}
	}
	braille := newBraillePlot(plotWidth, rows)
	for x, values := range columns {
		if h.stacked {
			cumulative := float64(0)
			bottom := 0
			for i, v := range values {
				cumulative += v
				top := int(math.Round(scale.normalize(cumulative) * float64(rows-1)))
				for y := bottom + 1; y <= top; y++ {
					if err := put(plotLeft+x, height-2-y, '█', colors[i]); err != nil {
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
				y := int(math.Round((1 - scale.normalize(v)) * float64((rows-1)*4)))
				px := 2*x + 1
				braille.dot(px, y, colors[i])
				if x > 0 && counts[x-1] > 0 && columns[x-1][i] > 0 {
					previous := int(math.Round((1 - scale.normalize(columns[x-1][i])) * float64((rows-1)*4)))
					braille.line(px-2, previous, px, y, colors[i])
				}
			}
		}
	}
	if !h.stacked {
		for i, c := range braille.cells {
			if c.bits != 0 {
				if err := put(plotLeft+i%plotWidth, 2+i/plotWidth, rune(0x2800+int(c.bits)), c.color); err != nil {
					return err
				}
			}
		}
	}

	if plotWidth >= 9 {
		if err := label(plotLeft, height-1, chartSpan(plotWidth, step)); err != nil {
			return err
		}
	}
	if plotWidth >= 28 {
		if err := label(plotLeft+plotWidth/2-2, height-1, chartSpan(plotWidth/2, step)); err != nil {
			return err
		}
	}
	return label(w-3, height-1, "now")
}

// Keep one unit across the Y axis; round the range to avoid jittery maxima.
type historyScale struct {
	maximum, divisor float64
	logarithmic      bool
	knee             float64
	unit             string
}

func newHistoryScale(peak float64, stacked bool) historyScale {
	peak = math.Max(1, peak)
	raw := peak / 4
	power := math.Pow(10, math.Floor(math.Log10(raw)))
	step := power
	for _, multiple := range []float64{1, 2, 5, 10} {
		step = multiple * power
		if step >= raw {
			break
		}
	}
	s := historyScale{maximum: math.Ceil(peak/step) * step, divisor: 1, unit: "B/s"}
	if stacked {
		s.unit = "entries/s"
		if s.maximum >= 1e6 {
			s.divisor = 1e6
			s.unit = "M entries/s"
		} else if s.maximum >= 1e3 {
			s.divisor = 1e3
			s.unit = "k entries/s"
		}
		return s
	}
	for _, unit := range []string{"kB/s", "MB/s", "GB/s", "TB/s", "PB/s", "EB/s"} {
		if s.maximum/s.divisor < 1000 {
			break
		}
		s.divisor *= 1000
		s.unit = unit
	}
	return s
}
func (s historyScale) tick(value float64) string {
	v := value / s.divisor
	if v == 0 {
		return "0"
	}
	if v > 0 && v < 0.1 {
		return fmt.Sprintf("%.0e", v)
	}
	if v >= 100 {
		return fmt.Sprintf("%.0f", v)
	}
	return fmt.Sprintf("%.1f", v)
}

// A linear neighborhood of zero joined smoothly to logarithmic scaling. Unlike
// log(value), log1p(value/knee) is defined at zero and retains idle samples.
func (s historyScale) normalize(value float64) float64 {
	if !s.logarithmic {
		return value / s.maximum
	}
	return math.Log1p(value/s.knee) / math.Log1p(s.maximum/s.knee)
}
func (s historyScale) value(fraction float64) float64 {
	if !s.logarithmic {
		return fraction * s.maximum
	}
	return s.knee * math.Expm1(fraction*math.Log1p(s.maximum/s.knee))
}
