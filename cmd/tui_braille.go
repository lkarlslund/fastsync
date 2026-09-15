package main

import "github.com/mum4k/termdash/cell"

type brailleCell struct {
	bits  uint8
	color cell.Color
}
type braillePlot struct {
	width, rows int
	cells       []brailleCell
}

func newBraillePlot(width, rows int) *braillePlot {
	return &braillePlot{width: width, rows: rows, cells: make([]brailleCell, width*rows)}
}
func (p *braillePlot) dot(x, y int, color cell.Color) {
	if x < 0 || y < 0 || x >= 2*p.width || y >= 4*p.rows {
		return
	}
	bits := [2][4]uint8{{1, 2, 4, 64}, {8, 16, 32, 128}}
	c := &p.cells[(y/4)*p.width+x/2]
	c.bits |= bits[x%2][y%4]
	c.color = color
}
func (p *braillePlot) line(x0, y0, x1, y1 int, color cell.Color) {
	abs := func(x int) int {
		if x < 0 {
			return -x
		}
		return x
	}
	dx, dy := abs(x1-x0), -abs(y1-y0)
	sx, sy := 1, 1
	if x0 > x1 {
		sx = -1
	}
	if y0 > y1 {
		sy = -1
	}
	err := dx + dy
	for {
		p.dot(x0, y0, color)
		if x0 == x1 && y0 == y1 {
			return
		}
		e := 2 * err
		if e >= dy {
			err += dy
			x0 += sx
		}
		if e <= dx {
			err += dx
			y0 += sy
		}
	}
}
