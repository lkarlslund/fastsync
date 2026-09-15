package main

import (
	"github.com/mum4k/termdash/cell"
	"strings"
)

// Shared chart, legend and table colors keep the same metric recognizable.
var (
	dashboardInk    = cell.ColorNumber(252)
	dashboardMuted  = cell.ColorNumber(245)
	dashboardBorder = cell.ColorNumber(60)
	dashboardGrid   = cell.ColorNumber(238)
	dashboardRead   = cell.ColorNumber(111)
	dashboardWrite  = cell.ColorNumber(81)
	dashboardWire   = cell.ColorNumber(222)
	dashboardDir    = cell.ColorNumber(111)
	dashboardLink   = cell.ColorNumber(222)
	dashboardSame   = cell.ColorNumber(114)
	dashboardCopy   = cell.ColorNumber(177)
)

func metricColor(name string) cell.Color {
	switch name {
	case "Read":
		return dashboardRead
	case "Write", "Flushed":
		return dashboardWrite
	case "Wire", "Payload":
		return dashboardWire
	case "Dirs":
		return dashboardDir
	case "Link":
		return dashboardLink
	case "Same":
		return dashboardSame
	case "Copied":
		return dashboardCopy
	default:
		return dashboardInk
	}
}
func usageMeter(used, limit int64) string {
	if limit <= 0 {
		return ""
	}
	filled := int(min(int64(12), max(int64(0), used)*12/limit))
	return strings.Repeat("━", filled) + strings.Repeat("─", 12-filled)
}
