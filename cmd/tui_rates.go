package main

import (
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/lkarlslund/fastsync"
	"strings"
	"time"
)

type rateMetric struct {
	name     string
	bytes    bool
	counters []fastsync.PerformanceCounterType
}

var rateMetrics = []rateMetric{
	{"Wire", true, []fastsync.PerformanceCounterType{fastsync.SentOverWire, fastsync.RecievedOverWire}},
	{"Payload", true, []fastsync.PerformanceCounterType{fastsync.SentBytes, fastsync.RecievedBytes}},
	{"Read", true, []fastsync.PerformanceCounterType{fastsync.ReadBytes}},
	{"Write", true, []fastsync.PerformanceCounterType{fastsync.WrittenBytes}},
	{"Flushed", true, nil},
	{"Files", false, []fastsync.PerformanceCounterType{fastsync.FilesProcessed}},
	{"Dirs", false, []fastsync.PerformanceCounterType{fastsync.DirectoriesProcessed}},
	{"Link", false, []fastsync.PerformanceCounterType{fastsync.FilesLinked}},
	{"Same", false, []fastsync.PerformanceCounterType{fastsync.FilesUnchanged}},
	{"Copied", false, []fastsync.PerformanceCounterType{fastsync.FilesCopied}},
	{"Checked", false, []fastsync.PerformanceCounterType{fastsync.ExistingExamined}},
	{"Deleted", false, []fastsync.PerformanceCounterType{fastsync.EntriesDeleted}},
}

type metricRates struct {
	instant, minute, fiveMinute float64
	total                       uint64
}
type rateObservation struct {
	end, interval time.Duration
	values        []uint64
}
type rateHistory struct {
	samples []rateObservation
	flushed uint64
}

func (h *rateHistory) collect(s stats) []metricRates {
	interval := s.interval
	if interval <= 0 {
		interval = time.Second
	}
	o := rateObservation{end: s.elapsed, interval: interval, values: make([]uint64, len(rateMetrics))}
	result := make([]metricRates, len(rateMetrics))
	for i, m := range rateMetrics {
		for _, k := range m.counters {
			o.values[i] += s.performance.Get(k)
			result[i].total += s.total.Get(k)
		}
		if m.counters == nil {
			result[i].total = s.tuning.FlushedBytes
			if s.tuning.FlushedBytes >= h.flushed {
				o.values[i] = s.tuning.FlushedBytes - h.flushed
			}
		}
		result[i].instant = float64(o.values[i]) / interval.Seconds()
	}
	h.flushed = s.tuning.FlushedBytes
	h.samples = append(h.samples, o)
	for len(h.samples) > 1 && h.samples[0].end <= s.elapsed-5*time.Minute {
		h.samples = h.samples[1:]
	}
	for i := range result {
		result[i].minute = h.average(i, s.elapsed, time.Minute)
		result[i].fiveMinute = h.average(i, s.elapsed, 5*time.Minute)
	}
	return result
}
func (h *rateHistory) average(metric int, end, window time.Duration) float64 {
	var count, seconds float64
	for _, s := range h.samples {
		overlap := s.end - max(s.end-s.interval, end-window)
		if overlap <= 0 {
			continue
		}
		seconds += overlap.Seconds()
		count += float64(s.values[metric]) * float64(overlap) / float64(s.interval)
	}
	if seconds == 0 {
		return 0
	}
	return count / seconds
}
func formatRateTable(s stats) string {
	rates := s.rates
	if len(rates) != len(rateMetrics) {
		var h rateHistory
		rates = h.collect(s)
	}
	var out strings.Builder
	fmt.Fprintf(&out, "%-8s %8s %8s %8s %9s\n", "/s", "Instant", "1m", "5m", "Total")
	for i, m := range rateMetrics {
		r := rates[i]
		value := func(v float64) string {
			if m.bytes {
				return strings.ReplaceAll(humanize.Bytes(uint64(v)), " ", "")
			}
			return fmt.Sprintf("%.1f", v)
		}
		total := humanize.Comma(int64(r.total))
		if m.bytes {
			total = strings.ReplaceAll(humanize.Bytes(r.total), " ", "")
		}
		fmt.Fprintf(&out, "%-8s %8s %8s %8s %9s\n", m.name, value(r.instant), value(r.minute), value(r.fiveMinute), total)
	}
	return out.String()
}
