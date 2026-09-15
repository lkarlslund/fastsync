package main

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// RSS on Linux includes more than Go's heap. Elsewhere use Go-managed memory.
// This is a sampled abort guard, not an OS-enforced allocation limit.
func processMemory() (uint64, error) {
	if runtime.GOOS == "linux" {
		data, err := os.ReadFile("/proc/self/statm")
		if err != nil {
			return 0, err
		}
		fields := strings.Fields(string(data))
		if len(fields) < 2 {
			return 0, fmt.Errorf("invalid /proc/self/statm")
		}
		pages, err := strconv.ParseUint(fields[1], 10, 64)
		return pages * uint64(os.Getpagesize()), err
	}
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.Sys - stats.HeapReleased, nil
}

func startMemoryWatch(limit uint64, graceful ...chan<- error) {
	check := func() bool {
		used, err := processMemory()
		if err == nil && used > limit {
			err = fmt.Errorf("memory limit exceeded: %d > %d bytes; operation incomplete", used, limit)
		}
		if err == nil {
			return true
		}
		if len(graceful) > 0 {
			select {
			case graceful[0] <- err:
			default:
			}
			return false
		}
		fmt.Fprintln(os.Stderr, "memory guard:", err)
		os.Exit(1)
		return false
	}
	if !check() {
		return
	}
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			if !check() {
				return
			}
		}
	}()
}
