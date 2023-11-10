//go:build !darwin && !freebsd && !windows
// +build !darwin,!freebsd,!windows

package main

import "syscall"

func getAMtime(s syscall.Stat_t) (syscall.Timespec, syscall.Timespec, syscall.Timespec) {
	return s.Atim, s.Mtim, s.Ctim
}
