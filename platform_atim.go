//go:build !darwin && !freebsd
// +build !darwin,!freebsd

package main

import "syscall"

func getAMtime(s syscall.Stat_t) (syscall.Timespec, syscall.Timespec, syscall.Timespec) {
	return s.Atim, s.Mtim, s.Ctim
}
