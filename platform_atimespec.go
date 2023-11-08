//go:build darwin || freebsd
// +build darwin freebsd

package main

import "syscall"

func getAMtime(s syscall.Stat_t) (syscall.Timespec, syscall.Timespec, syscall.Timespec) {
	return s.Atimespec, s.Mtimespec, s.Ctimespec
}
