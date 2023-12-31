//go:build !freebsd && !windows
// +build !freebsd,!windows

package main

import "syscall"

func mkNod(localpath string, iftyp uint32, rdev uint64) error {
	return syscall.Mknod(localpath, iftyp, int(rdev))
}
