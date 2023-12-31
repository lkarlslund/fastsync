//go:build freebsd
// +build freebsd

package main

import "syscall"

func mkNod(localpath string, iftyp uint32, rdev uint64) error {
	return syscall.Mknod(localpath, iftyp, rdev)
}
