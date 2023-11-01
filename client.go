package main

import "syscall"

type inodeinfo struct {
	inode     uint64
	path      string
	remaining int32
}

func (i inodeinfo) LessThan(i2 inodeinfo) bool {
	return i.inode < i2.inode
}

type folderinfo struct {
	name      string
	atim      syscall.Timespec
	mtim      syscall.Timespec
	remaining int32
}

func (f folderinfo) LessThan(f2 folderinfo) bool {
	return f.name < f2.name
}
