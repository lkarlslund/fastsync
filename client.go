package main

type inodeinfo struct {
	inode     uint64
	path      string
	remaining int32
}

func (i inodeinfo) LessThan(i2 inodeinfo) bool {
	return i.inode < i2.inode
}
