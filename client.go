package main

type inodeinfo struct {
	inode     uint64
	path      string
	remaining int32
}

func (i inodeinfo) LessThan(i2 inodeinfo) bool {
	return i.inode < i2.inode
}

type folderinfo struct {
	remaining int32
	info      FileInfo
}

func (f folderinfo) LessThan(f2 folderinfo) bool {
	return f.info.Name < f2.info.Name
}
