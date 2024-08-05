package fastsync

import (
	"io/fs"
	"syscall"
)

//go:generate codecgen -d 7331 -o codectypes_generated.go codectypes.go

type FileInfo struct {
	Name  string
	Mode  fs.FileMode // Go simplified file type, not for chmod
	Size  int64
	IsDir bool

	Permissions uint32
	Xattrs      map[string][]byte // xattrs contain ACL !!

	Owner, Group uint32
	Inode, Nlink uint64
	Dev, Rdev    uint64
	LinkTo       string

	Atim syscall.Timespec
	Mtim syscall.Timespec
	Ctim syscall.Timespec
}

type FileListResponse struct {
	ParentDirectory string
	Files           []FileInfo
}

type GetChunkArgs struct {
	Path   string
	Offset uint64
	Size   uint64
}
