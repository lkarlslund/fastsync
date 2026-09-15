//go:build !linux && !windows

package fastsync

import (
	"io/fs"
	"syscall"
)

func (fi FileInfo) Create(FileInfo) error        { return ErrNotSupportedByPlatform }
func (fi *FileInfo) Chown(FileInfo) error        { return ErrNotSupportedByPlatform }
func (fi FileInfo) Chmod(FileInfo) error         { return ErrNotSupportedByPlatform }
func (fi FileInfo) SetTimestamps(FileInfo) error { return ErrNotSupportedByPlatform }
func (fi *FileInfo) extractNativeInfo(fsfi fs.FileInfo) error {
	if stat, ok := fsfi.Sys().(*syscall.Stat_t); ok {
		fi.Inode = stat.Ino
		fi.Nlink = uint64(stat.Nlink) // force to uint64 for 32-bit systems
		fi.Dev = uint64(stat.Dev)
		fi.Rdev = uint64(stat.Rdev)
		fi.Owner = stat.Uid
		fi.Group = stat.Gid
		fi.Permissions = uint32(stat.Mode)

		atim, mtim, ctim := getAMtime(*stat)
		fi.Atim = atim
		fi.Mtim = mtim
		fi.Ctim = ctim
		return nil
	}
	return ErrNotSupportedByPlatform // wrong
}
