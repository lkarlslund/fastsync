//go:build !windows
// +build !windows

package main

import (
	"io/fs"
	"os"
	"syscall"

	unix "golang.org/x/sys/unix"
)

func (f FileInfo) Create(remotefi FileInfo) error {
	if remotefi.Mode&fs.ModeDevice != 0 {
		if remotefi.Mode&fs.ModeCharDevice != 0 {
			return mkNod(f.Name, syscall.S_IFCHR, remotefi.Rdev)
		} else {
			// Block device
			return mkNod(f.Name, syscall.S_IFBLK, remotefi.Rdev)
		}
	} else if remotefi.Mode&fs.ModeNamedPipe != 0 {
		return mkNod(f.Name, syscall.S_IFIFO, remotefi.Rdev)
	} else if remotefi.Mode&fs.ModeSocket != 0 {
		return mkNod(f.Name, syscall.S_IFSOCK, remotefi.Rdev)
	} else if remotefi.Mode&fs.ModeSymlink != 0 {
		return syscall.Symlink(remotefi.LinkTo, f.Name)
	}
	file, err := os.Create(f.Name)
	defer file.Close()
	return err
}

func (f FileInfo) SetTimestamps(fi2 FileInfo) error {
	return unix.UtimesNanoAt(unix.AT_FDCWD, f.Name, []unix.Timespec{unix.Timespec(fi2.Atim), unix.Timespec(fi2.Mtim)}, unix.AT_SYMLINK_NOFOLLOW)
}

func (f FileInfo) Chmod(fi2 FileInfo) error {
	return unix.Fchmodat(unix.AT_FDCWD, f.Name, fi2.Permissions, unix.AT_SYMLINK_NOFOLLOW)
}

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
