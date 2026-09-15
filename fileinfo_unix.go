//go:build linux
// +build linux

package fastsync

import (
	"io/fs"
	"os"
	"syscall"

	unix "golang.org/x/sys/unix"
)

func (fi FileInfo) Create(fi2 FileInfo) error {
	if fi2.Mode&fs.ModeDevice != 0 {
		if fi2.Mode&fs.ModeCharDevice != 0 {
			return mknodNoFollow(fi.Name, syscall.S_IFCHR, fi2.Rdev)
		} else {
			// Block device
			return mknodNoFollow(fi.Name, syscall.S_IFBLK, fi2.Rdev)
		}
	} else if fi2.Mode&fs.ModeNamedPipe != 0 {
		return mknodNoFollow(fi.Name, syscall.S_IFIFO, fi2.Rdev)
	} else if fi2.Mode&fs.ModeSocket != 0 {
		return mknodNoFollow(fi.Name, syscall.S_IFSOCK, fi2.Rdev)
	} else if fi2.Mode&fs.ModeSymlink != 0 {
		return symlinkNoFollow(fi2.LinkTo, fi.Name)
	}
	file, err := OpenFileNoFollow(fi.Name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
	if err == nil {
		err = file.Close()
	}
	return err
}

func (fi *FileInfo) Chown(fi2 FileInfo) error {
	return chownNoFollow(fi.Name, int(fi2.Owner), int(fi2.Group))
}

func (fi FileInfo) SetTimestamps(fi2 FileInfo) error {
	return timesNoFollow(fi.Name, []unix.Timespec{unix.Timespec(fi2.Atim), unix.Timespec(fi2.Mtim)})
}

func (fi FileInfo) Chmod(fi2 FileInfo) error {
	return chmodNoFollow(fi.Name, fi2.Permissions&07777)
	// return unix.Fchmodat(unix.AT_FDCWD, f.Name, fi2.Permissions, unix.AT_SYMLINK_NOFOLLOW)
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
