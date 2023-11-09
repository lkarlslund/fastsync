package main

import (
	"fmt"
	"io/fs"
	"os"
	"slices"
	"syscall"

	"github.com/joshlf/go-acl"
	"github.com/pkg/xattr"
	unix "golang.org/x/sys/unix"
)

type FileInfo struct {
	Name  string
	Mode  fs.FileMode // Go simplified file type, not for chmod
	Size  int64
	IsDir bool

	Permissions uint32
	ACL         acl.ACL
	Xattrs      map[string][]byte

	Owner, Group uint32
	Inode, Nlink uint64
	Dev, Rdev    uint64
	LinkTo       string

	Atim syscall.Timespec
	Mtim syscall.Timespec
	Ctim syscall.Timespec
}

func PathToFileInfo(absolutepath string) (FileInfo, error) {
	fi, err := os.Lstat(absolutepath)
	if err != nil {
		return FileInfo{}, err
	}
	return InfoToFileInfo(fi, absolutepath)
}

func InfoToFileInfo(info os.FileInfo, absolutepath string) (FileInfo, error) {
	fi := FileInfo{
		Name:  absolutepath,
		Mode:  info.Mode(),
		Size:  info.Size(),
		IsDir: info.IsDir(),
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
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

		if fi.Mode&os.ModeSymlink != 0 {
			logger.Trace().Msgf("Detected %v as symlink", fi.Name)
			// Symlink - read link and store in fi variable
			linkto := make([]byte, 65536)
			n, err := syscall.Readlink(absolutepath, linkto)
			if err != nil {
				logger.Error().Msgf("Error reading link to %v: %v", fi.Name, err)
			} else {
				logger.Trace().Msgf("Detected %v as symlink to %v", fi.Name, string(linkto))
			}
			fi.LinkTo = string(linkto[0:n])
		} else if fi.Mode&os.ModeCharDevice != 0 && fi.Mode&os.ModeDevice != 0 {
			logger.Trace().Msgf("Detected %v as character device", fi.Name)
		} else if fi.Mode&os.ModeDir != 0 {
			logger.Trace().Msgf("Detected %v as directory", fi.Name)
		} else if fi.Mode&os.ModeSocket != 0 {
			logger.Trace().Msgf("Detected %v as socket", fi.Name)
		} else if fi.Mode&os.ModeNamedPipe != 0 {
			logger.Trace().Msgf("Detected %v as FIFO", fi.Name)
		} else if fi.Mode&os.ModeDevice != 0 {
			logger.Trace().Msgf("Detected %v as device", fi.Name)
		} else {
			logger.Trace().Msgf("Detected %v as regular file", fi.Name)
		}
	} else {
		return fi, fmt.Errorf("stat failed, I got a %T", info.Sys())
	}
	if info.Mode()&os.ModeSymlink == 0 {
		acl, err := acl.Get(absolutepath)
		if err != nil && err.Error() != "operation not supported" {
			logger.Warn().Msgf("Failed to get ACL for file %v: %v", fi.Name, err)
		}
		fi.ACL = acl

		if xattr.XATTR_SUPPORTED {
			xattrs, err := xattr.LList(absolutepath)
			if err != nil && err.Error() != "operation not supported" {
				logger.Warn().Msgf("Failed to get Xattrs for file %v: %v", fi.Name, err)
			}
			fi.Xattrs = make(map[string][]byte)
			for _, curxattr := range xattrs {
				value, err := xattr.LGet(absolutepath, curxattr)
				if err != nil && err.Error() != "operation not supported" {
					logger.Warn().Msgf("Failed to get Xattr %v for file %v: %v", curxattr, fi.Name, err)
				}
				fi.Xattrs[curxattr] = value
			}
		}
	}
	// syscall.Getxattr(absolutepath, "security.capability", fi.Xattrs)
	return fi, nil
}

func (fi FileInfo) Compare(fi2 FileInfo) (different, requiresDelete bool) {
	panic("not implemented")
	return false, false
}

func (fi FileInfo) ApplyChanges(fi2 FileInfo) error {
	logger.Debug().Msgf("Updating metadata for %s", fi.Name)
	err := os.Lchown(fi.Name, int(fi2.Owner), int(fi2.Group))
	if err != nil {
		logger.Error().Msgf("Error changing owner for %s: %v", fi.Name, err)
	}
	if fi2.Mode&fs.ModeSymlink == 0 {
		// err = unix.Chmod(fi.Name, fi2.Permissions)
		err = os.Chmod(fi.Name, fi2.Mode)
		if err != nil {
			logger.Error().Msgf("Error changing mode for %s: %v", fi.Name, err)
		}

		if len(fi2.ACL) > 0 {
			currentacl, err := acl.Get(fi.Name)
			if err != nil {
				logger.Error().Msgf("Error getting ACL for %s: %v", fi.Name, err)
			} else {
				if !slices.Equal(currentacl, fi2.ACL) {
					err = acl.Set(fi.Name, fi2.ACL)
					if err != nil {
						logger.Error().Msgf("Error setting ACL %+v (was %+v) for %s: %v", fi2.ACL, currentacl, fi.Name, err)
					}
				}
			}
		}

		if xattr.XATTR_SUPPORTED && fi2.Xattrs != nil {
			if fi.Xattrs != nil {
				// delete attribute that do not exist in fi2
				for attr := range fi.Xattrs {
					if _, found := fi2.Xattrs[attr]; !found {
						err = xattr.LRemove(fi.Name, attr)
						if err != nil {
							logger.Error().Msgf("Error removing Xattr %v for %s: %v", attr, fi.Name, err)
						}
					}
				}
			}

			// set attributes
			for attr, values := range fi2.Xattrs {
				if localvalues, found := fi.Xattrs[attr]; found {
					if !slices.Equal(localvalues, values) {
						err = xattr.LSet(fi.Name, attr, values)
						if err != nil {
							logger.Error().Msgf("Error setting Xattr %v for %s: %v", attr, fi.Name, err)
						}
					}
				} else {
					err = xattr.LSet(fi.Name, attr, values)
					if err != nil {
						logger.Error().Msgf("Error setting Xattr %v for %s: %v", attr, fi.Name, err)
					}
				}
			}
		}
	}

	err = unix.UtimesNanoAt(unix.AT_FDCWD, fi.Name, []unix.Timespec{unix.Timespec(fi2.Atim), unix.Timespec(fi2.Mtim)}, unix.AT_SYMLINK_NOFOLLOW)
	if err != nil {
		logger.Error().Msgf("Error changing times for %s: %v", fi.Name, err)
	}

	return nil
}
