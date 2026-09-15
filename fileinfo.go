package fastsync

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"syscall"

	"github.com/pkg/xattr"
)

var (
	ErrNotSupportedByPlatform = errors.New("not supported on this platform")
	ErrTypeError              = errors.New("type error")
)

func PathToFileInfo(absolutepath string) (FileInfo, error) {
	return pathToFileInfo(absolutepath, true)
}

func pathToFileInfo(absolutepath string, attrs bool) (FileInfo, error) {
	return pinnedFileInfo(absolutepath, attrs)
}

func InfoToFileInfo(info os.FileInfo, absolutepath string) (FileInfo, error) {
	return infoToFileInfo(info, absolutepath, true)
}

func infoToFileInfo(info os.FileInfo, absolutepath string, attrs bool) (FileInfo, error) {
	return pathToFileInfo(absolutepath, attrs)
}
func rawFileInfo(info os.FileInfo, absolutepath string, attrs bool) (FileInfo, error) {
	fi := FileInfo{
		Name:  absolutepath,
		Mode:  info.Mode(),
		Size:  info.Size(),
		IsDir: info.IsDir(),
	}

	if fi.Mode&os.ModeSymlink != 0 {
		Logger.Trace().Msgf("Detected %v as symlink", fi.Name)
		// Symlink - read link and store in fi variable
		linkto, err := os.Readlink(absolutepath)
		if err != nil {
			return fi, err
		}
		fi.LinkTo = linkto
	} else if fi.Mode&os.ModeCharDevice != 0 && fi.Mode&os.ModeDevice != 0 {
		Logger.Trace().Msgf("Detected %v as character device", fi.Name)
	} else if fi.Mode&os.ModeDir != 0 {
		Logger.Trace().Msgf("Detected %v as directory", fi.Name)
	} else if fi.Mode&os.ModeSocket != 0 {
		Logger.Trace().Msgf("Detected %v as socket", fi.Name)
	} else if fi.Mode&os.ModeNamedPipe != 0 {
		Logger.Trace().Msgf("Detected %v as FIFO", fi.Name)
	} else if fi.Mode&os.ModeDevice != 0 {
		Logger.Trace().Msgf("Detected %v as device", fi.Name)
	} else {
		Logger.Trace().Msgf("Detected %v as regular file", fi.Name)
	}

	if attrs {
		if xattr.XATTR_SUPPORTED {
			xattrs, err := xattr.LList(absolutepath)
			if err != nil && !errors.Is(err, syscall.ENOTSUP) {
				return fi, fmt.Errorf("list xattrs %s: %w", fi.Name, err)
			}
			fi.Xattrs = make(map[string][]byte)
			for _, curxattr := range xattrs {
				value, err := xattr.LGet(absolutepath, curxattr)
				if err != nil {
					return fi, fmt.Errorf("read xattr %s on %s: %w", curxattr, fi.Name, err)
				}
				fi.Xattrs[curxattr] = value
			}
		}
	}

	err := fi.extractNativeInfo(info)
	return fi, err
}

const permissionBits = os.ModePerm | os.ModeSetuid | os.ModeSetgid | os.ModeSticky

func (fi FileInfo) ApplyChanges(fi2 FileInfo) error {
	ownershipChanged := fi.Owner != fi2.Owner || fi.Group != fi2.Group
	if ownershipChanged {
		if err := fi.Chown(fi2); err != nil {
			return fmt.Errorf("chown %s: %w", fi.Name, err)
		}
	}
	if fi2.Mode&fs.ModeSymlink == 0 {
		// chown can clear setuid/setgid; restore mode after ownership.
		if ownershipChanged || fi.Mode&permissionBits != fi2.Mode&permissionBits {
			if err := fi.Chmod(fi2); err != nil {
				return fmt.Errorf("chmod %s: %w", fi.Name, err)
			}
		}
	}
	if fi2.Xattrs != nil {
		if !xattr.XATTR_SUPPORTED && len(fi2.Xattrs) > 0 {
			return ErrNotSupportedByPlatform
		}
		for attr := range fi.Xattrs {
			if _, found := fi2.Xattrs[attr]; !found {
				if err := removeXattrNoFollow(fi.Name, attr); err != nil {
					return err
				}
			}
		}
		for attr, value := range fi2.Xattrs {
			old, found := fi.Xattrs[attr]
			if !found || !slices.Equal(old, value) || ownershipChanged {
				if err := setXattrNoFollow(fi.Name, attr, value); err != nil {
					return err
				}
			}
		}
	}
	if fi.Mtim != fi2.Mtim || fi.Atim != fi2.Atim {
		if err := fi.SetTimestamps(fi2); err != nil {
			return fmt.Errorf("timestamps %s: %w", fi.Name, err)
		}
	}
	return nil
}
