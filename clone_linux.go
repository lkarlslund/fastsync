package fastsync

import (
	"errors"
	"golang.org/x/sys/unix"
	"os"
)

func cloneFile(dest, source *os.File) error {
	err := unix.IoctlFileClone(int(dest.Fd()), int(source.Fd()))
	if errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTTY) || errors.Is(err, unix.EXDEV) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOSYS) {
		return ErrNotSupportedByPlatform
	}
	return err
}
