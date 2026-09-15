//go:build linux

package fastsync

import (
	"golang.org/x/sys/unix"
	"os"
)

func duplicateFlushFile(file *os.File) (*os.File, error) {
	fd, err := unix.FcntlInt(file.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), file.Name()), nil
}
