//go:build !linux

package fastsync

import "os"

func duplicateFlushFile(file *os.File) (*os.File, error) {
	return OpenFileNoFollow(file.Name(), os.O_RDWR, 0)
}
