//go:build linux

package fastsync

import (
	"errors"

	"golang.org/x/sys/unix"
)

const completionStampsSupported = true

func syncCompletionFilesystem(path string) error {
	f, err := openNoFollow(path)
	if err != nil {
		return err
	}
	return errors.Join(unix.Syncfs(int(f.Fd())), f.Close())
}
