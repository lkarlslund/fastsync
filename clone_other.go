//go:build !linux

package fastsync

import "os"

func cloneFile(dest, source *os.File) error { return ErrNotSupportedByPlatform }
