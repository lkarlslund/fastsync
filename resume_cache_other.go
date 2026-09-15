//go:build !linux

package fastsync

import "os"

func lockResumeCache(*os.File) error { return ErrNotSupportedByPlatform }
