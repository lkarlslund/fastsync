package fastsync

import (
	"golang.org/x/sys/unix"
	"os"
)

func lockResumeCache(f *os.File) error { return unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB) }
