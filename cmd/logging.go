package main

import (
	"io"
	"os"

	"github.com/lkarlslund/fastsync"
	"github.com/rs/zerolog"
)

func openLogFile(path string) (*os.File, error) {
	return fastsync.OpenFileNoFollow(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
}

func loggerWithFile(level zerolog.Level, display, file io.Writer) zerolog.Logger {
	if file != nil {
		display = zerolog.MultiLevelWriter(file, display)
	}
	return zerolog.New(display).With().Timestamp().Logger().Level(level)
}
