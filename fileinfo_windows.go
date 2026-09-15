package fastsync

import "io/fs"

func (f FileInfo) Create(FileInfo) error                { return ErrNotSupportedByPlatform }
func (f FileInfo) SetTimestamps(FileInfo) error         { return ErrNotSupportedByPlatform }
func (f FileInfo) Chmod(FileInfo) error                 { return ErrNotSupportedByPlatform }
func (f *FileInfo) Chown(FileInfo) error                { return ErrNotSupportedByPlatform }
func (f *FileInfo) extractNativeInfo(fs.FileInfo) error { return ErrNotSupportedByPlatform }
