//go:build !linux

package fastsync

import "os"

// Do not silently substitute path-based, symlink-following operations on a
// platform without an audited no-follow backend.
func OpenFileNoFollow(string, int, os.FileMode) (*os.File, error) {
	return nil, ErrNotSupportedByPlatform
}
func DirectoryPathNoFollow(string) (string, error)        { return "", ErrNotSupportedByPlatform }
func openNoFollow(string) (*os.File, error)               { return nil, ErrNotSupportedByPlatform }
func lstatNoFollow(string) (os.FileInfo, error)           { return nil, ErrNotSupportedByPlatform }
func pinnedFileInfo(string, bool) (FileInfo, error)       { return FileInfo{}, ErrNotSupportedByPlatform }
func readDirNoFollow(string) ([]os.DirEntry, error)       { return nil, ErrNotSupportedByPlatform }
func mkdirAllNoFollow(string, os.FileMode) error          { return ErrNotSupportedByPlatform }
func createTempNoFollow(string, string) (*os.File, error) { return nil, ErrNotSupportedByPlatform }
func mkdirTempNoFollow(string, string) (string, error)    { return "", ErrNotSupportedByPlatform }
func removeNoFollow(string) error                         { return ErrNotSupportedByPlatform }
func removeAllNoFollow(string) error                      { return ErrNotSupportedByPlatform }
func renameNoFollow(string, string) error                 { return ErrNotSupportedByPlatform }
func linkNoFollow(string, string) error                   { return ErrNotSupportedByPlatform }
func mknodNoFollow(string, uint32, uint64) error          { return ErrNotSupportedByPlatform }
func setXattrNoFollow(string, string, []byte) error       { return ErrNotSupportedByPlatform }
func removeXattrNoFollow(string, string) error            { return ErrNotSupportedByPlatform }
