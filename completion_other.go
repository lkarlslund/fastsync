//go:build !linux

package fastsync

const completionStampsSupported = false

func syncCompletionFilesystem(path string) error {
	return ErrNotSupportedByPlatform
}
