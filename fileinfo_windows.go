package main

import (
	"io/fs"
	"os"
	"syscall"
	"time"
)

func (f FileInfo) Create(remotefi FileInfo) error {
	if remotefi.Mode&fs.ModeDevice != 0 {
		if remotefi.Mode&fs.ModeCharDevice != 0 {
			return ErrNotSupportedByPlatform
		} else {
			// Block device
			return ErrNotSupportedByPlatform
		}
	} else if remotefi.Mode&fs.ModeNamedPipe != 0 {
		return ErrNotSupportedByPlatform
	} else if remotefi.Mode&fs.ModeSocket != 0 {
		return ErrNotSupportedByPlatform
	} else if remotefi.Mode&fs.ModeSymlink != 0 {
		return syscall.Symlink(remotefi.LinkTo, f.Name)
	}
	// regular file
	file, err := os.Create(f.Name)
	file.Close()
	return err
}

func (f FileInfo) SetTimestamps(fi2 FileInfo) error {
	return os.Chtimes(f.Name, time.Unix(fi2.Atim.Unix()), time.Unix(fi2.Mtim.Unix()))
}

func (f FileInfo) Chmod(fi2 FileInfo) error {
	return ErrNotSupportedByPlatform
}

type FILE_ATTRIBUTE_TAG_INFO struct {
	FileAttributes uint32
	ReparseTag     uint32
}

const FileAttributeTagInfo = 9

func (f *FileInfo) Chown(fi2 FileInfo) error {
	return ErrNotSupportedByPlatform
}

func (f *FileInfo) extractNativeInfo(fsfi fs.FileInfo) error {
	native, ok := fsfi.Sys().(*syscall.Win32FileAttributeData)
	if !ok {
		return ErrNotSupportedByPlatform
	}
	// Times
	f.Atim = syscall.NsecToTimespec(native.LastAccessTime.Nanoseconds())
	f.Mtim = syscall.NsecToTimespec(native.LastWriteTime.Nanoseconds())
	f.Ctim = syscall.NsecToTimespec(native.CreationTime.Nanoseconds())

	// Get inode numbers
	var d syscall.ByHandleFileInformation
	file, err := os.Open(f.Name)
	if err != nil {
		return err
	}
	err = syscall.GetFileInformationByHandle(syscall.Handle(file.Fd()), &d)
	if err != nil {
		return err
	}
	f.Nlink = uint64(d.NumberOfLinks)
	f.Dev = uint64(d.VolumeSerialNumber)
	f.Inode = uint64(d.FileIndexHigh)<<32 | uint64(d.FileIndexLow)

	return nil
}
