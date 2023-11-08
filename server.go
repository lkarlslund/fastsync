package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"

	"github.com/cespare/xxhash/v2"
	"github.com/joshlf/go-acl"
	"github.com/lkarlslund/gonk"
)

type filehandleindex struct {
	name string
	fh   *os.File
}

func (fhi filehandleindex) LessThan(fhi2 filehandleindex) bool {
	return fhi.name < fhi2.name
}

type Server struct {
	BasePath string
	ReadOnly bool

	files gonk.Gonk[filehandleindex]
}

type FileInfo struct {
	Name  string
	Mode  fs.FileMode // Go simplified file type, not for chmod
	Size  int64
	IsDir bool

	Permissions uint32
	ACL         acl.ACL

	Owner, Group uint32
	Inode, Nlink uint64
	Dev, Rdev    uint64
	LinkTo       string

	Atim syscall.Timespec
	Mtim syscall.Timespec
	Ctim syscall.Timespec
}

type FileListResponse struct {
	ParentFolder string
	Files        []FileInfo
}

func (s *Server) List(path string, reply *FileListResponse) error {
	logger.Trace().Msgf("Listing files in %s", path)

	var flr FileListResponse
	flr.ParentFolder = path

	entries, err := os.ReadDir(filepath.Join(s.BasePath, path))
	if err != nil {
		return err
	}
	for _, d := range entries {
		absolutepath := filepath.Join(s.BasePath, path, d.Name())
		relativepath := filepath.Join(path, d.Name())
		info, err := d.Info()
		if err != nil {
			return err
		}
		fi, err := s.infoToFileInfo(info, relativepath, absolutepath)
		if err != nil {
			return err
		}
		flr.Files = append(flr.Files, fi)
	}
	*reply = flr
	return nil
}

func (s *Server) Stat(path string, reply *FileInfo) error {
	logger.Trace().Msgf("Stat entry %s", path)

	absolutepath := filepath.Join(s.BasePath, path)
	relativepath := path

	info, err := os.Lstat(absolutepath)
	if err != nil {
		return err
	}
	fi, err := s.infoToFileInfo(info, relativepath, absolutepath)
	*reply = fi
	return err
}

func (s *Server) infoToFileInfo(info os.FileInfo, relativepath, absolutepath string) (FileInfo, error) {
	fi := FileInfo{
		Name:  relativepath,
		Mode:  info.Mode(),
		Size:  info.Size(),
		IsDir: info.IsDir(),
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		fi.Inode = stat.Ino
		fi.Nlink = uint64(stat.Nlink) // force to uint64 for 32-bit systems
		fi.Dev = uint64(stat.Dev)
		fi.Rdev = uint64(stat.Rdev)
		fi.Owner = stat.Uid
		fi.Group = stat.Gid
		fi.Permissions = stat.Mode

		fi.Atim = stat.Atim
		fi.Mtim = stat.Mtim
		fi.Ctim = stat.Ctim

		if fi.Mode&os.ModeSymlink != 0 {
			logger.Debug().Msgf("Detected %v as symlink", fi.Name)
			// Symlink - read link and store in fi variable
			linkto := make([]byte, 65536)
			n, err := syscall.Readlink(absolutepath, linkto)
			if err != nil {
				logger.Error().Msgf("Error reading link to %v: %v", fi.Name, err)
			} else {
				logger.Debug().Msgf("Detected %v as symlink to %v", fi.Name, string(linkto))
			}
			fi.LinkTo = string(linkto[0:n])
		} else if fi.Mode&os.ModeCharDevice != 0 && fi.Mode&os.ModeDevice != 0 {
			logger.Debug().Msgf("Detected %v as character device", fi.Name)
		} else if fi.Mode&os.ModeDir != 0 {
			logger.Debug().Msgf("Detected %v as directory", fi.Name)
		} else if fi.Mode&os.ModeSocket != 0 {
			logger.Debug().Msgf("Detected %v as socket", fi.Name)
		} else if fi.Mode&os.ModeNamedPipe != 0 {
			logger.Debug().Msgf("Detected %v as FIFO", fi.Name)
		} else if fi.Mode&os.ModeDevice != 0 {
			logger.Debug().Msgf("Detected %v as device", fi.Name)
		} else {
			logger.Debug().Msgf("Detected %v as regular file", fi.Name)
		}
	} else {
		return fi, fmt.Errorf("stat failed, I got a %T", info.Sys())
	}
	if info.Mode()&os.ModeSymlink == 0 {
		acl, err := acl.Get(absolutepath)
		if err != nil && err.Error() != "operation not supported" {
			logger.Warn().Msgf("Failed to get ACL for file %v: %v", relativepath, err)
		}
		fi.ACL = acl
	}
	return fi, nil
}

type GetChunkArgs struct {
	Path   string
	Offset uint64
	Size   uint64
}

func (s *Server) Open(path string, reply *interface{}) error {
	logger.Trace().Msgf("Opening file %s", path)
	h, err := os.Open(filepath.Join(s.BasePath, path))
	if err != nil {
		return err
	}
	s.files.Store(
		filehandleindex{
			name: path,
			fh:   h,
		},
	)
	return nil
}

func (s *Server) GetChunk(args GetChunkArgs, data *[]byte) error {
	logger.Trace().Msgf("Getting chunk from file %s at offset %d size %d", args.Path, args.Offset, args.Size)
	fi, found := s.files.Load(filehandleindex{
		name: args.Path,
	})
	if !found {
		return errors.New("file handle not found")
	}
	d := make([]byte, args.Size)
	n, err := fi.fh.ReadAt(d, int64(args.Offset))
	if err != nil {
		return err
	}
	if n != int(args.Size) {
		return errors.New("end of file reached")
	}
	*data = d
	return nil
}

func (s *Server) ChecksumChunk(args GetChunkArgs, checksum *uint64) error {
	logger.Trace().Msgf("Checksumming chunk from file %s at offset %d size %d", args.Path, args.Offset, args.Size)
	fi, found := s.files.Load(filehandleindex{
		name: args.Path,
	})
	if !found {
		return errors.New("file handle not found")
	}
	data := make([]byte, args.Size)
	n, err := fi.fh.ReadAt(data, int64(args.Offset))
	if err != nil {
		return err
	}
	if n != int(args.Size) {
		return errors.New("end of file reached")
	}
	cs := xxhash.Sum64(data)
	*checksum = cs
	return nil
}

func (s *Server) Close(path string, reply *interface{}) error {
	logger.Trace().Msgf("Closing file %s", path)
	fi, found := s.files.Load(filehandleindex{
		name: path,
	})
	if !found {
		return errors.New("file handle not found")
	}
	s.files.Delete(fi)
	fi.fh.Close()
	return nil
}
