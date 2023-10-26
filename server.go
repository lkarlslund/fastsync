package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/lkarlslund/gonk"
)

type QueueItem struct {
	Command  string
	Path     string
	FileInfo *FileInfo
}

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
	Mode  fs.FileMode
	Size  int64
	IsDir bool

	Owner, Group                       uint32
	Inode, Nlink                       uint64
	Dev, Rdev                          uint64
	ModifyTime, AccessTime, CreateTime time.Time
	LinkTo                             string
}

type FileListResponse struct {
	Files []FileInfo
}

func (s *Server) Listfiles(path string, reply *FileListResponse) error {
	logger.Trace().Msgf("Listing files in %s", path)
	var flr FileListResponse
	entries, err := os.ReadDir(filepath.Join(s.BasePath, path))
	if err != nil {
		return err
	}
	for _, d := range entries {
		info, err := d.Info()
		if err != nil {
			return err
		}
		logger.Trace().Msgf("Found file %s", d.Name())
		fi := FileInfo{
			Name:       filepath.Join(path, d.Name()),
			Mode:       info.Mode(),
			Size:       info.Size(),
			IsDir:      info.IsDir(),
			ModifyTime: info.ModTime(),
		}
		if stat, ok := info.Sys().(*syscall.Stat_t); ok {
			fi.Inode = stat.Ino
			fi.Nlink = stat.Nlink
			fi.Dev = stat.Dev
			fi.Rdev = stat.Rdev
			fi.Owner = stat.Uid
			fi.Group = stat.Gid

			fi.AccessTime = time.Unix(stat.Atim.Sec, stat.Atim.Nsec)
			fi.CreateTime = time.Unix(stat.Ctim.Sec, stat.Ctim.Nsec)
			fi.ModifyTime = time.Unix(stat.Mtim.Sec, stat.Mtim.Nsec)

			switch stat.Mode {
			case syscall.S_IFBLK:
				logger.Debug().Msgf("Detected %v as block device", fi.Name)
				var linkto []byte
				_, err := syscall.Readlink(fi.Name, linkto)
				if err != nil {
					logger.Error().Msgf("Error reading link to %v: %v", fi.Name, err)
				}
				fi.LinkTo = string(linkto)
			case syscall.S_IFCHR:
				logger.Debug().Msgf("Detected %v as character device", fi.Name)

			case syscall.S_IFDIR:
				// Directory - no special handling
			case syscall.S_IFIFO:
				logger.Debug().Msgf("Detected %v as FIFO device", fi.Name)
			case syscall.S_IFLNK:
				// Symlink - read link and store in fi variable

			case syscall.S_IFMT:
				logger.Debug().Msgf("Detected %v as mount point", fi.Name)
			case syscall.S_IFREG:
				// Regular file - no special handling
			case syscall.S_IFSOCK:
				// Unsupported
				logger.Debug().Msgf("Detected %v as SOCKET device", fi.Name)
			}
		} else {
			return fmt.Errorf("Stat failed, I got a %T", info.Sys())
		}
		flr.Files = append(flr.Files, fi)
	}
	*reply = flr
	return err
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
		return errors.New("File handle not found")
	}
	d := make([]byte, args.Size)
	n, err := fi.fh.ReadAt(d, int64(args.Offset))
	if err != nil {
		return err
	}
	if n != int(args.Size) {
		return errors.New("End of file reached")
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
		return errors.New("File handle not found")
	}
	data := make([]byte, args.Size)
	n, err := fi.fh.ReadAt(data, int64(args.Offset))
	if err != nil {
		return err
	}
	if n != int(args.Size) {
		return errors.New("End of file reached")
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
		return errors.New("File handle not found")
	}
	s.files.Delete(fi)
	fi.fh.Close()
	return nil
}
