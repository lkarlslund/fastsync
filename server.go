package main

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/cespare/xxhash/v2"
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

	shutdown chan struct{}
	files    gonk.Gonk[filehandleindex]
}

type FileListResponse struct {
	ParentDirectory string
	Files           []FileInfo
}

func (s *Server) Shutdown(input any, reply *any) error {
	logger.Info().Msg("Shutting down server")
	if len(s.shutdown) == 0 {
		s.shutdown <- struct{}{}
	}
	return nil
}

func (s *Server) List(path string, reply *FileListResponse) error {
	logger.Trace().Msgf("Listing files in %s", path)

	var flr FileListResponse
	flr.ParentDirectory = path

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
		fi, err := InfoToFileInfo(info, absolutepath)
		if err != nil {
			return err
		}
		// Override path to only send the relative path
		fi.Name = relativepath
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
	fi, err := InfoToFileInfo(info, absolutepath)
	// Override path to only send the relative path
	fi.Name = relativepath
	*reply = fi
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

func (s *Server) Wait() {
	<-s.shutdown
}
