package fastsync

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/lkarlslund/gonk"
)

var ErrPleaseSayHello = errors.New("Client needs to say hello")
var ErrPleaseSayHelloOnce = errors.New("Client needs to say hello just once")

type filehandleindex struct {
	name string
	fh   *os.File
}

func (fhi filehandleindex) Compare(fhi2 filehandleindex) int {
	return strings.Compare(fhi.name, fhi2.name)
}

func NewServer() *Server {
	return &Server{
		BasePath: ".",
		ReadOnly: true,
		shutdown: make(chan struct{}),
		Perf:     NewPerformance(),
	}
}

type Server struct {
	BasePath string

	Options SharedOptions

	ReadOnly bool

	clientsaidhello bool
	shutdown        chan struct{}
	files           gonk.Gonk[filehandleindex]

	Perf *performance
}

func (s *Server) Hello(options SharedOptions, reply *any) error {
	//	if s.clientsaidhello {
	//		return ErrPleaseSayHelloOnce
	//	}
	s.Options = options
	s.clientsaidhello = true
	return nil
}

func (s *Server) Shutdown(input any, reply *any) error {
	if !s.clientsaidhello {
		return ErrPleaseSayHello
	}
	Logger.Info().Msg("Shutting down server")
	if len(s.shutdown) == 0 {
		s.shutdown <- struct{}{}
	}
	return nil
}

func (s *Server) List(path string, reply *FileListResponse) error {
	if !s.clientsaidhello {
		return ErrPleaseSayHello
	}
	Logger.Trace().Msgf("Listing files in %s", path)

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
		if !s.Options.SendXattr {
			fi.Xattrs = nil
		}
		flr.Files = append(flr.Files, fi)
	}
	*reply = flr
	return nil
}

func (s *Server) Stat(path string, reply *FileInfo) error {
	if !s.clientsaidhello {
		return ErrPleaseSayHello
	}
	Logger.Trace().Msgf("Stat entry %s", path)

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

func (s *Server) Open(path string, reply *interface{}) error {
	if !s.clientsaidhello {
		return ErrPleaseSayHello
	}
	Logger.Trace().Msgf("Opening file %s", path)
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
	if !s.clientsaidhello {
		return ErrPleaseSayHello
	}
	Logger.Trace().Msgf("Getting chunk from file %s at offset %d size %d", args.Path, args.Offset, args.Size)
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
	if !s.clientsaidhello {
		return ErrPleaseSayHello
	}
	Logger.Trace().Msgf("Checksumming chunk from file %s at offset %d size %d", args.Path, args.Offset, args.Size)
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
	if !s.clientsaidhello {
		return ErrPleaseSayHello
	}
	Logger.Trace().Msgf("Closing file %s", path)
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
