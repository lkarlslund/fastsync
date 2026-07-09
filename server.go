package fastsync

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/cespare/xxhash/v2"
)

var ErrPleaseSayHello = errors.New("Client needs to say hello")
var ErrPleaseSayHelloOnce = errors.New("Client needs to say hello just once")
var ErrInvalidPath = errors.New("invalid path")

func NewServer() *Server {
	return &Server{
		BasePath: ".",
		ReadOnly: true,
		shutdown: make(chan struct{}, 1),
		files:    make(map[string]*os.File),
		Perf:     NewPerformance(),
	}
}

type Server struct {
	BasePath string

	Options SharedOptions

	ReadOnly bool

	clientsaidhello bool
	shutdown        chan struct{}
	filesMu         sync.Mutex
	files           map[string]*os.File

	Perf *performance
}

func (s *Server) localPath(path string) (string, error) {
	rel := filepath.Clean(path)
	if rel == "." || rel == string(filepath.Separator) {
		return s.BasePath, nil
	}
	rel = strings.TrimLeft(rel, `/\`)
	if rel == "" {
		return s.BasePath, nil
	}
	if filepath.VolumeName(rel) != "" || !filepath.IsLocal(rel) {
		return "", fmt.Errorf("%w: %q", ErrInvalidPath, path)
	}
	return filepath.Join(s.BasePath, rel), nil
}

func (s *Server) Hello(options SharedOptions, reply *any) error {
	//	if s.clientsaidhello {
	//		return ErrPleaseSayHelloOnce
	//	}
	if options.ProtocolVersion != PROTOCOLVERSION {
		return fmt.Errorf("Server expects protocol version %v, but client is running %v, please use same binary version for transfers", PROTOCOLVERSION, options.ProtocolVersion)
	}
	s.Options = options
	s.clientsaidhello = true
	return nil
}

func (s *Server) Shutdown(input any, reply *any) error {
	if !s.clientsaidhello {
		return ErrPleaseSayHello
	}
	Logger.Info().Msg("Shutting down server")
	select {
	case s.shutdown <- struct{}{}:
	default:
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

	localpath, err := s.localPath(path)
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(localpath)
	if err != nil {
		return err
	}
	for _, d := range entries {
		absolutepath := filepath.Join(localpath, d.Name())
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

	absolutepath, err := s.localPath(path)
	if err != nil {
		return err
	}
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
	localpath, err := s.localPath(path)
	if err != nil {
		return err
	}
	h, err := os.Open(localpath)
	if err != nil {
		return err
	}
	s.filesMu.Lock()
	if s.files == nil {
		s.files = make(map[string]*os.File)
	}
	old := s.files[path]
	s.files[path] = h
	s.filesMu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	return nil
}

func (s *Server) GetChunk(args GetChunkArgs, data *[]byte) error {
	if !s.clientsaidhello {
		return ErrPleaseSayHello
	}
	Logger.Trace().Msgf("Getting chunk from file %s at offset %d size %d", args.Path, args.Offset, args.Size)
	s.filesMu.Lock()
	fh, found := s.files[args.Path]
	s.filesMu.Unlock()
	if !found {
		return errors.New("file handle not found")
	}
	if args.Size > uint64(int(^uint(0)>>1)) {
		return fmt.Errorf("chunk size too large: %d", args.Size)
	}
	d := make([]byte, int(args.Size))
	n, err := fh.ReadAt(d, int64(args.Offset))
	if err != nil {
		return err
	}
	if n != int(args.Size) {
		return io.ErrUnexpectedEOF
	}
	*data = d
	return nil
}

func (s *Server) ChecksumChunk(args GetChunkArgs, checksum *uint64) error {
	if !s.clientsaidhello {
		return ErrPleaseSayHello
	}
	Logger.Trace().Msgf("Checksumming chunk from file %s at offset %d size %d", args.Path, args.Offset, args.Size)
	s.filesMu.Lock()
	fh, found := s.files[args.Path]
	s.filesMu.Unlock()
	if !found {
		return errors.New("file handle not found")
	}
	if args.Size > uint64(int(^uint(0)>>1)) {
		return fmt.Errorf("chunk size too large: %d", args.Size)
	}
	data := make([]byte, int(args.Size))
	n, err := fh.ReadAt(data, int64(args.Offset))
	if err != nil {
		return err
	}
	if n != int(args.Size) {
		return io.ErrUnexpectedEOF
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
	s.filesMu.Lock()
	fh, found := s.files[path]
	if !found {
		s.filesMu.Unlock()
		return errors.New("file handle not found")
	}
	delete(s.files, path)
	s.filesMu.Unlock()
	return fh.Close()
}

func (s *Server) Wait() {
	<-s.shutdown
}
