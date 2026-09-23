package fastsync

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
	passwordKey                  []byte
	authNonce                    []byte
	authAttempted, authenticated bool
	BasePath                     string
	AutoTune                     bool
	readGate                     *ioGate
	tuning                       *tuningCoordinator

	metadataGate *ioGate

	Options SharedOptions

	ReadOnly bool

	clientsaidhello atomic.Bool
	helloMu         sync.Mutex
	shutdown        chan struct{}
	filesMu         sync.Mutex
	files           map[string]*os.File
	root            *os.File

	host     hostSampler
	localIO  atomic.Uint64
	activeIO atomic.Int64
	Perf     *performance
}

// PinRoot keeps the source filesystem mounted while this server is running.
func (s *Server) PinRoot() error {
	if s.root != nil {
		return errors.New("source root is already pinned")
	}
	base, err := DirectoryPathNoFollow(s.BasePath)
	if err != nil {
		return err
	}
	root, err := openNoFollow(base)
	if err != nil {
		return err
	}
	info, err := root.Stat()
	if err != nil || !info.IsDir() {
		_ = root.Close()
		if err != nil {
			return err
		}
		return errors.New("source root is not a directory")
	}
	s.BasePath = base
	s.root = root
	return nil
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

// SelectRoot scopes this connection before Hello. Older servers reject this RPC.
func (s *Server) SelectRoot(path string, reply *any) error {
	s.helloMu.Lock()
	defer s.helloMu.Unlock()
	if s.clientsaidhello.Load() {
		return ErrPleaseSayHelloOnce
	}
	if !s.authenticated {
		return ErrAuthentication
	}
	if !filepath.IsLocal(path) {
		return ErrInvalidPath
	}
	base, err := DirectoryPathNoFollow(s.BasePath)
	if err != nil {
		return err
	}
	selected, err := DirectoryPathNoFollow(filepath.Join(base, path))
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(base, selected)
	if err != nil || !filepath.IsLocal(rel) {
		return ErrInvalidPath
	}
	root, err := openNoFollow(selected)
	if err != nil {
		return err
	}
	info, err := root.Stat()
	if err != nil || !info.IsDir() {
		_ = root.Close()
		if err != nil {
			return err
		}
		return errors.New("selected source is not a directory")
	}
	if s.root != nil {
		_ = s.root.Close()
	}
	s.BasePath = selected
	s.root = root
	return nil
}

func (s *Server) Hello(options SharedOptions, reply *VersionInfo) error {
	s.helloMu.Lock()
	defer s.helloMu.Unlock()
	if s.clientsaidhello.Load() {
		return ErrPleaseSayHelloOnce
	}

	if !s.authenticated {
		return ErrAuthentication
	}
	if err := (VersionInfo{options.ProtocolVersion, options.BehaviorVersion}).check(); err != nil {
		return err
	}
	if reply != nil {
		*reply = CurrentVersions()
	}

	s.Options = options
	s.clientsaidhello.Store(true)
	return nil
}

func (s *Server) Shutdown(input any, reply *any) error {
	if !s.clientsaidhello.Load() {
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
	if s.metadataGate != nil {
		release := s.metadataGate.acquire()
		defer release(0, 0)
	}

	defer s.trackIO()()
	if !s.clientsaidhello.Load() {
		return ErrPleaseSayHello
	}
	Logger.Trace().Msgf("Listing files in %s", path)

	var flr FileListResponse
	flr.ParentDirectory = path

	localpath, err := s.localPath(path)
	if err != nil {
		return err
	}
	entries, err := readDirNoFollow(localpath)
	if err != nil {
		return err
	}
	for _, d := range entries {
		absolutepath := filepath.Join(localpath, d.Name())
		relativepath := filepath.Join(path, d.Name())
		fi, err := pathToFileInfo(absolutepath, s.Options.SendXattr)
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
	if s.metadataGate != nil {
		release := s.metadataGate.acquire()
		defer release(0, 0)
	}

	defer s.trackIO()()
	if !s.clientsaidhello.Load() {
		return ErrPleaseSayHello
	}
	Logger.Trace().Msgf("Stat entry %s", path)

	absolutepath, err := s.localPath(path)
	if err != nil {
		return err
	}
	relativepath := path

	info, err := lstatNoFollow(absolutepath)
	if err != nil {
		return err
	}
	fi, err := infoToFileInfo(info, absolutepath, s.Options.SendXattr)
	// Override path to only send the relative path
	fi.Name = relativepath
	*reply = fi
	return err
}

func (s *Server) Open(path string, reply *interface{}) error {
	if s.metadataGate != nil {
		release := s.metadataGate.acquire()
		defer release(0, 0)
	}

	defer s.trackIO()()
	if !s.clientsaidhello.Load() {
		return ErrPleaseSayHello
	}
	Logger.Trace().Msgf("Opening file %s", path)
	localpath, err := s.localPath(path)
	if err != nil {
		return err
	}
	h, err := openNoFollow(localpath)
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
	if !s.clientsaidhello.Load() {
		return ErrPleaseSayHello
	}
	if s.readGate != nil {
		release := s.readGate.acquire()
		started := time.Now()
		defer func() { release(args.Size, time.Since(started)) }()
	}

	Logger.Trace().Msgf("Getting chunk from file %s at offset %d size %d", args.Path, args.Offset, args.Size)
	s.filesMu.Lock()
	fh, found := s.files[args.Path]
	s.filesMu.Unlock()
	if !found {
		return errors.New("file handle not found")
	}
	if args.Size > 16*1024*1024 || args.Offset > (1<<63-1)-args.Size {
		return fmt.Errorf("chunk size too large: %d", args.Size)
	}
	d := make([]byte, int(args.Size))
	var n int
	err := timedIO(&s.localIO, &s.activeIO, func() error { var e error; n, e = fh.ReadAt(d, int64(args.Offset)); return e })
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
	if !s.clientsaidhello.Load() {
		return ErrPleaseSayHello
	}
	if s.readGate != nil {
		release := s.readGate.acquire()
		started := time.Now()
		defer func() { release(args.Size, time.Since(started)) }()
	}

	Logger.Trace().Msgf("Checksumming chunk from file %s at offset %d size %d", args.Path, args.Offset, args.Size)
	s.filesMu.Lock()
	fh, found := s.files[args.Path]
	s.filesMu.Unlock()
	if !found {
		return errors.New("file handle not found")
	}
	if args.Size > 16*1024*1024 || args.Offset > (1<<63-1)-args.Size {
		return fmt.Errorf("chunk size too large: %d", args.Size)
	}
	data := make([]byte, int(args.Size))
	var n int
	err := timedIO(&s.localIO, &s.activeIO, func() error { var e error; n, e = fh.ReadAt(data, int64(args.Offset)); return e })
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
	if s.metadataGate != nil {
		release := s.metadataGate.acquire()
		defer release(0, 0)
	}

	if !s.clientsaidhello.Load() {
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

// Hash provides independent full-file verification, without exposing a writable RPC.
func (s *Server) Hash(path string, reply *string) error {
	if !s.clientsaidhello.Load() {
		return ErrPleaseSayHello
	}
	local, err := s.localPath(path)
	if err != nil {
		return err
	}
	info, err := lstatNoFollow(local)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("hash requires a regular file")
	}
	digest, err := hashFile(local)
	if err == nil {
		*reply = digest
	}
	return err
}

// Each connection owns its handles and negotiated options.
func (s *Server) NewSession() *Server {
	session := NewServer()
	session.passwordKey = append([]byte(nil), s.passwordKey...)
	session.metadataGate = s.metadataGate
	session.AutoTune, session.readGate, session.tuning = s.AutoTune, s.readGate, s.tuning
	session.BasePath, session.Perf, session.shutdown = s.BasePath, s.Perf, s.shutdown
	return session
}
func (s *Server) CloseFiles() {
	if s.tuning != nil {
		s.tuning.mu.Lock()
		if s.tuning.owner == s {
			s.tuning.owner = nil
			s.tuning.turn = 0
		}
		s.tuning.mu.Unlock()
	}

	s.filesMu.Lock()
	defer s.filesMu.Unlock()
	for path, file := range s.files {
		_ = file.Close()
		delete(s.files, path)
	}
	if s.root != nil {
		_ = s.root.Close()
		s.root = nil
	}
}

func (s *Server) trackIO() func() {
	started := time.Now()
	s.activeIO.Add(1)
	return func() { s.localIO.Add(uint64(time.Since(started))); s.activeIO.Add(-1) }
}
