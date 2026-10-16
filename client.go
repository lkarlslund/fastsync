package fastsync

import (
	"container/list"
	"errors"
	"fmt"
	"net/rpc"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type inodeKey struct {
	dev, inode uint64
}

type inodeinfo struct {
	seedOnce          sync.Once
	bytesCounted      bool // owned by the scheduler dispatcher; warmup never counts bytes
	diskBacked        bool
	seed              *reuseSeed
	seedErr           error
	localhardlinkpath string
	done              chan struct{}
	err               error  // published by closing done
	remaining         uint64 // protected by inodesMu; counts completed paths
	users             int
	idle              *list.Element
}

type dirinfo struct {
	name         string
	info         FileInfo
	extraentries []string // files/folders that are local only, and should be deleted
	remaining    int64
	failed       bool
}

type Client struct {
	ResumeCache, ResumeIdentity string
	ResumePosition              bool
	resume                      *resumeCache
	Password                    string // Local only; never serialized or logged.
	reuseLocks                  [256]sync.Mutex
	warming                     bool // changed only between joined passes
	warmingReferences           bool
	phase                       atomic.Int32
	shutdownRequested           atomic.Bool
	BasePath                    string
	SourcePath                  string
	Include                     []string
	Reference                   []string
	CompletionStamps            bool
	InodeSpillDirectory         string
	InodeMemoryGroups           int

	FlushInterval       time.Duration
	writtenTotal        atomic.Uint64
	lastCheckpoint      time.Time
	lastCheckpointBytes uint64
	checkpointOverride  func() error

	flusher                  *fileFlusher
	FlushBytes               int64
	FlushFiles, FlushWorkers int

	AlwaysChecksum bool
	Durable        bool // Flush each staged file and its parent before reporting completion.

	Options SharedOptions

	Delete                             bool
	MetadataParallel                   int
	metadataGate                       *ioGate
	Pipeline, AutoTune                 bool
	WriteParallel, CachedFiles         int
	BufferBytes                        int64
	writerFiles, streamGate, writeGate *ioGate
	tuningMu                           sync.Mutex
	tuningState                        TransferTuning

	ParallelFile, ParallelDir, QueueSize int
	PreserveHardlinks                    bool
	BlockSize                            int

	fileWorkerWG sync.WaitGroup

	filequeue    chan FileInfo
	inodesMu     sync.Mutex
	inodes       map[inodeKey]*inodeinfo
	localOwners  map[inodeKey]inodeKey // shared destination inode -> source inode
	inodeStore   *inodeStore
	inodeIdle    list.List
	residentDisk int

	dircacheMu sync.Mutex
	dircache   map[string]*dirinfo

	directoryPending atomic.Int64

	errorsMu   sync.Mutex
	errorCount uint64
	firstError error

	queueMu        sync.Mutex
	remoteClient   *rpc.Client
	localIO        atomic.Uint64
	activeIO       atomic.Int64
	diagnosticsMu  sync.Mutex
	diagnostics    BottleneckStatus
	schedulingPeak atomic.Int64
	stageQueued    [3]atomic.Int64
	dependencies   atomic.Int64
	Perf           *performance
}

func NewClient() *Client {
	c := &Client{
		FlushInterval: 30 * time.Second,
		FlushBytes:    256 << 20, FlushFiles: 64, FlushWorkers: 64,
		ParallelFile:     64,
		MetadataParallel: 8,
		WriteParallel:    64, CachedFiles: 64, BufferBytes: 128 * 1024 * 1024,
		QueueSize:         1024,
		ParallelDir:       8,
		PreserveHardlinks: true,
		InodeMemoryGroups: 100000,
		BlockSize:         16 * 1024,
		Options: SharedOptions{
			ProtocolVersion: PROTOCOLVERSION,
			BehaviorVersion: BEHAVIORVERSION,
		},
		inodes:   make(map[inodeKey]*inodeinfo),
		dircache: make(map[string]*dirinfo),
		Perf:     NewPerformance(),
	}

	return c
}

func (c *Client) Run(client *rpc.Client) (runErr error) {
	if err := c.validateQueueSettings(); err != nil {
		return err
	}

	if err := c.initPipeline(); err != nil {
		return err
	}

	c.errorsMu.Lock()
	c.errorCount, c.firstError = 0, nil
	c.errorsMu.Unlock()

	c.queueMu.Lock()
	c.filequeue = make(chan FileInfo, c.QueueSize)
	c.queueMu.Unlock()
	c.remoteClient = client
	c.schedulingPeak.Store(0)
	c.dependencies.Store(0)
	c.inodesMu.Lock()
	c.inodes = make(map[inodeKey]*inodeinfo)
	c.localOwners = make(map[inodeKey]inodeKey)
	c.inodeIdle.Init()
	c.residentDisk = 0
	c.inodesMu.Unlock()
	c.dircacheMu.Lock()
	c.dircache = make(map[string]*dirinfo)
	c.dircacheMu.Unlock()

	err := c.hello(client)
	if err != nil {
		return err
	}

	stopTuning, err := c.startTuning(client)
	if err != nil {
		return err
	}
	defer func() {
		c.phase.Store(4)
		Logger.Info().Msg("Flushing remaining file data; waiting for pending writes")
		runErr = errors.Join(runErr, stopTuning())
		Logger.Info().Msg("File data flush finished")
	}()

	stopDiagnostics := c.startDiagnostics(client)
	defer stopDiagnostics()

	// Check that remote path exists and we can connect to server

	var rootdirinfo FileInfo
	err = client.Call("Server.Stat", "/", &rootdirinfo)
	if err != nil {
		return err
	}
	if !rootdirinfo.IsDir {
		return fmt.Errorf("source root is not a directory")
	}
	if st, err := lstatNoFollow(c.BasePath); err != nil || !st.IsDir() {
		return fmt.Errorf("destination must be an existing directory: %s", c.BasePath)
	}
	if c.InodeSpillDirectory != "" {
		if err := c.openInodeSpill(); err != nil {
			return err
		}
		defer func() {
			c.inodesMu.Lock()
			store := c.inodeStore
			c.inodeStore = nil
			c.inodesMu.Unlock()
			runErr = errors.Join(runErr, store.close())
		}()
	}
	cache, loaded, cacheErr := c.openResumeCache(rootdirinfo)
	if cacheErr != nil {
		return cacheErr
	}
	if cache != nil {
		defer func() {
			c.phase.Store(5)
			Logger.Info().Msg("Saving and flushing resume cache")
			cache.Close()
			c.resume = nil
			Logger.Info().Msg("Resume cache saved")
		}()
	}
	if c.PreserveHardlinks && !loaded {
		c.phase.Store(1)
		c.warming = true
		Logger.Info().Msg("Pass 1: checking existing paths and warming hardlink cache")
		c.warmExistingPass(client, rootdirinfo)
		c.warming = false
		if len(c.Reference) > 0 && c.runError() != nil {
			return c.runError()
		}
		if cache != nil {
			cache.publish(c.runError() == nil)
		}
		// Warmup failures remain in the final error state, but must not prevent
		// the second walk from copying accessible paths or retrying a listing.
		if err := c.runError(); err != nil {
			Logger.Warn().Err(err).Msg("Existing-file scan had errors; continuing with link/copy pass")
		}
		c.queueMu.Lock()
		c.filequeue = make(chan FileInfo, c.QueueSize)
		c.queueMu.Unlock()
	}
	c.phase.Store(2)
	Logger.Info().Msg("Pass 2: reusing existing inodes, then copying missing data")
	c.directoryPending.Store(0)
	c.fileWorkerWG.Add(1)
	go func() { defer c.fileWorkerWG.Done(); c.scheduleFiles(client) }()
	c.walkAlphabetical(client, rootdirinfo)

	// close the file queue so file workers can finish
	close(c.filequeue)
	// wait for all workers to finish
	c.fileWorkerWG.Wait()

	Logger.Debug().Msg("Client routine done")

	return c.runError()
}

func (c *Client) validateQueueSettings() error {
	if err := c.validateIncludes(); err != nil {
		return err
	}
	if c.InodeSpillDirectory != "" && (!c.PreserveHardlinks || c.ResumeCache != "" || c.InodeMemoryGroups < 1) {
		return fmt.Errorf("inode spill requires hardlinks, a positive memory-group limit, and no resume-cache")
	}
	if c.CompletionStamps && c.FlushInterval <= 0 {
		return fmt.Errorf("completion-stamps requires background file flushing")
	}
	if c.CompletionStamps && !completionStampsSupported {
		return fmt.Errorf("completion-stamps is only supported on Linux")
	}
	if c.ParallelDir < 1 {
		return fmt.Errorf("ParallelDir must be at least 1, got %d", c.ParallelDir)
	}
	if c.ParallelFile < 1 {
		return fmt.Errorf("ParallelFile must be at least 1, got %d", c.ParallelFile)
	}
	if c.QueueSize < 0 {
		return fmt.Errorf("QueueSize must be non-negative, got %d", c.QueueSize)
	}
	if c.BlockSize > 16*1024*1024 {
		return fmt.Errorf("BlockSize must not exceed 16 MiB")
	}
	if c.BlockSize < 1 {
		return fmt.Errorf("BlockSize must be at least 1, got %d", c.BlockSize)
	}
	return nil
}

func (c *Client) ProcessedItemInDir(path string) {
	c.processedItemInDir(path, false)
}

func (c *Client) processedItemInDir(path string, failed bool) {
	if c.warming {
		return
	}
	donewithdirectory := false
	founddirectory := false
	var doneitem dirinfo
	c.dircacheMu.Lock()
	if item, found := c.dircache[path]; found {
		founddirectory = true
		item.failed = item.failed || failed
		item.remaining--
		left := item.remaining
		Logger.Trace().Msgf("directory %s has usage %v left", item.name, left)
		if left <= 0 { // zero for folders with contents, -1 for blank folders
			doneitem = *item
			delete(c.dircache, path)
			donewithdirectory = true // delete operation must be outside this atomic operation
		}
	}
	c.dircacheMu.Unlock()
	if !founddirectory {
		c.recordError("Failed to find directory info for postprocessing %s", path)
	}
	if donewithdirectory {
		ok := c.postProcessDir(&doneitem)
		if path != "/" {
			c.processedItemInDir(filepath.Dir(path), !ok)
		}
	}
}

func (c *Client) PostProcessDir(item *dirinfo) {
	c.postProcessDir(item)
}

func (c *Client) postProcessDir(item *dirinfo) bool {
	if item.failed || (!c.CompletionStamps && c.runError() != nil) {
		return false
	}
	if err := c.checkDirectory(c.remoteClient, item.info); err != nil {
		c.recordError("%s: %v", item.name, err)
		return false
	}
	if c.Delete {
		for _, extraentry := range item.extraentries {
			err := removeAllNoFollow(filepath.Join(c.BasePath, item.name, extraentry))
			if err != nil {
				c.recordError("Error unlinking %v: %v", filepath.Join(c.BasePath, item.name, extraentry), err)
				return false
			}
			if err == nil {
				c.Perf.Add(EntriesDeleted, 1)
			}
		}
	}
	if c.CompletionStamps && topLevelDirectory(item.info) {
		if err := c.flushCompletionDirectory(item); err != nil {
			c.recordError("flush completed directory %s: %v", item.name, err)
			return false
		}
	}

	// Apply modify times to directory
	localdirfi, err := c.localFileInfo(filepath.Join(c.BasePath, item.name))
	if err != nil {
		c.recordError("Problem getting local directory information for %v: %v", filepath.Join(c.BasePath, item.name), err)
		return false
	} else {
		if err := c.applyMetadata(localdirfi, item.info); err != nil {
			c.recordError("Problem applying directory metadata for %v: %v", filepath.Join(c.BasePath, item.name), err)
			return false
		}
	}
	if c.CompletionStamps && topLevelDirectory(item.info) {
		if err := syncCompletionStamp(filepath.Join(c.BasePath, item.name)); err != nil {
			_ = c.invalidateCompletionStamp(item.info)
			c.recordError("persist completion stamp %s: %v", item.name, err)
			return false
		}
		Logger.Info().Str("path", item.name).Msg("Directory durably complete")
	}
	c.checkpointResumeDirectory(item)
	return true
}

func (c *Client) Stats() (inodes, directories, filequeue, directoriestack int) {
	c.queueMu.Lock()
	defer c.queueMu.Unlock()
	c.inodesMu.Lock()
	inodes = len(c.inodes)
	if c.inodeStore != nil {
		inodes += c.inodeStore.keys - c.residentDisk
	}
	c.inodesMu.Unlock()

	c.dircacheMu.Lock()
	directories = len(c.dircache)
	c.dircacheMu.Unlock()

	directoriestack = int(c.directoryPending.Load())
	return inodes, directories, len(c.filequeue), directoriestack
}

func (c *Client) recordError(format string, args ...any) {
	err := fmt.Errorf(format, args...)
	c.errorsMu.Lock()
	c.errorCount++
	if c.firstError == nil {
		c.firstError = err
	}
	c.errorsMu.Unlock()
	Logger.Error().Err(err).Msg("Sync failed")
}
func (c *Client) runError() error {
	c.errorsMu.Lock()
	defer c.errorsMu.Unlock()
	if c.errorCount == 0 {
		return nil
	}
	return fmt.Errorf("sync failed with %d error(s); first: %w", c.errorCount, c.firstError)
}

func (c *Client) hello(client *rpc.Client) error { return c.Handshake(client) }

// NotifyShutdown updates local presentation while Run finishes cleanup.
func (c *Client) NotifyShutdown() { c.shutdownRequested.Store(true) }
