package fastsync

import (
	"errors"
	"fmt"
	"net/rpc"
	"sync"
	"time"
)

// ioGate bounds active operations. Shrinking never cancels an operation in flight.
type ioGate struct {
	mu                            sync.Mutex
	cond                          *sync.Cond
	limit, maximum, active, peak  int
	bytes                         uint64
	nanos                         uint64
	calls                         uint64
	started                       time.Time
	previousRate, previousLatency float64
	previousLimit                 int
	trial                         bool
	direction                     int
	reason                        string
}

func newIOGate(maximum int, adaptive bool) *ioGate {
	g := &ioGate{limit: maximum, maximum: maximum, started: time.Now(), direction: 1, reason: "manual"}
	if adaptive {
		g.limit = min(8, maximum)
		g.reason = "warming up"
	}
	g.cond = sync.NewCond(&g.mu)
	return g
}
func (g *ioGate) acquire() func(uint64, time.Duration) {
	g.mu.Lock()
	for g.active >= g.limit {
		g.cond.Wait()
	}
	g.active++
	g.peak = max(g.peak, g.active)
	g.mu.Unlock()
	return func(bytes uint64, duration time.Duration) {
		g.mu.Lock()
		g.active--
		g.bytes += bytes
		g.nanos += uint64(duration)
		g.calls++
		g.cond.Broadcast()
		g.mu.Unlock()
	}
}
func (g *ioGate) status() (int, int, string) {
	if g == nil {
		return 0, 0, "disabled"
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.limit, g.active, g.reason
}

// step seeks the smallest concurrency within 10% of observed throughput.
// Windows without enough work never cause an increase.
func (g *ioGate) step(now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	elapsed := now.Sub(g.started).Seconds()
	if elapsed < 20 {
		return
	}
	rate := float64(g.bytes) / elapsed
	latency := float64(g.nanos) / float64(max(g.calls, 1))
	busy := g.peak >= g.limit && g.calls >= 16 && g.bytes >= 1<<20
	g.started = now
	g.bytes = 0
	g.nanos = 0
	g.calls = 0
	g.peak = g.active
	if !busy {
		if g.trial {
			g.limit = g.previousLimit
			g.trial = false
			g.direction = -g.direction
			g.cond.Broadcast()
		}
		g.reason = "insufficient sustained demand"
		g.trial = false
		return
	}
	if g.trial {
		keep := rate >= g.previousRate*0.90
		if g.limit > g.previousLimit {
			keep = rate > g.previousRate*1.10 && latency < g.previousLatency*1.50
		}
		if !keep {
			g.limit = g.previousLimit
			g.reason = "reverted unhelpful probe"
		} else {
			g.reason = "kept useful probe"
		}
		g.trial = false
		g.direction = -g.direction
		g.cond.Broadcast()
		return
	}
	next := g.limit
	if g.direction > 0 {
		next = min(g.maximum, max(g.limit+1, g.limit*2))
	} else {
		next = max(1, g.limit/2)
	}
	if next == g.limit {
		g.direction = -g.direction
		g.reason = "at configured bound"
		return
	}
	g.previousRate = rate
	g.previousLatency = latency
	g.previousLimit = g.limit
	g.limit = next
	g.trial = true
	g.reason = fmt.Sprintf("testing %d operations", next)
	g.cond.Broadcast()
}

type tuningCoordinator struct {
	mu    sync.Mutex
	owner *Server
	seen  time.Time
	turn  uint64
}
type TuneReply struct {
	ClientTurn  bool
	ResetClient bool
	ReadLimit   int
	ReadActive  int
	Reason      string
}

// TuneIO coordinates alternating source/client probes. Only one connection may
// drive the server-wide controller; other clients still share its read limit.
func (s *Server) TuneIO(_ struct{}, reply *TuneReply) error {
	if !s.clientsaidhello.Load() {
		return ErrPleaseSayHello
	}
	if s.tuning == nil || s.readGate == nil {
		return fmt.Errorf("server tuning is disabled")
	}
	s.tuning.mu.Lock()
	defer s.tuning.mu.Unlock()
	now := time.Now()
	if s.tuning.owner == nil || now.Sub(s.tuning.seen) > 90*time.Second {
		s.tuning.owner = s
	}
	if s.tuning.owner == s {
		s.tuning.seen = now

		phase := s.tuning.turn % 6
		s.tuning.turn++
		switch phase {
		case 0:
			s.readGate.resetWindow(now)
		case 1, 2:
			if s.AutoTune {
				s.readGate.step(now)
			}
		case 3:
			reply.ResetClient = true
		case 4, 5:
			reply.ClientTurn = true
		}

	}
	reply.ReadLimit, reply.ReadActive, reply.Reason = s.readGate.status()
	return nil
}
func (s *Server) ConfigureIO(reads int, adaptive bool) error {
	if reads < 1 || reads > 1024 {
		return fmt.Errorf("read-parallel must be between 1 and 1024")
	}
	s.AutoTune = adaptive
	s.readGate = newIOGate(reads, adaptive)
	s.tuning = &tuningCoordinator{}
	return nil
}

type TransferTuning struct {
	Phase                                          int32
	CheckQueue, CopyQueue, LinkQueue, Dependencies int64
	PendingFlushBytes                              uint64
	PendingFlushFiles                              int
	LastFlush                                      time.Duration
	FlushCount, FlushedBytes                       uint64
	FlushedBytesPerSecond                          float64

	WriteLimit, ActiveWrites, ActiveFiles, FileLimit int
	BufferReserved, BufferLimit                      int64
	ReadLimit                                        int
	Reason                                           string
}

func (c *Client) Tuning() TransferTuning {
	c.flushState()
	c.tuningMu.Lock()
	defer c.tuningMu.Unlock()
	state := c.tuningState
	state.Phase = c.phase.Load()
	state.CheckQueue = c.stageQueued[0].Load()
	state.CopyQueue = c.stageQueued[1].Load()
	state.LinkQueue = c.stageQueued[2].Load()
	state.Dependencies = c.dependencies.Load()
	state.WriteLimit, state.ActiveWrites, state.Reason = c.writeGate.status()
	if c.streamGate != nil {
		_, state.ActiveFiles, _ = c.streamGate.status()
		state.FileLimit = c.streamGate.maximum
	}
	state.BufferReserved = int64(state.ActiveFiles) * int64(c.BlockSize) * 4
	state.BufferLimit = c.BufferBytes
	return state
}
func (c *Client) initPipeline() error {
	c.tuningMu.Lock()
	defer c.tuningMu.Unlock()
	if c.AutoTune {
		c.Pipeline = true
		if c.FlushInterval < 20*time.Second {
			return fmt.Errorf("autotune flush-interval must be at least 20s")
		}
	}
	if c.FlushInterval < 0 {
		return fmt.Errorf("flush-interval must not be negative")
	}
	if c.FlushInterval > 0 {
		if c.FlushBytes < int64(c.BlockSize) || c.FlushFiles < 1 || c.FlushFiles > 4096 || c.FlushWorkers < 1 || c.FlushWorkers > 128 {
			return fmt.Errorf("invalid background flush limits")
		}
	}
	c.flusher = nil
	c.metadataGate = nil
	c.streamGate = nil
	c.writerFiles = nil
	c.writtenTotal.Store(0)
	c.lastCheckpoint = time.Time{}
	c.lastCheckpointBytes = 0
	c.tuningState = TransferTuning{}
	if !c.Pipeline {
		c.writeGate = newIOGate(c.ParallelFile, false)
		return nil
	}
	if c.MetadataParallel < 1 || c.MetadataParallel > 1024 {
		return fmt.Errorf("metadata-parallel must be between 1 and 1024")
	}
	c.metadataGate = newIOGate(c.MetadataParallel, false)
	if c.WriteParallel < 1 || c.WriteParallel > 1024 || c.CachedFiles < 1 || c.CachedFiles > 1024 {
		return fmt.Errorf("write-parallel and cached-files must be between 1 and 1024")
	}
	slots := min(int64(c.CachedFiles), c.BufferBytes/(4*int64(c.BlockSize)))
	if slots < 1 {
		return fmt.Errorf("buffer-bytes must allow at least four transfer blocks")
	}
	c.streamGate = newIOGate(int(slots), false)
	c.writerFiles = newIOGate(min(c.WriteParallel, int(slots)), false)
	c.writeGate = newIOGate(min(c.WriteParallel, int(slots)), c.AutoTune)
	return nil
}

// File flushes run independently of tuning, without a filesystem-wide barrier.
func (c *Client) startTuning(client *rpc.Client) (func() error, error) {
	finish := func() error { return nil }
	if c.FlushInterval > 0 {
		c.tuningMu.Lock()
		c.flusher = newFileFlusher(c)
		finish = c.flusher.finish
		c.lastCheckpoint = time.Now()
		c.tuningMu.Unlock()
	}
	if !c.AutoTune {
		return finish, nil
	}
	var initial TuneReply
	if err := client.Call("Server.TuneIO", struct{}{}, &initial); err != nil {
		return nil, errors.Join(err, finish())
	}
	c.tuningMu.Lock()
	c.tuningState.ReadLimit = initial.ReadLimit
	c.tuningMu.Unlock()
	stop, done := make(chan struct{}), make(chan struct{})
	var tuningErr error
	go func() {
		defer close(done)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		replies := make(chan *rpc.Call, 1)
		var pending *rpc.Call
		var reply TuneReply
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if pending == nil {
					reply = TuneReply{}
					pending = client.Go("Server.TuneIO", struct{}{}, &reply, replies)
				}
			case result := <-replies:
				pending = nil
				if result.Error != nil {
					tuningErr = result.Error
					c.recordError("IO tuning: %v", tuningErr)
					return
				}
				if reply.ResetClient {
					c.writeGate.resetWindow(time.Now())
				}
				if reply.ClientTurn {
					c.writeGate.step(time.Now())
				}
				c.tuningMu.Lock()
				c.tuningState.ReadLimit = reply.ReadLimit
				c.tuningMu.Unlock()
				state := c.Tuning()
				Logger.Info().Msgf("IO tuning: source=%d writes=%d (%s); pending flush=%d files/%d bytes", state.ReadLimit, state.WriteLimit, state.Reason, state.PendingFlushFiles, state.PendingFlushBytes)
			}
		}
	}()
	return func() error { close(stop); <-done; return errors.Join(tuningErr, finish()) }, nil
}

func (g *ioGate) resetWindow(now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.trial {
		g.limit = g.previousLimit
		g.trial = false
		g.cond.Broadcast()
	}
	g.started = now
	g.bytes = 0
	g.nanos = 0
	g.calls = 0
	g.peak = g.active
}

func (s *Server) ConfigureMetadata(parallel int) error {
	if parallel < 1 || parallel > 1024 {
		return fmt.Errorf("metadata-parallel must be between 1 and 1024")
	}
	s.metadataGate = newIOGate(parallel, false)
	return nil
}
func (c *Client) timeMetadata(operation func() error) error {
	if c.metadataGate != nil {
		release := c.metadataGate.acquire()
		defer release(0, 0)
	}
	return c.timeLocalIO(operation)
}
