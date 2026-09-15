package fastsync

import (
	"fmt"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// HostStatus is an additive protocol extension. Older servers need not implement
// Server.Status; missing or delayed telemetry never fails the data transfer.
// It contains only resource counters, not paths, process lists or host secrets.
type HostStatus struct {
	ActiveIO                                int64 // number of currently executing filesystem operations
	Version                                 int
	Valid                                   bool
	CPUBusy, IOWait                         float64 // percent over the sampling interval
	IOPressure, CPUPressure, MemoryPressure float64 // PSI some avg10, percent
	AvailableMemory                         uint64
	IONanoseconds                           uint64 // cumulative worker time in local IO; may exceed wall time
}

type hostSampler struct {
	mu                sync.Mutex
	total, idle, wait uint64
	primed            bool
}

func parseCPU(data string) (total, idle, wait uint64, err error) {
	lines := strings.SplitN(data, "\n", 2)
	fields := strings.Fields(lines[0])
	if len(fields) < 5 || fields[0] != "cpu" {
		return 0, 0, 0, fmt.Errorf("missing aggregate CPU counters")
	}
	// Guest counters are already included in user/nice. Only sum through steal.
	for i := 1; i < len(fields) && i <= 8; i++ {
		value, e := strconv.ParseUint(fields[i], 10, 64)
		if e != nil {
			return 0, 0, 0, e
		}
		total += value
		if i == 4 {
			idle = value
		}
		if i == 5 {
			wait = value
		}
	}
	return
}
func pressure(path string) float64 {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	return parsePressure(string(data))
}
func parsePressure(data string) float64 {
	for _, line := range strings.Split(data, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "some" {
			continue
		}
		for _, field := range fields[1:] {
			if strings.HasPrefix(field, "avg10=") {
				value, err := strconv.ParseFloat(strings.TrimPrefix(field, "avg10="), 64)
				if err == nil && value >= 0 && value <= 100 {
					return value
				}
			}
		}
	}
	return 0
}
func (s *hostSampler) sample(ioTime uint64) HostStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := HostStatus{Version: 1, IONanoseconds: ioTime}
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return status
	}
	total, idle, wait, err := parseCPU(string(data))
	if err != nil {
		return status
	}
	if s.primed && total > s.total && idle >= s.idle && wait >= s.wait {
		elapsed := total - s.total
		inactive := (idle - s.idle) + (wait - s.wait)
		if inactive <= elapsed {
			status.Valid = true
			status.CPUBusy = 100 * float64(elapsed-inactive) / float64(elapsed)
			status.IOWait = 100 * float64(wait-s.wait) / float64(elapsed)
		}
	}
	s.total, s.idle, s.wait, s.primed = total, idle, wait, true
	status.IOPressure = pressure("/proc/pressure/io")
	status.CPUPressure = pressure("/proc/pressure/cpu")
	status.MemoryPressure = pressure("/proc/pressure/memory")
	if data, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) >= 2 && fields[0] == "MemAvailable:" {
				value, _ := strconv.ParseUint(fields[1], 10, 64)
				status.AvailableMemory = value * 1024
			}
		}
	}
	return status
}

func (s *Server) Status(_ struct{}, reply *HostStatus) error {
	if !s.clientsaidhello.Load() {
		return ErrPleaseSayHello
	}
	*reply = s.host.sample(s.localIO.Load())
	reply.ActiveIO = s.activeIO.Load()
	return nil
}

func timedIO(counter *atomic.Uint64, active *atomic.Int64, operation func() error) error {
	active.Add(1)
	defer active.Add(-1)
	started := time.Now()
	defer func() { counter.Add(uint64(time.Since(started))) }()
	return operation()
}

type BottleneckStatus struct {
	Label           string
	Reason          string
	Client, Server  HostStatus
	ServerAvailable bool
}

// IO needs both host pressure and measured IO work by this transfer. Host-wide
// pressure by itself can be caused by unrelated jobs and is not enough evidence.
func classifyBottleneck(local, remote HostStatus, remoteAvailable bool, localWork, remoteWork float64) BottleneckStatus {
	result := BottleneckStatus{Label: "Unknown", Reason: "No clear resource limit", Client: local, Server: remote, ServerAvailable: remoteAvailable}
	candidates := []struct {
		name string
		host HostStatus
		work float64
	}{{"Client", local, localWork}}
	if remoteAvailable {
		candidates = append(candidates, struct {
			name string
			host HostStatus
			work float64
		}{"Server", remote, remoteWork})
	}
	best := 0.0
	for _, candidate := range candidates {
		h := candidate.host
		if !h.Valid {
			continue
		}
		label, reason, score := "", "", 0.0
		if (candidate.work >= 0.5 || h.ActiveIO > 0) && (h.IOPressure >= 10 || h.IOWait >= 10) {
			label = candidate.name + " IO"
			reason = fmt.Sprintf("IO pressure %.0f%%; IO wait %.0f%%; %.1f worker-s/s, %d active IO", h.IOPressure, h.IOWait, candidate.work, h.ActiveIO)
			score = max(h.IOPressure, h.IOWait)
		}
		if h.CPUBusy >= 90 && h.CPUPressure >= 5 {
			label = candidate.name + " CPU"
			reason = fmt.Sprintf("CPU busy %.0f%%; CPU pressure %.0f%%", h.CPUBusy, h.CPUPressure)
			score = max(score, h.CPUBusy)
		}
		if h.MemoryPressure >= 10 {
			label = candidate.name + " memory"
			reason = fmt.Sprintf("Memory pressure %.0f%%", h.MemoryPressure)
			score = 100 + h.MemoryPressure
		}
		if score > best {
			result.Label, result.Reason, best = label, reason, score
		}
	}
	if !remoteAvailable && result.Label == "Unknown" {
		result.Reason = "Server telemetry unavailable; no clear client limit"
	}
	return result
}

func (c *Client) Diagnostics() BottleneckStatus {
	c.diagnosticsMu.Lock()
	defer c.diagnosticsMu.Unlock()
	if c.diagnostics.Label == "" {
		return BottleneckStatus{Label: "Unknown", Reason: "Collecting resource samples"}
	}
	return c.diagnostics
}

func (c *Client) startDiagnostics(client *rpc.Client) func() {
	c.diagnosticsMu.Lock()
	c.diagnostics = BottleneckStatus{}
	c.diagnosticsMu.Unlock()
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		sampler := hostSampler{}
		previousLocal := sampler.sample(c.localIO.Load())
		previousLocalAt := time.Now()
		previousRemote := HostStatus{}
		previousRemoteAt := time.Time{}
		remoteSampleAt := time.Time{}
		remoteWork := 0.0
		var response HostStatus
		var pending *rpc.Call
		var replies chan *rpc.Call
		requestedAt := time.Time{}
		unsupported := false
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case now := <-ticker.C:
				local := sampler.sample(c.localIO.Load())
				local.ActiveIO = c.activeIO.Load()
				localWork := counterRate(local.IONanoseconds, previousLocal.IONanoseconds, now.Sub(previousLocalAt))
				previousLocal, previousLocalAt = local, now
				if pending != nil {
					select {
					case result := <-replies:
						pending = nil
						if result.Error == nil && response.Version == 1 && now.Sub(requestedAt) <= 3*time.Second {
							remoteWork = counterRate(response.IONanoseconds, previousRemote.IONanoseconds, now.Sub(previousRemoteAt))
							previousRemote, previousRemoteAt, remoteSampleAt = response, now, now
						} else {
							remoteSampleAt = time.Time{}
							if result.Error != nil && strings.Contains(result.Error.Error(), "can't find method") {
								unsupported = true
							}
						}
					default:
					}
				}
				available := !remoteSampleAt.IsZero() && now.Sub(remoteSampleAt) <= 3*time.Second
				diagnosis := classifyBottleneck(local, previousRemote, available, localWork, remoteWork)
				c.diagnosticsMu.Lock()
				c.diagnostics = diagnosis
				c.diagnosticsMu.Unlock()
				// At most one outstanding request; a stalled RPC cannot stall sampling or
				// transfer shutdown, or accumulate an unbounded queue of telemetry calls.
				if pending == nil && !unsupported {
					response = HostStatus{}
					replies = make(chan *rpc.Call, 1)
					requestedAt = now
					pending = client.Go("Server.Status", struct{}{}, &response, replies)
				}
			}
		}
	}()
	return func() { close(stop); <-done }
}

func counterRate(current, previous uint64, elapsed time.Duration) float64 {
	if current < previous || elapsed <= 0 {
		return 0
	}
	return float64(current-previous) / float64(elapsed)
}
