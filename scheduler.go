package fastsync

import (
	"fmt"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type scheduledFile struct {
	checkSequence uint64
	remote        FileInfo
	entry         *inodeinfo
	follower      bool
	work          func() error
	queued        time.Time
}
type fileCompletion struct {
	job   *scheduledFile
	stage int
	err   error
}

// A single dispatcher owns the ready queues and dependency lists. The admission
// bound includes queued, running and dependency-blocked jobs, not just channels.
func (c *Client) scheduleFiles(client *rpc.Client) {
	var queues [3][]*scheduledFile
	var jobs [3]chan *scheduledFile
	done := make(chan fileCompletion, 3*c.ParallelFile)
	var workers sync.WaitGroup
	for stage := range jobs {
		jobs[stage] = make(chan *scheduledFile)
		for n := 0; n < c.ParallelFile; n++ {
			workers.Add(1)
			go func(stage int) {
				defer workers.Done()
				for j := range jobs[stage] {
					c.Perf.Add(QueueWaitNanos, uint64(time.Since(j.queued)))
					c.Perf.Add(QueueDispatches, 1)
					var err error
					path := filepath.Join(c.BasePath, j.remote.Name)
					switch stage {
					case 0:
						if c.AlwaysChecksum && j.remote.Mode.IsRegular() {
							j.work = func() error { return c.syncIndependent(client, j.remote, path) }
						} else {
							err = c.syncIndependentDispatch(client, j.remote, path, func(work func() error) error { j.work = work; return nil })
						}
					case 1:
						err = j.work()
					case 2:
						if j.entry.err != nil {
							err = fmt.Errorf("hardlink source failed: %w", j.entry.err)
							break
						}
						if j.entry.seed != nil {
							j.entry.seedOnce.Do(func() { j.entry.seedErr = c.validateReuseSeed(client, j.entry) })
							if j.entry.seedErr != nil {
								err = j.entry.seedErr
								break
							}
						}
						err = checkRemote(client, j.remote)
						if err == nil {
							err = c.timeMetadata(func() error {
								a, e := lstatNoFollow(j.entry.localhardlinkpath)
								if e != nil {
									return e
								}
								b, e := lstatNoFollow(path)
								if e == nil && os.SameFile(a, b) {
									c.Perf.Add(FilesUnchanged, 1)
									return nil
								}
								if e = publishHardlink(j.entry.localhardlinkpath, path, c.Durable); e == nil {
									c.Perf.Add(FilesLinked, 1)
								}
								return e
							})
						}
					}
					if err != nil || stage != 0 || j.work == nil {
						if err != nil {
							c.recordError("%s: %v", j.remote.Name, err)
						} else {
							c.Perf.Add(FilesProcessed, 1)
							c.Perf.Add(BytesProcessed, uint64(j.remote.Size))
						}
						// Directory finalization can perform RPC and metadata IO;
						// never block the dispatcher on it.
						c.ProcessedItemInDir(filepath.Dir(j.remote.Name))
					}
					done <- fileCompletion{j, stage, err}
				}
			}(stage)
		}
	}
	waiting := make(map[*inodeinfo][]*scheduledFile)
	pending := 0
	bound := max(1, c.QueueSize) + 3*c.ParallelFile
	input := c.filequeue
	enqueue := func(stage int, j *scheduledFile) { j.queued = time.Now(); queues[stage] = append(queues[stage], j) }
	var checkSequence uint64
	checked := orderedChecks{pending: make(map[uint64]fileCompletion)}
	finish := func(result fileCompletion) {
		j := result.job
		if result.err == nil && result.stage == 0 && j.work != nil {
			enqueue(1, j)
			return
		}
		if j.entry != nil {
			if !j.follower {
				j.entry.err = result.err
				close(j.entry.done)
				for _, f := range waiting[j.entry] {
					c.Perf.Add(QueueWaitNanos, uint64(time.Since(f.queued)))
					c.Perf.Add(QueueDispatches, 1)
					enqueue(2, f)
					c.dependencies.Add(-1)
				}
				delete(waiting, j.entry)
			}
			c.inodesMu.Lock()
			j.entry.remaining--
			if j.entry.remaining == 0 {
				delete(c.inodes, inodeKey{j.remote.Dev, j.remote.Inode})
			}
			c.inodesMu.Unlock()
		}
		pending--
	}
	for input != nil || pending > 0 {
		var receive <-chan FileInfo
		if pending < bound {
			receive = input
		}
		var send [3]chan *scheduledFile
		var next [3]*scheduledFile
		for stage := range queues {
			c.stageQueued[stage].Store(int64(len(queues[stage])))
			if len(queues[stage]) > 0 {
				send[stage] = jobs[stage]
				next[stage] = queues[stage][0]
			}
		}
		c.stageQueued[0].Store(int64(len(queues[0]) + len(checked.pending)))
		select {
		case remote, ok := <-receive:
			if !ok {
				input = nil
				continue
			}
			pending++
			if int64(pending) > c.schedulingPeak.Load() {
				c.schedulingPeak.Store(int64(pending))
			}
			j := &scheduledFile{remote: remote}
			if c.PreserveHardlinks && remote.Nlink > 1 {
				key := inodeKey{remote.Dev, remote.Inode}
				c.inodesMu.Lock()
				j.entry, j.follower = c.inodes[key]
				if !j.follower {
					j.entry = &inodeinfo{localhardlinkpath: filepath.Join(c.BasePath, remote.Name), done: make(chan struct{}), remaining: remote.Nlink}
					c.inodes[key] = j.entry
				}
				c.inodesMu.Unlock()
			}
			if j.follower {
				select {
				case <-j.entry.done:
					enqueue(2, j)
				default:
					j.queued = time.Now()
					waiting[j.entry] = append(waiting[j.entry], j)
					c.dependencies.Add(1)
				}
			} else {
				j.checkSequence = checkSequence
				checkSequence++
				enqueue(0, j)
			}
		case send[0] <- next[0]:
			queues[0][0] = nil
			queues[0] = queues[0][1:]
		case send[1] <- next[1]:
			queues[1][0] = nil
			queues[1] = queues[1][1:]
		case send[2] <- next[2]:
			queues[2][0] = nil
			queues[2] = queues[2][1:]
		case result := <-done:
			if result.stage == 0 {
				checked.complete(result, finish)
			} else {
				finish(result)
			}

		}
	}
	for stage := range jobs {
		close(jobs[stage])
		c.stageQueued[stage].Store(0)
	}
	workers.Wait()
}

// Metadata latency must not reorder admission into the copy queue. These results
// consume the same scheduler admission bound as every other pending job.
type orderedChecks struct {
	next    uint64
	pending map[uint64]fileCompletion
}

func (o *orderedChecks) complete(result fileCompletion, finish func(fileCompletion)) {
	o.pending[result.job.checkSequence] = result
	for {
		r, ok := o.pending[o.next]
		if !ok {
			return
		}
		delete(o.pending, o.next)
		o.next++
		finish(r)
	}
}
