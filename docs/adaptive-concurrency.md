# Bounded transfer pipeline and background flushing

The optional --pipeline mode separates source read-ahead from destination writes.
--autotune enables it and coordinates independent source/client IO limits.

    fastsync server --directory /snapshot/backups --read-parallel 64 --metadata-parallel 8 --autotune
    fastsync client HOST:7331 --source server-a --directory /archive/server-a --autotune --blocksize 524288 --write-parallel 64 --cached-files 64 --buffer-bytes 134217728 --flush-bytes 268435456 --flush-files 64 --flush-workers 64 --flush-interval 30s

## Bounds and scheduling

Source read operations and metadata operations have separate gates shared by all
connections. Destination metadata has its own limit; --pdir controls scanning.
At most --write-parallel files perform sequential writes. Additional admitted
files may prefetch, subject to both --cached-files and --buffer-bytes.

Each admitted stream reserves four blocks: two circulating payload buffers, a
delta-comparison scratch block and decoder allowance. Small files allocate only
their content length; large VMDKs/ISOs never allocate whole-file buffers.
The payload budget excludes other process memory, Go garbage awaiting collection,
codec overhead and kernel page cache. --ramlimit is a separate sampled RSS guard.
Hardlink followers wait before acquiring stream/writer permits.

## Background file flushing

Flushing is part of regular copying, independently of autotuning. Up to 64 workers
(default --flush-workers) call Sync on descriptors for files fastsync actually
wrote. There is no syncfs call, initial global drain, periodic global barrier or
filesystem-wide flush. A flush serializes only with writes to that same file.

Completed files are queued before their original descriptors close; retained
descriptors continue to refer to the same inode after rename. Large open files
are queued at16MiB batches (or a quarter of a smaller configured flush budget),
on the30-second timer, or when the pending-byte budget requires draining.

--flush-files bounds retained descriptors including active files. --flush-bytes
bounds logical bytes written or reserved for writing but not yet confirmed by a
successful file flush. These are separate from user-space streaming buffers.
When either budget fills, affected producers backpressure until background work
frees capacity. This prevents unlimited backlog; it cannot eliminate waits when
storage is slower than incoming data.

Normal completion waits for outstanding own-file flushes. Flush failures fail
the copy, including failures after a file has been published. Failed staging
files may still be flushed during cleanup. --flush-interval=0 disables background
flushing for manual operation; autotuning requires it enabled.
--durable remains the separate per-file plus parent-directory durability policy.
Background data flushing alone is not a promise of crash-durable directory
publication; the archive loop keeps its final completion/verification safeguards.

The TUI shows retained file count, pending bytes, last per-file flush duration
and a smoothed rate of fastsync bytes confirmed by completed flushes. Kernel
filesystems can share metadata work, so per-file fsync is not physical isolation
from other workloads, but we do not explicitly flush their files.

## Adaptive controls

Source/client probes alternate through30-second windows so only one side changes
during each experiment. Source uses completed reads. Destination throughput uses
completed file-flush bytes, not buffered write returns. Async completion and
changing workloads can still confound short windows; this controller is experimental.

Limits start at min(8, configured maximum). Increases need >10% throughput gain
without >50% write/read latency growth. Decreases can retain90% of throughput to
favor lower load. Unhelpful probes revert. Source controls are shared across
connections, with one connection driving the controller and an expiring lease.
Writes and background flushes share the adaptive destination IO gate. The
flush-worker setting is a ceiling, not additional IO concurrency. Metadata
limits remain independently configurable fixed limits.
No compression, OS, RAID, checksum-selection or durability settings are auto-tuned.

## Validation

Race suite, vet and CLI copy/resume/verify pass. Tests cover files larger than
the total buffer budget, hardlinks with one writer, unchanged-file resume,
delta reuse, read/write failures, source gate sharing, coordinated probe phases,
regression rollback, periodic flushing without autotuning, flush failure on
completion, byte/file bounds, and continued writes to another file while one
file's flush is blocked.

The earlier filesystem-wide periodic checkpoint implementation was replaced:
it created global pauses and is not the intended regular write cycle.

## Separate checking, copying and linking

The file scheduler has separate worker pools for metadata checking, data copying,
and ready hardlinks (each capped by --pfile). Waiting hardlink followers are held
as dependency records, without occupying a worker. Owner failure fails its
followers; publication must succeed before followers are released. Checksum mode
runs content reads in the copy pool. All existing selection and verification
rules remain in effect, including source stability checks.

Admission is bounded to max(1, --queuesize) + 3 * --pfile jobs total across ready,
running and dependency-blocked work. The existing input channel adds at most
--queuesize entries. No goroutine is spawned per file. Payload and flush budgets
are unchanged. When the bound fills, scanning backpressures; separation permits
bounded lookahead, not an unlimited scan ahead of copying.

The dashboard shows unchanged, copied and linked files/sec, ready queue depths,
dependency count and average queue wait per dispatch (including dependency waits).
Copied counts regular data-stage completions, including local reflink/delta work;
metadata-only repairs and special-file creation still contribute to Files/sec.
Statistics occupy a full-height right panel; graph and log share the left panel.
Source metadata batching and metadata-specific adaptive concurrency are not part
of this change. Source and destination data tuning remain independent.

## Repeatable traversal

Traversal is alphabetical and depth-first, with files and directories sorted
by name together. Directory listings may be prefetched (at most --pdir buffered
prefetch responses, with --pdir concurrent RPCs); only the ordered walker emits
file work. Pending traversal state is the depth-first frontier, not a full-tree
manifest. Individual directory listings are still returned as whole lists.
Metadata check results are released into the copy queue in admission order,
within the existing scheduler bound. Hardlink ownership is chosen on admission,
so a restart with the same source picks the same first path in each group.

Copy and hardlink completion timing remains parallel and can vary. The change
prevents ownership from changing due to racing directory scans; it does not
search the entire destination for an alternate existing member if the first
alphabetical member is absent. Old unordered partial runs can still need a
one-time topology reconciliation. Source changes, cache state and the growing
completed prefix still matter when comparing restart performance.

## Existing paths before missing paths

When preserving hardlinks, pass 1 is read-only on the destination: it skips
missing/type-conflicting destination subtrees and searches existing paths for
qualifying source-inode cache seeds. A seed must match type, metadata, and the
regular size/mtime or explicit checksum policy. Nonqualifying candidates do not
claim the group. Fixed striped locks ensure a checksum is not repeated for every
alias after one member qualifies. Single-link files are handled in pass 2.

Only the in-memory inode cache survives the barrier, one path per reusable source
inode plus validation metadata; no missing-file inventory or persistent index is
created. Candidate eligibility and destination inode ownership checks prevent
merging distinct source inodes. Cached members are revalidated before first use.
Source mutation/replaced cached destination errors fail the run.

Pass 2 rediscovers all paths in alphabetical order, creates missing directories,
links to cached members, and copies groups without a reusable member. Warmed
entries are retained through the barrier and count down only during pass 2.
The cached path can therefore be b/file even when missing a/file sorts first.
Remaining new groups use first admission as before. Metadata reconciliation,
deletion, final own-file flushes and archive verification stay in pass 2.

The TUI reports Existing files versus Link/copy, existing-path examination rate,
and reusable groups found. Initial scan time is an explicit cost: the live run
must complete cache warming before it starts copying. Existing-path examination
includes inode-cache hits, not necessarily a stat/content read for each alias.

Warmup listing/entry errors are accumulated but do not abort the second pass.
The second walk retries listings naturally and copies accessible paths. Any
recorded error still makes final completion fail and prevents a done marker,
even if the second pass succeeds. The existing deletion/postprocessing guard
after errors remains conservative.
