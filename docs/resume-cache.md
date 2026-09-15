# Persistent resume cache

Use `--resume-cache /private/state/server-a.jsonl --resume-id SNAPSHOT_ID` to
save hardlink reuse hints. The file and its lock must be outside the destination;
place them on a fast local filesystem. A hint contains a source inode identity
and a relative path, not file contents or a complete inventory. Treat the cache
as private operational data: it contains source/destination identifiers and paths.

Use a stable, immutable source snapshot ID. The cache also binds to the endpoint
and selected root (CLI), source root identity, destination root identity,
protocol/behavior versions, and checksum/xattr options. A different destination
or snapshot falls back to scanning. Each saved hint is independently checked
against current source and destination metadata, using the normal checksum rule.
Stale or truncated journals trigger the existing-path traversal; valid partial
hints are retained to avoid repeated checks. No cache is proof of verification.

A complete warmup allows subsequent runs to validate one hint per group instead
of traversing every existing pathname during warmup. The copy pass normally still
walks the tree. Newly copied groups are appended as soon as publication succeeds.
Only hardlinked groups are indexed. Journal writes are buffered and checkpointed
every 30 seconds. Successful completion, returned errors, Ctrl-C, SIGTERM, and
SIGHUP close/flush the cache. The CLI memory guard also uses graceful cancellation
for clients. SIGKILL, kernel OOM kills, and power loss cannot run cleanup; the
last durable checkpoint is used. Only one process can open a given cache for use.

## Optional position-based resume

`--resume-position` defaults to **false**. It enables skipping durably completed
**top-level subtrees**, such as dated backup directories, for immutable snapshot
sources. The destination must be exclusively managed by this transfer between
runs. This option does not inspect changes deep inside skipped subtrees. Remove
or rename the cache after external destination modifications, or omit the flag
to check all paths again. Missing/replaced top-level directories are revisited.

With a cache configured and background file flushing enabled, completed-subtree
markers are saved regardless of whether `--resume-position` is enabled. The flag
only controls using those markers on startup. Checkpointing waits for fastsync's
own pending writes in the subtree and flushes directory metadata bottom-up before
persisting the marker. It does not call syncfs or drain unrelated file writes.
Errors prevent new subtree markers; completed siblings remain usable. Parallel
completion cannot mark an unfinished subtree complete. A partially finished
top-level subtree is revisited from its beginning; there is no per-file inventory
or "last filename" shortcut. Top-level regular files are always revisited.

Position mode requires the cache, a snapshot ID, and background flushing. Current
run file/byte totals exclude skipped prior subtrees; the TUI reports their count
separately. Cached hardlink groups spanning skipped trees may remain in memory
because the full source link count includes paths intentionally not visited.

The archive loop supports `resume_cache: true` and `resume_position: true` in its
private configuration. Both default to false. It uses the configured snapshot
GUID and saves separate cache files beside each server's status records. Copy
completion is still followed by full verification; position resume never bypasses
verification or the archive completion checks.
