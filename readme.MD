# FastSync

FastSync pulls directory trees over a compressed TCP connection. It is designed for large raw server backups with hardlinks shared across historical versions.

It preserves file contents, symlink targets, hardlink relationships, numeric ownership, permissions (including setuid/setgid/sticky), timestamps and extended attributes, including ACL xattrs where supported. Run both ends with sufficient privileges to read and reproduce this metadata; unsupported or failed operations fail the run.

## Platform support

Archive filesystem operations currently require Linux. Paths are opened through pinned directory descriptors without following symlink components; symlink entries are copied literally. Other platforms fail with an unsupported-platform error rather than using an unsafe fallback. On older Linux kernels, permission changes use a no-follow file-descriptor fallback and can fail if the entry cannot be opened.

## Build

```sh
go build -o fastsync ./cmd
```

Use the same build on both hosts. The old binary on a source host must be replaced before using the new verification RPC.

## Copy one server and all its history

Expose the complete directory for **one backed-up server**, containing every dated version. Copy that whole tree in one invocation so hardlinks between versions are tracked together. All destination versions must be on the same filesystem and, for Btrfs, the same subvolume.

The transport is unencrypted TCP, with optional password authentication (see below). Bind to loopback and forward it over SSH:

```sh
# On the source host, serving one server's complete history:
fastsync server --directory /srv/backups/SERVER --bind 127.0.0.1:7331

# On the destination host, in another terminal; keep this tunnel running:
ssh -N -L 7331:127.0.0.1:7331 backup-user@source.example.com

# On the destination host:
mkdir -p /archive/SERVER
fastsync client 127.0.0.1:7331 --directory /archive/SERVER \
  --hardlinks --xattr --pfile 64 --pdir 8 --ramlimit 10737418240
```

Keep the source tree stable throughout copying and verification: use a source snapshot or suspend backup creation/rotation for that server. FastSync checks source identity, size and change timestamps and fails on detected mutations, but these checks do not replace a stable source.

Copy failures produce a nonzero exit status. Regular-file changes are staged in the destination directory and renamed into place after successful transfer and metadata application. Followers of a hardlink wait for that success. Existing destination hardlinks are separated before replacing independent source files, protecting older versions during reruns.

Rerun a failed copy. `--checksum` forces comparison of existing file contents; otherwise equal size and mtime allow content to be skipped. Metadata is reconciled separately. Correct existing hardlink groups keep their inodes and do not trigger content reads or staging without `--checksum`. With `--checksum`, matching content is checked once per shared source inode and left in place. If a shared inode needs metadata or link-topology repair, a local reflink (or local-copy fallback) separates it without remote checksums or data transfer. Unchanged files do not have access times rewritten. Missing hardlinks are created directly. Matching blocks can be reused from an existing destination file; staged updates still write those blocks locally. An interrupted new file may need retransmission. Abrupt termination or a machine crash can leave `.fastsync-*` staging entries; they are not completed backups. Do not run overlapping clients against the same destination tree.

`--durable` is opt-in (default false). It flushes each staged regular file and the destination directory after publication. Without it, the OS writes cached data back normally: rename still prevents exposing a partially copied file during ordinary operation, but a power loss can lose recently completed data. The same safe staging and hardlink coordination are used in both modes.

## Independently verify before retiring history

```sh
fastsync verify 127.0.0.1:7331 --directory /archive/SERVER \
  --report /root/SERVER-verification.jsonl --ramlimit 10737418240
```

Verification checks every source entry against the destination: full-file SHA-256 content, type, size, ownership, permissions, mtime, xattrs, symlink targets, device numbers and hardlink relationships. Each shared source inode is hashed once when its destination links agree. Link counts need not match globally because some links may exist outside the selected source tree. Extra destination entries are allowed, preserving archive-only history; they are not certified by this report.

Verification never repairs or deletes archive entries. Ordinary reads can update access times according to mount policy; atime and ctime are not compared as archival metadata. The JSON-lines report defaults to stdout. `--report` must name a new file outside the destination tree; existing reports are never overwritten. A valid result requires **exit status zero and a final summary with `"complete":true`**. An interrupted/truncated report is not proof of completion. Keep the report with your migration records. It records verification results and content digests, not a self-contained restore manifest or a substitute for the source snapshot.

When retiring migrated backups, retire only older source versions after verification and a restore spot-check. **Keep the newest completed dated backup on the source host; keep all versions on the destination host.** Determine the newest completed version from the dated directories and backup completion information, not blindly from `latest`: `latest` symlinks can be stale or broken. Retention is a separate operation; FastSync never deletes source backups.

Symlink targets are copied literally. Backup catalog links such as `latest -> /srv/backups/SERVER/DATE` need a deliberate destination-only relocation step (for example a relative `latest -> DATE`) after verification, recorded as an intentional difference. Do not rewrite absolute symlinks inside the backed-up server filesystems. A later strict verification will report any intentional catalog-link difference.

## Bottleneck diagnostics

The transfer dashboard shows `Bottleneck: Client IO`, `Server IO`, `Client CPU`, `Server CPU`, `Client memory`, `Server memory`, or `Unknown`. Client means the receiving host; server means the source. These are heuristic resource-pressure diagnoses, not proof of a single root cause. The detector does not guess network saturation from a low transfer rate.

An additive `Server.Status` RPC carries a versioned status structure with CPU busy/iowait, Linux pressure-stall metrics, available memory, and FastSync's active/cumulative filesystem operation time. The client samples itself and polls the server once per second. I/O attribution requires both host pressure and FastSync I/O activity; CPU attribution requires high host CPU use plus scheduling pressure. This cannot reliably detect every limit, such as a single saturated application thread on an otherwise idle many-core machine.

Telemetry is optional: older servers, unsupported platforms, and stale responses produce `Unknown` for unavailable evidence without failing the copy. Server data expires after three seconds, with at most one request outstanding. Host readings are scoped to the OS view available to the process and may include other workloads. Diagnostic polling stops when the copy finishes. No host paths, process listings, or credentials are transferred.

## Options and resource use

- `--pfile` defaults to 64 file workers; `--pdir` defaults to 8 concurrent directory listing requests; `--queuesize` defaults to 1024 pending files.
- `--blocksize` defaults to 64 KiB in the CLI and accepts at most 16 MiB.
- `--hardlinks` and `--xattr` default to true for both copy and verification.
- `--durable` defaults to false; enable it to flush every staged file and published parent directory.
- `--checksum` performs block comparison during copying; it is **not** read-only verification.
- `--delete` defaults to false. It removes extra **destination** entries for mirroring. Leave it disabled for archive migration. Verification rejects deletion.
- `--ramlimit` defaults to zero (disabled). A nonzero value samples Linux process RSS every 100 ms and aborts with a nonzero exit when exceeded; other platforms use Go-managed memory. This is a sampled abort guard, not a hard allocation limit. An abrupt memory-limit exit can leave staging files and an incomplete report. Use an OS memory limit as well when a hard ceiling is required.

Directory listings and inode indexes still consume memory proportional to the work. Copying one server at a time limits their scope; links outside the selected tree can keep inode entries alive until the run ends. Verification retains indexes for linked inodes to check relationships. An optional [persistent resume cache](docs/resume-cache.md) saves hardlink hints and completed-subtree checkpoints; it is not a full file inventory. Leave capacity for the OS, filesystem compression and bees; size the FastSync memory guard and deduplication limit to leave sufficient host memory available.

## Tests

```sh
go test ./...
go test -race ./...
```

Tests cover history hardlinks, hardlink splits on rerun, slow and failed transfers, changing sources, metadata failures, symlink updates, special permissions, xattr-only changes and independent verification of corruption and link topology.

### Archive a selected server

A server can expose a parent backup directory while each client selects one
server with `client HOST:PORT --source server-a --directory /archive/server-a`.
Use the same `--source server-a` for `verify`. Selection is connection-scoped,
confined to the exposed root, and requires a server supporting SelectRoot.

`scripts/archive-loop.py CONFIG.json` copies and verifies a fixed list of
servers sequentially, preserving hardlinks and xattrs. It inherits the terminal
for the copy dashboard. The configuration contains binary, endpoint, snapshot,
snapshot_guid, destination, mount, mount_uuid, ramlimit and servers.
Run it under tmux. State lives beside the configuration: current.json and
SERVER/status.json, verification reports and SERVER/done.json. A lock prevents
concurrent loops. DONE requires successful copying, successful verification and
a filesystem flush. Restarting skips DONE servers with matching archive identity;
failed/interrupted servers are retried. It never deletes source backups.
The operator must keep the configured source snapshot fixed; the deployment's
source wrapper checks its ZFS GUID before starting.

File admission uses alphabetical, depth-first traversal (case-sensitive name
ordering), including directories among file entries. Prefetched listings and
metadata checks may finish out of order, but they cannot reorder file admission
or the copy queue. Hardlink owners are chosen from the first admitted path.
Transfers and ready hardlinks remain parallel; completion times are not ordered.

With hardlink preservation enabled, a read-only first pass visits existing
subtrees and warms the inode cache from qualifying existing hardlinked files.
Missing subtrees are skipped. The second pass reuses those destination inodes
before copying unresolved groups. No missing-file inventory is retained; both
passes walk alphabetically. Metadata repairs and deletion occur only in pass 2.

## Password authentication and compatibility

Password protection is optional. Set `--password-file /path/to/credential` on
both server and client (and on `verify` or `shutdown`). The file must contain one
nonempty line and be readable only by its owner, such as mode 0600. A trailing
newline is ignored; spaces are preserved. Empty, missing, or overly permissive
files fail rather than disabling authentication. Omitting the option on both
ends keeps password-free operation. Supplying it on only one end is an error.
Do not put a real credential in command arguments, source control, or example
configuration. For archive loops, set `password_file` in the loop's private configuration;
the wrapper passes the file path to both copy and verification.

The handshake uses a fresh connection-specific challenge and HMAC-SHA-256 proofs
in both directions. Passwords themselves are not transmitted. Use a strong,
random shared password: a captured exchange permits offline password guessing.
This provides access control, not transport encryption or protection against an
active intermediary. Continue using an SSH tunnel on untrusted networks. An
authenticated peer can read the served tree and request shutdown; there are no
per-user roles. Protect the credential file accordingly.

Both endpoints must match **protocol version 4 and behavior version 4** exactly.
The client checks the server's versions before authentication/source selection;
the server checks the client's versions before enabling access. Legacy peers,
missing versions, and mismatches fail without a downgrade. Both binaries must
be upgraded together; differently built binaries can interoperate when these
compatibility versions match. See `AGENTS.md` for the mandatory version-bump rule.
