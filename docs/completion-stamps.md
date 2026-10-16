# Completed directories and read-only references

`client --completion-stamps` makes a top-level destination directory's source
mtime a durable completion stamp. This option currently requires Linux and
background file flushing. Before writing a selected directory, the client
changes its mtime to a value different from the source and persists that change.
Nested directories finish after their children. Once the entire top-level
subtree succeeds, the client waits for its pending file flushes, uses one
filesystem barrier to persist descendant metadata, rechecks the source, restores
the source directory metadata, and syncs the directory and its parent.

A failed subtree keeps its incomplete timestamp. Successful siblings can still
complete even when the overall invocation returns an error. A matching timestamp
is useful only for an immutable source and an exclusively managed destination;
it does not detect later changes deep inside a tree or establish content hashes
or hardlink topology. Copies made without this option have no such durability
guarantee. An external planner must also account for transfer errors and sidecars.

`--reference generation-a` selects an exact top-level directory as a read-only
source of hardlink reuse seeds. It requires hardlinks and explicit `--include`
copy selections, and cannot overlap an include or use deletion or resume-cache.
References are walked once, before the existing-path scan of copy selections.
They are excluded from the copy pass, and their paths and directory metadata are
never changed. Source and destination identity/metadata are revalidated when a
seed is first used. Missing or incompatible reference seeds fail the invocation
so the planner can promote that reference to repair.

For example, use `--reference generation-a --include generation-b` to copy a
missing generation while preserving links into a completed generation. Selection
of retention windows, completed backups, gap anchors and audits belongs in the
operational planner. These flags do not infer backup policy.

Reference traversal and completion stamps were introduced in protocol/behavior
version 14. Version 15 also supports temporary disk-backed hardlink indexing and
revalidates spilled reuse seeds when they are loaded again. Both endpoint versions
must match exactly.

## Large hardlink histories

`--inode-spill-directory /private/cache --inode-memory-groups 100000` bounds the
in-memory hardlink dictionary and destination-inode ownership claims. Completed
idle groups spill to a temporary bbolt database. Active groups remain in memory
until their queued followers finish, preserving dependency and publication order.
The index preserves canonical paths, link counters and unique-byte accounting.
Database errors fail the transfer; entries are never silently dropped.

Use an existing private directory on a fast local filesystem, outside the copied
tree. The index is removed on normal cleanup. After a crash its old file is not
loaded or trusted; it may be deleted once no client uses it. Spilling cannot be
combined with the persistent resume cache. bbolt uses file-backed memory mappings;
their reclaimable resident pages count toward the CLI's RSS memory guard. Size the
guard appropriately or use an OS memory limit with a bounded Go heap for large
spill files. The database is an invocation-local working index, not verification
or durable completion evidence.
