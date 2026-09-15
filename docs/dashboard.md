# Dashboard rates

The throughput and stacked activity charts use one fixed time bucket per terminal
column, with the latest bucket at the right edge. Press `1`, `2`, `3`, or `4` for
1, 5, 15, or 60 seconds per column on both charts. The axis shows the resulting
window duration, which depends on terminal width. Resizing adds/removes columns
without redistributing old samples. Each resolution retains at most 2,048 buckets;
missing observations remain gaps. Coarse buckets average observed rates using
actual elapsed time, not summed per-second rates. The newest bucket updates until
its interval ends, then scrolls left by one column.

Throughput uses braille characters (2×4 subpixels per terminal cell) for smooth
lines. Activity stacks, bottom to top, are directories (blue), hardlinks (amber),
unchanged files (green), and copied files (purple). Press `L` to toggle linear and
zero-safe logarithmic Y scaling; linear is the default. Both modes auto-scale to
visible data. The logarithmic mode uses log1p and a linear neighborhood around
zero. Activity transforms cumulative boundaries, not individual components: the
top still represents the total, but segment heights in log mode aren't additive
rate comparisons. Current scale and time resolution appear above each plot.

Throughput shows local reads, local writes, and combined wire traffic. Local
writes measure bytes accepted by file writes, not physical disk completion.
The separate flushed rate describes completion of fastsync's own file flushes.

The statistics table shows instant (latest sampling interval), trailing 1-minute,
and trailing 5-minute rates with current-run totals alongside. Startup averages
use only the observed duration. Samples crossing a window boundary contribute
proportionally to their overlap; idle intervals remain part of the average.
Averages are collected independently of rendering, so a skipped TUI refresh does
not lose accounting. Flushed rates use changes in the completed-flush byte counter.

`Processed` and `Unique data` are current-run totals of successfully completed
regular-file data, including unchanged files. Processed counts the full size at
every pathname; Unique data counts each source (device, inode) once. For example,
three hardlinks to a 10 GB file contribute 30 GB processed and 10 GB unique.
Identical content in different inodes counts separately; these are neither
compressed disk usage nor an inventory of the entire source before traversal.
Symlinks and directories do not contribute data bytes. Neither total is plotted
as a throughput rate. Warmup does not count bytes; copy-pass completion does.

Unique accounting shares the existing hardlink records when preservation is on.
With preservation disabled, a separate temporary index tracks only multiply
linked source files for statistics, releasing entries after all links finish.
These local counters do not change the wire format or transfer semantics and
therefore do not require a compatibility-version bump.

The dashboard uses matching colors across chart legends and rate rows. Throughput
and activity sit side by side above the event log. The full-height statistics
panel reserves 52 columns, with rates first and resource details below. Buffer
and stream bars show usage against configured limits, not overall copy progress.
The palette uses 256-color terminal colors and keeps the terminal background.

To render a synthetic preview of the real layout without connecting to a server:

```sh
FASTSYNC_TUI_PREVIEW=/tmp/fastsync-preview.svg go test ./cmd -run TestDashboardPreview -count=1
```

The client initializes its dashboard before credential loading and connection
setup, so the copy header and all connection/cache startup events enter the same
TUI log. Startup errors unwind the dashboard and restore the terminal.

The dashboard remains visible during graceful shutdown until client cleanup and
final logging finish. It identifies loading and validating persisted inode hints,
saving the resume cache, and flushing remaining file data. Hydration shows hints
validated and inodes loaded; the flush queue shows remaining files and bytes.
These are live counters rather than a completion percentage or estimated finish
time, since neither the remaining validation cost nor disk latency is known.
