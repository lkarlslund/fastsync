# Dashboard rates

The throughput and stacked activity charts show the last 60 seconds, with the
latest sample at the right edge labeled `now`. Activity stacks, bottom to top,
are directories (cyan), hardlinks (yellow), unchanged files (green), and copied
files (magenta). The scale uses the largest visible stack. Narrow terminals
average samples sharing a column; wider terminals expand each sample into a bar.
Rates use the elapsed collection interval rather than assuming exactly one second.

Throughput shows local reads, local writes, and combined wire traffic. Local
writes measure bytes accepted by file writes, not physical disk completion.
The separate flushed rate describes completion of fastsync's own file flushes.

The statistics table shows instant (latest sampling interval), trailing 1-minute,
and trailing 5-minute rates with current-run totals alongside. Startup averages
use only the observed duration. Samples crossing a window boundary contribute
proportionally to their overlap; idle intervals remain part of the average.
Averages are collected independently of rendering, so a skipped TUI refresh does
not lose accounting. Flushed rates use changes in the completed-flush byte counter.

Logical processed bytes are no longer displayed, including in the final console
summary: they credited a whole file at completion, including unchanged files and
hardlinks, and were easily confused with throughput. The internal counter remains
for compatibility. Existing-file warmup appears in the Checked row, separately
from completed copy/link work.
