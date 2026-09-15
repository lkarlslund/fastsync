package fastsync

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLogicalAndUniqueByteTotals(t *testing.T) {
	for _, preserve := range []bool{true, false} {
		t.Run(map[bool]string{true: "preserved", false: "separate-copies"}[preserve], func(t *testing.T) {
			src, dst := t.TempDir(), t.TempDir()
			writeTestFile(t, src, "a", "data")
			for _, name := range []string{"b", "c"} {
				if err := os.Link(filepath.Join(src, "a"), filepath.Join(src, name)); err != nil {
					t.Fatal(err)
				}
			}
			// Identical content in a distinct inode must still count separately.
			writeTestFile(t, src, "d", "data")
			if err := os.Symlink("a", filepath.Join(src, "symlink")); err != nil {
				t.Fatal(err)
			}
			// A link outside the selection must not inflate either total.
			if err := os.Link(filepath.Join(src, "a"), filepath.Join(t.TempDir(), "outside")); err != nil {
				t.Fatal(err)
			}
			for pass := 0; pass < 2; pass++ {
				c := runTestSync(t, src, dst, func(c *Client) { c.PreserveHardlinks = preserve })
				if c.Perf.Get(BytesProcessed) != 16 || c.Perf.Get(BytesUniqueProcessed) != 8 {
					t.Fatalf("pass %d logical=%d unique=%d", pass, c.Perf.Get(BytesProcessed), c.Perf.Get(BytesUniqueProcessed))
				}
			}
		})
	}
}
func TestWarmSeedByteTotals(t *testing.T) {
	src, dst := makeABC(t)
	c, _ := resumeSync(t, src, dst, false)
	size := uint64(len("shared data"))
	if c.Perf.Get(BytesProcessed) != 3*size || c.Perf.Get(BytesUniqueProcessed) != size {
		t.Fatalf("warmed totals: %d/%d", c.Perf.Get(BytesProcessed), c.Perf.Get(BytesUniqueProcessed))
	}
	if c.Perf.Get(WrittenBytes) != 0 {
		t.Fatal("reuse wrote data")
	}
}
