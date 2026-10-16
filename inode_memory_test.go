package fastsync

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
)

func BenchmarkRetainedInodeMemory(b *testing.B) {
	for _, warm := range []bool{false, true} {
		b.Run(fmt.Sprintf("warm=%v", warm), func(b *testing.B) {
			const groups = 100000
			for n := 0; n < b.N; n++ {
				runtime.GC()
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				c := NewClient()
				for i := 0; i < groups; i++ {
					path := fmt.Sprintf("/generation/%s/file-%09d", strings.Repeat("subdirectory/", 8), i)
					e := &inodeinfo{localhardlinkpath: "/archive" + path, remaining: 40, done: make(chan struct{})}
					close(e.done)
					if warm {
						e.seed = &reuseSeed{source: FileInfo{Name: path, Inode: uint64(i), Nlink: 40}}
					}
					c.inodes[inodeKey{1, uint64(i)}] = e
				}
				runtime.GC()
				runtime.ReadMemStats(&after)
				b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc)/groups, "B/group")
				runtime.KeepAlive(c)
			}
		})
	}
}

func BenchmarkSpilledInodeMemory(b *testing.B) {
	const groups = 100000
	for n := 0; n < b.N; n++ {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		c := NewClient()
		c.InodeMemoryGroups = 1000
		store, err := openInodeStore(b.TempDir())
		if err != nil {
			b.Fatal(err)
		}
		c.inodeStore = store
		for i := 0; i < groups; i++ {
			path := fmt.Sprintf("/generation/%s/file-%09d", strings.Repeat("subdirectory/", 8), i)
			e := &inodeinfo{localhardlinkpath: "/archive" + path, remaining: 40, done: make(chan struct{}), seed: &reuseSeed{source: FileInfo{Name: path, Inode: uint64(i), Nlink: 40}}}
			close(e.done)
			key := inodeKey{1, uint64(i)}
			c.inodes[key] = e
			if err := c.idleInodeLocked(key, e); err != nil {
				b.Fatal(err)
			}
		}
		runtime.GC()
		runtime.ReadMemStats(&after)
		b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc)/groups, "B/group")
		b.ReportMetric(float64(len(c.inodes)), "resident-groups")
		runtime.KeepAlive(c)
		if err := store.close(); err != nil {
			b.Fatal(err)
		}
	}
}
