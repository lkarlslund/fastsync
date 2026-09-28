package fastsync

import (
	"testing"
	"unsafe"
)

func TestReuseSeedRetainsLocalValidationFields(t *testing.T) {
	seed := &reuseSeed{}
	local := FileInfo{Dev: 1, Inode: 2, Size: 3, LinkTo: "target", Rdev: 4}
	local.Mtim.Sec = 5
	seed.local.Dev, seed.local.Inode = local.Dev, local.Inode
	seed.local.Size, seed.local.Mtim = local.Size, local.Mtim
	seed.local.LinkTo, seed.local.Rdev = local.LinkTo, local.Rdev
	if !seed.sameLocal(local) {
		t.Fatal("matching local state rejected")
	}
	for _, change := range []func(*FileInfo){
		func(f *FileInfo) { f.Dev++ },
		func(f *FileInfo) { f.Inode++ },
		func(f *FileInfo) { f.Size++ },
		func(f *FileInfo) { f.Mtim.Nsec++ },
		func(f *FileInfo) { f.LinkTo = "other" },
		func(f *FileInfo) { f.Rdev++ },
	} {
		changed := local
		change(&changed)
		if seed.sameLocal(changed) {
			t.Fatal("changed local state accepted")
		}
	}
}

var benchmarkReuseSeed *reuseSeed

func BenchmarkReuseSeedFootprint(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		benchmarkReuseSeed = &reuseSeed{}
	}
	b.ReportMetric(float64(unsafe.Sizeof(reuseSeed{})), "B/seed")
	b.ReportMetric(float64(2*unsafe.Sizeof(FileInfo{})), "B/previous-seed")
}
