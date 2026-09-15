//go:build freebsd
// +build freebsd

package fastsync

func mkNod(localpath string, iftyp uint32, rdev uint64) error {
	return mknodNoFollow(localpath, iftyp, rdev)
}
