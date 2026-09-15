//go:build linux

package fastsync

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/pkg/xattr"
	"golang.org/x/sys/unix"
)

// All archive parent components are opened without following symlinks. openat2
// provides the fast path; older kernels use a pinned descriptor for each step.
func openDirectoryNoFollow(path string) (int, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return -1, err
	}
	fd, err := unix.Openat2(unix.AT_FDCWD, abs, &unix.OpenHow{Flags: unix.O_PATH | unix.O_DIRECTORY | unix.O_CLOEXEC, Resolve: unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS})
	if err == nil {
		return fd, nil
	}
	if !errors.Is(err, unix.ENOSYS) {
		return -1, &os.PathError{Op: "open directory without symlinks", Path: path, Err: err}
	}
	return walkDirectoryNoFollow(abs)
}
func walkDirectoryNoFollow(abs string) (int, error) {
	fd, err := unix.Open("/", unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	for _, part := range strings.Split(strings.TrimPrefix(abs, "/"), "/") {
		if part == "" || part == "." {
			continue
		}
		next, e := unix.Openat(fd, part, unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		unix.Close(fd)
		if e != nil {
			return -1, &os.PathError{Op: "open directory without symlinks", Path: abs, Err: e}
		}
		fd = next
	}
	return fd, nil
}
func pinnedParent(path string) (fd int, name string, err error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return -1, "", err
	}
	if abs == "/" {
		fd, err = openDirectoryNoFollow("/")
		return fd, ".", err
	}
	fd, err = openDirectoryNoFollow(filepath.Dir(abs))
	return fd, filepath.Base(abs), err
}
func pathError(op, path string, err error) error {
	if err == nil {
		return nil
	}
	return &os.PathError{Op: op, Path: path, Err: err}
}

// OpenFileNoFollow rejects symlinks in the leaf and every parent component.
func OpenFileNoFollow(path string, flags int, mode os.FileMode) (*os.File, error) {
	parent, name, err := pinnedParent(path)
	if err != nil {
		return nil, err
	}
	defer unix.Close(parent)
	fd, err := unix.Openat(parent, name, flags|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, uint32(mode.Perm()))
	if err != nil {
		return nil, pathError("open without symlinks", path, err)
	}
	file := os.NewFile(uintptr(fd), path)
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	if !info.Mode().IsRegular() && !info.IsDir() {
		file.Close()
		return nil, fmt.Errorf("refusing content access to special file: %s", path)
	}
	return file, nil
}
func openNoFollow(path string) (*os.File, error) { return OpenFileNoFollow(path, os.O_RDONLY, 0) }
func DirectoryPathNoFollow(path string) (string, error) {
	fd, err := openDirectoryNoFollow(path)
	if err != nil {
		return "", err
	}
	unix.Close(fd)
	return filepath.Abs(path)
}

// L* xattr APIs have no portable fd-relative interface for symlink entries.
// The only followed link here is the kernel's /proc/self/fd/<held-directory>:
// it is an internal bridge to our pinned FD, never an archive-provided symlink.
// The final archive component is inspected with Lstat/Readlink/L*xattr only.
func withPinnedLeaf(path string, fn func(string) error) error {
	fd, name, err := pinnedParent(path)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	return fn(fmt.Sprintf("/proc/self/fd/%d/%s", fd, name))
}
func lstatNoFollow(path string) (info os.FileInfo, err error) {
	err = withPinnedLeaf(path, func(p string) error { var e error; info, e = os.Lstat(p); return e })
	return
}
func pinnedFileInfo(path string, attrs bool) (info FileInfo, err error) {
	err = withPinnedLeaf(path, func(p string) error {
		st, e := os.Lstat(p)
		if e != nil {
			return e
		}
		info, e = rawFileInfo(st, p, attrs)
		if e != nil {
			return e
		}
		after, e := os.Lstat(p)
		if e != nil {
			return e
		}
		if !os.SameFile(st, after) {
			return fmt.Errorf("entry changed while reading metadata: %s", path)
		}
		return nil
	})
	info.Name = path
	return
}
func readDirNoFollow(path string) ([]os.DirEntry, error) {
	f, err := OpenFileNoFollow(path, os.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return nil, err
	}
	entries, err := f.ReadDir(-1)
	err = errors.Join(err, f.Close())
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	return entries, err
}
func mkdirNoFollow(path string, mode os.FileMode) error {
	fd, name, err := pinnedParent(path)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	return pathError("mkdir", path, unix.Mkdirat(fd, name, uint32(mode.Perm())))
}
func mkdirAllNoFollow(path string, mode os.FileMode) error {
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	fd, err := unix.Open("/", unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer func() { unix.Close(fd) }()
	for _, part := range strings.Split(strings.TrimPrefix(abs, "/"), "/") {
		if part == "" {
			continue
		}
		next, e := unix.Openat(fd, part, unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if errors.Is(e, unix.ENOENT) {
			if e = unix.Mkdirat(fd, part, uint32(mode.Perm())); e != nil && !errors.Is(e, unix.EEXIST) {
				return pathError("mkdir", path, e)
			}
			next, e = unix.Openat(fd, part, unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		}
		if e != nil {
			return pathError("mkdir without symlinks", path, e)
		}
		unix.Close(fd)
		fd = next
	}
	return nil
}
func tempName(pattern string) string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return pattern + fmt.Sprintf("%x", b[:])
}
func createTempNoFollow(dir, pattern string) (*os.File, error) {
	fd, err := openDirectoryNoFollow(dir)
	if err != nil {
		return nil, err
	}
	defer unix.Close(fd)
	for i := 0; i < 100; i++ {
		name := tempName(pattern)
		n, e := unix.Openat(fd, name, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
		if errors.Is(e, unix.EEXIST) {
			continue
		}
		if e != nil {
			return nil, pathError("create temp", dir, e)
		}
		return os.NewFile(uintptr(n), filepath.Join(dir, name)), nil
	}
	return nil, os.ErrExist
}
func mkdirTempNoFollow(dir, pattern string) (string, error) {
	fd, err := openDirectoryNoFollow(dir)
	if err != nil {
		return "", err
	}
	defer unix.Close(fd)
	for i := 0; i < 100; i++ {
		name := tempName(pattern)
		e := unix.Mkdirat(fd, name, 0700)
		if errors.Is(e, unix.EEXIST) {
			continue
		}
		if e != nil {
			return "", pathError("mkdir temp", dir, e)
		}
		return filepath.Join(dir, name), nil
	}
	return "", os.ErrExist
}
func removeNoFollow(path string) error {
	fd, name, err := pinnedParent(path)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	err = unix.Unlinkat(fd, name, 0)
	if errors.Is(err, unix.EISDIR) {
		err = unix.Unlinkat(fd, name, unix.AT_REMOVEDIR)
	}
	return pathError("remove", path, err)
}
func removeTreeAt(parent int, name string) error {
	err := unix.Unlinkat(parent, name, 0)
	if err == nil || errors.Is(err, unix.ENOENT) {
		return nil
	}
	if !errors.Is(err, unix.EISDIR) {
		return err
	}
	fd, err := unix.Openat(parent, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	f := os.NewFile(uintptr(fd), name)
	defer f.Close()
	entries, err := f.ReadDir(-1)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := removeTreeAt(fd, e.Name()); err != nil {
			return err
		}
	}
	return unix.Unlinkat(parent, name, unix.AT_REMOVEDIR)
}
func removeAllNoFollow(path string) error {
	fd, name, err := pinnedParent(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	if name == "." {
		return fmt.Errorf("refusing recursive root removal")
	}
	return pathError("remove tree", path, removeTreeAt(fd, name))
}
func renameNoFollow(old, new string) error {
	a, an, err := pinnedParent(old)
	if err != nil {
		return err
	}
	defer unix.Close(a)
	b, bn, err := pinnedParent(new)
	if err != nil {
		return err
	}
	defer unix.Close(b)
	return pathError("rename", new, unix.Renameat(a, an, b, bn))
}
func linkNoFollow(old, new string) error {
	a, an, err := pinnedParent(old)
	if err != nil {
		return err
	}
	defer unix.Close(a)
	b, bn, err := pinnedParent(new)
	if err != nil {
		return err
	}
	defer unix.Close(b)
	return pathError("link", new, unix.Linkat(a, an, b, bn, 0))
}
func symlinkNoFollow(target, path string) error {
	fd, name, err := pinnedParent(path)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	return pathError("symlink", path, unix.Symlinkat(target, fd, name))
}
func mknodNoFollow(path string, mode uint32, dev uint64) error {
	fd, name, err := pinnedParent(path)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	return pathError("mknod", path, unix.Mknodat(fd, name, mode, int(dev)))
}
func chownNoFollow(path string, uid, gid int) error {
	fd, name, err := pinnedParent(path)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	return pathError("chown", path, unix.Fchownat(fd, name, uid, gid, unix.AT_SYMLINK_NOFOLLOW))
}
func chmodNoFollow(path string, mode uint32) error {
	fd, name, err := pinnedParent(path)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	err = unix.Fchmodat(fd, name, mode, unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOSYS) {
		// Older kernels cannot chmod an O_PATH descriptor. Pin a non-symlink inode
		// with a readable FD instead; fail closed when it cannot be opened.
		n, e := unix.Openat(fd, name, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if e != nil {
			return pathError("chmod", path, e)
		}
		defer unix.Close(n)
		err = unix.Fchmod(n, mode)
	}
	return pathError("chmod", path, err)
}
func timesNoFollow(path string, times []unix.Timespec) error {
	fd, name, err := pinnedParent(path)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	return pathError("timestamps", path, unix.UtimesNanoAt(fd, name, times, unix.AT_SYMLINK_NOFOLLOW))
}
func setXattrNoFollow(path, name string, value []byte) error {
	return withPinnedLeaf(path, func(p string) error { return xattr.LSet(p, name, value) })
}
func removeXattrNoFollow(path, name string) error {
	return withPinnedLeaf(path, func(p string) error { return xattr.LRemove(p, name) })
}
