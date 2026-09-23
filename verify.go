package fastsync

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/rpc"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"time"
)

// Verification emits JSON lines as it walks; a final successful summary is required.
// It never repairs the destination. Ordinary filesystem reads may update atime.
type VerificationRecord struct {
	Type        string `json:"type"`
	Path        string `json:"path,omitempty"`
	SHA256      string `json:"sha256,omitempty"`
	HardlinkTo  string `json:"hardlink_to,omitempty"`
	Error       string `json:"error,omitempty"`
	Files       uint64 `json:"files,omitempty"`
	Directories uint64 `json:"directories,omitempty"`
	Errors      uint64 `json:"errors,omitempty"`
	Complete    bool   `json:"complete,omitempty"`
	Started     string `json:"started,omitempty"`
	Finished    string `json:"finished,omitempty"`
}

type verifiedInode struct {
	local      inodeKey
	path, hash string
	ctime      syscall.Timespec
}

func (c *Client) Verify(client *rpc.Client, report io.Writer) error {
	if c.Delete {
		return errors.New("verification cannot be combined with deletion")
	}
	if err := c.validateQueueSettings(); err != nil {
		return err
	}
	if err := c.hello(client); err != nil {
		return err
	}
	encoder := json.NewEncoder(report)
	summary := VerificationRecord{Type: "summary", Started: time.Now().UTC().Format(time.RFC3339Nano)}
	if err := encoder.Encode(VerificationRecord{Type: "start", Path: c.BasePath, Started: summary.Started}); err != nil {
		return err
	}
	sourceInodes := make(map[inodeKey]verifiedInode)
	destInodes := make(map[inodeKey]inodeKey)
	var firstError error
	fail := func(path string, err error) error {
		summary.Errors++
		if firstError == nil {
			firstError = err
		}
		return encoder.Encode(VerificationRecord{Type: "error", Path: path, Error: err.Error()})
	}
	var walk func(FileInfo) error
	walk = func(remote FileInfo) error {
		path := filepath.Join(c.BasePath, remote.Name)
		local, err := pathToFileInfo(path, c.Options.SendXattr)
		if err != nil {
			return fail(remote.Name, err)
		}
		if err := compareMetadata(local, remote, c.Options.SendXattr); err != nil {
			if err := fail(remote.Name, err); err != nil {
				return err
			}
		}
		if remote.IsDir {
			if !local.IsDir {
				return nil
			} // Never follow a destination symlink as a directory.
			var listing FileListResponse
			if err := client.Call("Server.List", remote.Name, &listing); err != nil {
				return fail(remote.Name, err)
			}
			if remote.Name == "/" {
				listing.Files, err = c.selectTopLevel(listing.Files)
				if err != nil {
					return fail(remote.Name, err)
				}
			}
			for _, entry := range listing.Files {
				if err := walk(entry); err != nil {
					return err
				}
			}
			if err := checkRemote(client, remote); err != nil {
				return fail(remote.Name, err)
			}
			summary.Directories++
			return encoder.Encode(VerificationRecord{Type: "directory", Path: remote.Name})
		}
		record := VerificationRecord{Type: "file", Path: remote.Name}
		remoteKey, localKey := inodeKey{remote.Dev, remote.Inode}, inodeKey{local.Dev, local.Inode}
		already := false
		if c.PreserveHardlinks {
			if local.Nlink > 1 {
				if source, found := destInodes[localKey]; found && source != remoteKey {
					if err := fail(remote.Name, errors.New("destination links independent source inodes together")); err != nil {
						return err
					}
				}
				destInodes[localKey] = remoteKey
			}
			if canonical, found := sourceInodes[remoteKey]; found {
				if canonical.local != localKey {
					if err := fail(remote.Name, errors.New("source hardlink relationship missing at destination")); err != nil {
						return err
					}
				} else {
					if canonical.ctime != local.Ctim {
						if err := fail(remote.Name, errors.New("destination hardlink changed during verification")); err != nil {
							return err
						}
					}
					already = true
					record.SHA256 = canonical.hash
					record.HardlinkTo = canonical.path
				}
			}
		}
		if remote.Mode.IsRegular() && local.Mode.IsRegular() && !already {
			var sourceHash string
			if err := client.Call("Server.Hash", remote.Name, &sourceHash); err != nil {
				return fail(remote.Name, err)
			}
			localHash, err := hashFile(path)
			if err != nil {
				return fail(remote.Name, err)
			}
			if sourceHash != localHash {
				if err := fail(remote.Name, errors.New("SHA-256 content mismatch")); err != nil {
					return err
				}
			}
			record.SHA256 = sourceHash
		}
		if err := checkRemote(client, remote); err != nil {
			return fail(remote.Name, err)
		}
		if c.PreserveHardlinks && remote.Nlink > 1 && !already {
			sourceInodes[remoteKey] = verifiedInode{local: localKey, path: remote.Name, hash: record.SHA256, ctime: local.Ctim}
		}
		summary.Files++
		return encoder.Encode(record)
	}
	var root FileInfo
	if err := client.Call("Server.Stat", "/", &root); err != nil {
		if err := fail("/", err); err != nil {
			return err
		}
	} else if !root.IsDir {
		if err := fail("/", errors.New("source root is not a directory")); err != nil {
			return err
		}
	} else if err := walk(root); err != nil {
		return err
	}
	summary.Complete = summary.Errors == 0
	summary.Finished = time.Now().UTC().Format(time.RFC3339Nano)
	if err := encoder.Encode(summary); err != nil {
		return err
	}
	if firstError != nil {
		return fmt.Errorf("verification failed with %d error(s): %w", summary.Errors, firstError)
	}
	return nil
}

func compareMetadata(local, remote FileInfo, attrs bool) error {
	if local.Mode&os.ModeType != remote.Mode&os.ModeType {
		return errors.New("file type mismatch")
	}
	if local.Mode&permissionBits != remote.Mode&permissionBits {
		return errors.New("permissions mismatch")
	}
	if local.Owner != remote.Owner || local.Group != remote.Group {
		return errors.New("ownership mismatch")
	}
	if local.Mtim != remote.Mtim {
		return errors.New("mtime mismatch")
	}
	if remote.Mode.IsRegular() && local.Size != remote.Size {
		return errors.New("size mismatch")
	}
	if local.LinkTo != remote.LinkTo {
		return errors.New("symlink target mismatch")
	}
	if local.Rdev != remote.Rdev {
		return errors.New("device number mismatch")
	}
	if attrs && remote.Xattrs != nil {
		if len(local.Xattrs) != len(remote.Xattrs) {
			return errors.New("xattrs mismatch")
		}
		for name, value := range remote.Xattrs {
			other, found := local.Xattrs[name]
			if !found || !slices.Equal(other, value) {
				return fmt.Errorf("xattr %s mismatch", name)
			}
		}
	}
	return nil
}

func hashFile(path string) (digest string, err error) {
	file, err := openNoFollow(path)
	if err != nil {
		return "", err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	before, err := file.Stat()
	if err != nil {
		return "", err
	}
	if !before.Mode().IsRegular() {
		return "", errors.New("hash requires a regular file")
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	beforeNative := FileInfo{Name: path}
	if err := beforeNative.extractNativeInfo(before); err != nil {
		return "", err
	}
	after, err := file.Stat()
	if err != nil {
		return "", err
	}
	current, err := lstatNoFollow(path)
	if err != nil {
		return "", err
	}
	afterNative := FileInfo{Name: path}
	if err := afterNative.extractNativeInfo(after); err != nil {
		return "", err
	}
	if beforeNative.Ctim != afterNative.Ctim || !os.SameFile(before, current) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return "", fmt.Errorf("file changed while hashing: %s", path)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
