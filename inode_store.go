package fastsync

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"

	bolt "go.etcd.io/bbolt"
)

var inodeBucket = []byte("inodes")
var ownerBucket = []byte("owners")

// This database lives for one invocation. Its unsynced records are never used
// after a crash and cannot act as completion or resume evidence.
type inodeStore struct {
	db      *bolt.DB
	path    string
	pending map[inodeKey][]byte
	keys    int
}

type storedSeed struct {
	Source     FileInfo
	Dev, Inode uint64
	Size       int64
	Mtim       syscall.Timespec
	LinkTo     string
	Rdev       uint64
}

type storedInodeExtras struct {
	Seed             *storedSeed
	Error, SeedError string
}

func openInodeStore(directory string) (*inodeStore, error) {
	parent, err := DirectoryPathNoFollow(directory)
	if err != nil {
		return nil, err
	}
	f, err := createTempNoFollow(parent, ".fastsync-inodes-")
	if err != nil {
		return nil, err
	}
	path := f.Name()
	if err := f.Close(); err != nil {
		_ = removeNoFollow(path)
		return nil, err
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second, NoSync: true, NoFreelistSync: true})
	if err != nil {
		_ = removeNoFollow(path)
		return nil, err
	}
	s := &inodeStore{db: db, path: path, pending: make(map[inodeKey][]byte)}
	err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucket(inodeBucket); err != nil {
			return err
		}
		_, err := tx.CreateBucket(ownerBucket)
		return err
	})
	if err != nil {
		_ = s.close()
		return nil, err
	}
	return s, nil
}

func inodeStoreKey(key inodeKey) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b, key.dev)
	binary.BigEndian.PutUint64(b[8:], key.inode)
	return b
}

func encodeStoredInode(entry *inodeinfo) ([]byte, error) {
	path := []byte(entry.localhardlinkpath)
	b := binary.BigEndian.AppendUint64(nil, entry.remaining)
	if entry.bytesCounted {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	b = binary.BigEndian.AppendUint32(b, uint32(len(path)))
	b = append(b, path...)
	extra := storedInodeExtras{}
	if entry.err != nil {
		extra.Error = entry.err.Error()
	}
	if entry.seedErr != nil {
		extra.SeedError = entry.seedErr.Error()
	}
	if entry.seed != nil {
		s := entry.seed
		extra.Seed = &storedSeed{Source: s.source, Dev: s.local.Dev, Inode: s.local.Inode,
			Size: s.local.Size, Mtim: s.local.Mtim, LinkTo: s.local.LinkTo, Rdev: s.local.Rdev}
	}
	if extra.Seed != nil || extra.Error != "" || extra.SeedError != "" {
		var seed bytes.Buffer
		if err := gob.NewEncoder(&seed).Encode(extra); err != nil {
			return nil, err
		}
		b = append(b, seed.Bytes()...)
	}
	return b, nil
}

func storedInodePath(b []byte) (string, error) {
	if len(b) < 13 || b[8] > 1 || binary.BigEndian.Uint64(b) == 0 || binary.BigEndian.Uint32(b[9:]) == 0 || uint64(binary.BigEndian.Uint32(b[9:])) > uint64(len(b)-13) {
		return "", errors.New("invalid spilled inode record")
	}
	n := int(binary.BigEndian.Uint32(b[9:]))
	return string(b[13 : 13+n]), nil
}

func decodeStoredInode(b []byte) (*inodeinfo, error) {
	path, err := storedInodePath(b)
	if err != nil {
		return nil, err
	}
	n := len(path)
	e := &inodeinfo{remaining: binary.BigEndian.Uint64(b), bytesCounted: b[8] != 0, localhardlinkpath: path,
		done: make(chan struct{}), diskBacked: true}
	close(e.done)
	if len(b) > 13+n {
		var extra storedInodeExtras
		if err := gob.NewDecoder(bytes.NewReader(b[13+n:])).Decode(&extra); err != nil {
			return nil, fmt.Errorf("decode spilled seed: %w", err)
		}
		if s := extra.Seed; s != nil {
			e.seed = &reuseSeed{source: s.Source}
			e.seed.local.Dev, e.seed.local.Inode = s.Dev, s.Inode
			e.seed.local.Size, e.seed.local.Mtim = s.Size, s.Mtim
			e.seed.local.LinkTo, e.seed.local.Rdev = s.LinkTo, s.Rdev
		}
		if extra.Error != "" {
			e.err = errors.New(extra.Error)
		}
		if extra.SeedError != "" {
			e.seedErr = errors.New(extra.SeedError)
			e.seedOnce.Do(func() {})
		}
	}
	return e, nil
}

func (s *inodeStore) canonicalPath(key inodeKey) (string, error) {
	var path string
	read := func(value []byte) error {
		if value == nil {
			return nil
		}
		var err error
		path, err = storedInodePath(value)
		return err
	}
	if value, ok := s.pending[key]; ok {
		err := read(value)
		return path, err
	}
	err := s.db.View(func(tx *bolt.Tx) error { return read(tx.Bucket(inodeBucket).Get(inodeStoreKey(key))) })
	return path, err
}

func (s *inodeStore) load(key inodeKey) (*inodeinfo, error) {
	if value, ok := s.pending[key]; ok {
		return decodeStoredInode(value)
	}
	var entry *inodeinfo
	err := s.db.View(func(tx *bolt.Tx) error {
		value := tx.Bucket(inodeBucket).Get(inodeStoreKey(key))
		if value == nil {
			return nil
		}
		var err error
		entry, err = decodeStoredInode(value)
		return err
	})
	return entry, err
}

func (s *inodeStore) put(key inodeKey, entry *inodeinfo) error {
	value, err := encodeStoredInode(entry)
	if err != nil {
		return err
	}
	s.pending[key] = value
	if !entry.diskBacked {
		s.keys++
	}
	if len(s.pending) >= 1024 {
		return s.flush()
	}
	return nil
}

func (s *inodeStore) flush() error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(inodeBucket)
		for key, value := range s.pending {
			if err := bucket.Put(inodeStoreKey(key), value); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		clear(s.pending)
	}
	return err
}

func (s *inodeStore) remove(key inodeKey) error {
	err := s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(inodeBucket).Delete(inodeStoreKey(key)) })
	if err == nil {
		delete(s.pending, key)
		s.keys--
	}
	return err
}

func (s *inodeStore) owner(key inodeKey) (inodeKey, bool, error) {
	var owner inodeKey
	var found bool
	err := s.db.View(func(tx *bolt.Tx) error {
		value := tx.Bucket(ownerBucket).Get(inodeStoreKey(key))
		if value != nil {
			if len(value) != 16 {
				return errors.New("invalid spilled inode owner")
			}
			found = true
			owner = inodeKey{binary.BigEndian.Uint64(value), binary.BigEndian.Uint64(value[8:])}
		}
		return nil
	})
	return owner, found, err
}

func (s *inodeStore) putOwners(owners map[inodeKey]inodeKey) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(ownerBucket)
		for key, owner := range owners {
			if err := bucket.Put(inodeStoreKey(key), inodeStoreKey(owner)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *inodeStore) close() error {
	return errors.Join(s.db.Close(), os.Remove(s.path))
}
