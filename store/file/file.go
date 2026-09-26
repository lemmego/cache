// Package file provides a cache store backed by the filesystem.
//
// It survives a restart and is shared between processes on the same host,
// which makes it the right default for a scaffolded project: the CLI and the
// server are separate processes, and a memory cache would leave `cache:clear`
// a silent no-op.
//
// It is not safe across hosts. The atomic operations rest on rename(2) and
// O_EXCL, neither of which is atomic on NFS.
package file

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"crypto/sha256"
	"encoding/hex"

	"github.com/lemmego/cache"
)

// lockStripes bounds in-process contention without a mutex per key. The old
// implementation used a single RWMutex, which serialised every write in the
// process.
const lockStripes = 64

// headerSize is the fixed prefix on every entry: the expiry as Unix
// nanoseconds, big-endian, with zero meaning "never".
//
// The previous implementation gob-encoded a struct with an interface{} field,
// which meant every concrete type had to be gob.Register'ed and an
// unregistered one panicked. Values arrive here already encoded, so the file
// format only has to carry the expiry.
const headerSize = 8

// entryFileName matches the files this store creates, and nothing else. Flush
// and Prune use it so they cannot remove data the store does not own.
var entryFileName = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Store is a filesystem-backed cache.
type Store struct {
	dir    string
	root   string // dir plus a directory of this store's own, see New
	prefix string
	now    func() time.Time
	locks  [lockStripes]sync.Mutex
}

var (
	_ cache.Store        = (*Store)(nil)
	_ cache.LockProvider = (*Store)(nil)
	_ cache.Pruner       = (*Store)(nil)
)

// Config configures a file store.
type Config struct {
	// Dir is where entries are written. It is created if absent.
	Dir string
	// Prefix namespaces keys, so two stores can share a directory.
	Prefix string
	// Now overrides the clock, for tests.
	Now func() time.Time
}

// New returns a file store, creating its directory.
//
// It returns an error rather than panicking. The previous implementation
// panicked here and on five paths inside its write, which meant an unwritable
// disk took the whole process down.
func New(cfg Config) (*Store, error) {
	if cfg.Dir == "" {
		return nil, errors.New("cache/file: Dir is required")
	}
	// Each prefix gets its own subtree. Entry filenames are hashes, so a flush
	// could not otherwise tell one store's files from another's sharing the
	// directory — it would either have to read every file to find out, or
	// delete them all.
	sum := sha256.Sum256([]byte(cfg.Prefix))
	root := filepath.Join(cfg.Dir, "p-"+hex.EncodeToString(sum[:6]))
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("cache/file: creating %s: %w", root, err)
	}

	s := &Store{dir: cfg.Dir, root: root, prefix: cfg.Prefix, now: cfg.Now}
	if s.now == nil {
		s.now = time.Now
	}
	return s, nil
}

func (s *Store) Prefix() string { return s.prefix }

// path shards entries two levels deep. One directory holding every entry makes
// both the filesystem and any listing of it slow once a cache gets large.
//
// They live under this store's own root, so a flush can find exactly its own
// entries.
func (s *Store) path(key string) string {
	sum := sha256.Sum256([]byte(s.prefix + key))
	name := hex.EncodeToString(sum[:])
	return filepath.Join(s.root, name[0:2], name[2:4], name)
}

func (s *Store) lockFor(key string) *sync.Mutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s.prefix + key))
	return &s.locks[h.Sum32()%lockStripes]
}

func (s *Store) expiryFor(ttl time.Duration) time.Time {
	if ttl <= 0 {
		return time.Time{}
	}
	return s.now().Add(ttl)
}

// readEntry returns the value and its expiry.
//
// A file that is truncated, corrupt, or expired reads as a miss. A half-written
// cache entry must never fail a request — the whole point of a cache is that
// losing one is survivable.
func (s *Store) readEntry(key string) (value []byte, expiresAt time.Time, err error) {
	raw, err := os.ReadFile(s.path(key))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, time.Time{}, cache.ErrMiss
		}
		return nil, time.Time{}, err
	}
	if len(raw) < headerSize {
		return nil, time.Time{}, cache.ErrMiss
	}

	if nanos := int64(binary.BigEndian.Uint64(raw[:headerSize])); nanos != 0 {
		expiresAt = time.Unix(0, nanos)
		if !s.now().Before(expiresAt) {
			return nil, time.Time{}, cache.ErrMiss
		}
	}
	return raw[headerSize:], expiresAt, nil
}

// writeEntry writes atomically: a temp file in the same directory, then a
// rename. A reader therefore sees either the old entry or the new one, never a
// partial write.
func (s *Store) writeEntry(key string, value []byte, expiresAt time.Time) error {
	path := s.path(key)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("cache/file: creating %s: %w", filepath.Dir(path), err)
	}

	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return fmt.Errorf("cache/file: creating a temporary file: %w", err)
	}
	tmpName := tmp.Name()
	// Any failure past this point must not leave the temp file behind.
	defer func() {
		if err != nil {
			_ = os.Remove(tmpName)
		}
	}()

	var header [headerSize]byte
	if !expiresAt.IsZero() {
		binary.BigEndian.PutUint64(header[:], uint64(expiresAt.UnixNano()))
	}
	if _, err = tmp.Write(header[:]); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("cache/file: writing the header: %w", err)
	}
	if _, err = tmp.Write(value); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("cache/file: writing the value: %w", err)
	}
	if err = tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("cache/file: setting permissions: %w", err)
	}
	if err = tmp.Close(); err != nil {
		return fmt.Errorf("cache/file: closing the temporary file: %w", err)
	}
	if err = os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("cache/file: replacing %s: %w", path, err)
	}
	return nil
}

func (s *Store) Get(_ context.Context, key string) ([]byte, error) {
	mu := s.lockFor(key)
	mu.Lock()
	defer mu.Unlock()

	value, _, err := s.readEntry(key)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (s *Store) GetMany(ctx context.Context, keys []string) (map[string][]byte, error) {
	found := make(map[string][]byte, len(keys))
	for _, key := range keys {
		value, err := s.Get(ctx, key)
		if err != nil {
			if errors.Is(err, cache.ErrMiss) {
				continue
			}
			return nil, err
		}
		found[key] = value
	}
	return found, nil
}

func (s *Store) Put(_ context.Context, key string, value []byte, ttl time.Duration) error {
	mu := s.lockFor(key)
	mu.Lock()
	defer mu.Unlock()
	return s.writeEntry(key, value, s.expiryFor(ttl))
}

func (s *Store) PutMany(ctx context.Context, values map[string][]byte, ttl time.Duration) error {
	expiresAt := s.expiryFor(ttl)
	for key, value := range values {
		mu := s.lockFor(key)
		mu.Lock()
		err := s.writeEntry(key, value, expiresAt)
		mu.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Add(_ context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	mu := s.lockFor(key)
	mu.Lock()
	defer mu.Unlock()

	// An entry that exists but has expired is not an obstacle.
	if _, _, err := s.readEntry(key); err == nil {
		return false, nil
	} else if !errors.Is(err, cache.ErrMiss) {
		return false, err
	}
	if err := s.writeEntry(key, value, s.expiryFor(ttl)); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) Increment(_ context.Context, key string, delta int64) (int64, error) {
	mu := s.lockFor(key)
	mu.Lock()
	defer mu.Unlock()

	existing, expiresAt, err := s.readEntry(key)
	if errors.Is(err, cache.ErrMiss) {
		if err := s.writeEntry(key, cache.FormatCounter(delta), time.Time{}); err != nil {
			return 0, err
		}
		return delta, nil
	}
	if err != nil {
		return 0, err
	}

	current, err := cache.ParseCounter(existing)
	if err != nil {
		return 0, err
	}
	next := current + delta

	// Carry the existing expiry forward. The previous implementation wrote a
	// zero expiry here, silently converting a counter with a window into one
	// that never reset.
	if err := s.writeEntry(key, cache.FormatCounter(next), expiresAt); err != nil {
		return 0, err
	}
	return next, nil
}

func (s *Store) Decrement(ctx context.Context, key string, delta int64) (int64, error) {
	return s.Increment(ctx, key, -delta)
}

func (s *Store) Forget(_ context.Context, key string) (bool, error) {
	mu := s.lockFor(key)
	mu.Lock()
	defer mu.Unlock()

	_, _, err := s.readEntry(key)
	existed := err == nil

	if err := os.Remove(s.path(key)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, fmt.Errorf("cache/file: removing %s: %w", key, err)
	}
	return existed, nil
}

// Flush removes every entry this store wrote.
//
// It removes only files whose names match the hash format this store produces.
// The previous implementation called RemoveAll on every directory entry, so
// pointing it at a shared directory destroyed unrelated files.
func (s *Store) Flush(context.Context) error {
	return s.walkEntries(func(path string, _ os.FileInfo) error {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("cache/file: removing %s: %w", path, err)
		}
		return nil
	})
}

// Prune removes expired entries.
//
// It is explicit rather than a background sweeper: walking the tree is
// expensive, and every process on the host sharing the directory would
// otherwise walk it on its own timer.
func (s *Store) Prune(context.Context) error {
	now := s.now()
	return s.walkEntries(func(path string, _ os.FileInfo) error {
		file, err := os.Open(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		var header [headerSize]byte
		_, readErr := io.ReadFull(file, header[:])
		_ = file.Close()

		// A file too short to hold a header is a partial write; drop it.
		if readErr != nil {
			if errors.Is(readErr, io.ErrUnexpectedEOF) || errors.Is(readErr, io.EOF) {
				_ = os.Remove(path)
				return nil
			}
			return readErr
		}

		nanos := int64(binary.BigEndian.Uint64(header[:]))
		if nanos != 0 && !now.Before(time.Unix(0, nanos)) {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
		return nil
	})
}

// walkEntries visits only the files this store owns.
func (s *Store) walkEntries(fn func(path string, info os.FileInfo) error) error {
	return filepath.Walk(s.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if info.IsDir() || !entryFileName.MatchString(info.Name()) {
			return nil
		}
		return fn(path, info)
	})
}

func init() {
	cache.Register("file", func(_ context.Context, cfg *cache.Config) (cache.Store, error) {
		return New(Config{Dir: cfg.File.Path, Prefix: cfg.Prefix})
	})
}
