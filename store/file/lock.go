package file

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/lemmego/cache"
)

// NewLock returns a lock held across every process on this host.
//
// It rests on O_EXCL file creation, which is atomic on a local filesystem and
// is not on NFS. For a lock that has to hold across machines, use the redis
// store.
func (s *Store) NewLock(name, owner string, ttl time.Duration) cache.Lock {
	return &lock{store: s, key: s.prefix + "lock:" + name, owner: owner, ttl: ttl}
}

type lock struct {
	store *Store
	key   string
	owner string
	ttl   time.Duration
}

func (l *lock) Owner() string { return l.owner }

func (l *lock) Acquire(ctx context.Context) (bool, error) {
	return l.store.Add(ctx, l.key, []byte(l.owner), l.ttl)
}

func (l *lock) Block(ctx context.Context, wait time.Duration) (bool, error) {
	return cache.BlockUntil(ctx, wait, 0, l.Acquire)
}

func (l *lock) Get(ctx context.Context, fn func(context.Context) error) (bool, error) {
	return cache.RunLocked(ctx, l, fn)
}

// Release removes the lock only if this owner still holds it, with the read and
// the remove under the same stripe lock so a lock that expired and was retaken
// in between is not released out from under its new holder.
//
// Across processes the stripe lock buys nothing, and the read-then-remove is
// not atomic. The window is small and the consequence is bounded — a lock
// released a moment early — but it is real, and it is why redis does this with
// a compare-and-delete script instead.
func (l *lock) Release(context.Context) (bool, error) {
	mu := l.store.lockFor(l.key)
	mu.Lock()
	defer mu.Unlock()

	value, _, err := l.store.readEntry(l.key)
	if err != nil {
		if errors.Is(err, cache.ErrMiss) {
			return false, nil
		}
		return false, err
	}
	if string(value) != l.owner {
		return false, nil
	}
	if err := os.Remove(l.store.path(l.key)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	return true, nil
}

func (l *lock) ForceRelease(ctx context.Context) error {
	_, err := l.store.Forget(ctx, l.key)
	return err
}
