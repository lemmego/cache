package memory

import (
	"context"
	"time"

	"github.com/lemmego/cache"
)

// NewLock returns a lock held only within this process.
//
// The exclusion is real but its scope is one process: two servers, or a server
// and a CLI command, each have their own memory store and will both acquire the
// "same" lock. Use the redis store for anything that has to hold across
// processes.
func (s *Store) NewLock(name, owner string, ttl time.Duration) cache.Lock {
	// A logical key: Store.Add applies the prefix.
	return &lock{store: s, key: "lock:" + name, owner: owner, ttl: ttl}
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

// Release removes the lock only if this owner still holds it. The check and the
// delete happen under one shard lock, so a lock that expired and was retaken
// between them cannot be released out from under its new holder.
func (l *lock) Release(_ context.Context) (bool, error) {
	sh := l.store.shardFor(l.store.namespaced(l.key))
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, ok := sh.entries[l.store.namespaced(l.key)]
	if !ok || e.expired(l.store.now()) || string(e.value) != l.owner {
		return false, nil
	}
	delete(sh.entries, l.store.namespaced(l.key))
	return true, nil
}

func (l *lock) ForceRelease(ctx context.Context) error {
	_, err := l.store.Forget(ctx, l.key)
	return err
}
