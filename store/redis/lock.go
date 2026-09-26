package redis

import (
	"context"
	"time"

	goredis "github.com/go-redis/redis/v8"
	"github.com/lemmego/cache"
)

// releaseScript deletes the lock only if this owner still holds it.
//
// This has to be one server-side operation. Reading the owner and then deleting
// it as two commands leaves a window: if the lock expires between them and
// another process acquires it, the delete removes a lock it does not hold, and
// two workers run at once — the exact failure a lock exists to prevent.
var releaseScript = goredis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
else
	return 0
end
`)

// extendScript refreshes the expiry, again only for the current owner.
var extendScript = goredis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
	return 0
end
`)

// NewLock returns a lock held across every process talking to this Redis.
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

// Acquire is a single SET NX PX: the check and the write are one operation, so
// two callers cannot both succeed.
func (l *lock) Acquire(ctx context.Context) (bool, error) {
	return l.store.client.SetNX(ctx, l.key, l.owner, ttlFor(l.ttl)).Result()
}

func (l *lock) Block(ctx context.Context, wait time.Duration) (bool, error) {
	return cache.BlockUntil(ctx, wait, 0, l.Acquire)
}

func (l *lock) Get(ctx context.Context, fn func(context.Context) error) (bool, error) {
	return cache.RunLocked(ctx, l, fn)
}

func (l *lock) Release(ctx context.Context) (bool, error) {
	removed, err := releaseScript.Run(ctx, l.store.client, []string{l.key}, l.owner).Int64()
	if err != nil {
		return false, err
	}
	return removed > 0, nil
}

func (l *lock) ForceRelease(ctx context.Context) error {
	return l.store.client.Del(ctx, l.key).Err()
}

// Extend pushes the expiry out, for a holder still working. It reports false if
// the lock has already been lost, which tells the caller to stop rather than
// carry on believing it is protected.
func (l *lock) Extend(ctx context.Context, ttl time.Duration) (bool, error) {
	extended, err := extendScript.Run(ctx, l.store.client, []string{l.key}, l.owner, ttl.Milliseconds()).Int64()
	if err != nil {
		return false, err
	}
	return extended > 0, nil
}
