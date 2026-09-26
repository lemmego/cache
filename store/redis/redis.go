// Package redis provides a cache store backed by Redis.
//
// It is the only built-in store whose locks hold across machines, and the only
// one where a cache entry written by one host is visible to another.
package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	goredis "github.com/go-redis/redis/v8"
	"github.com/lemmego/cache"
)

// Store is a Redis-backed cache.
type Store struct {
	client goredis.UniversalClient
	prefix string

	// allowFlushDB permits Flush to empty the whole database instead of
	// scanning. Off by default: a Lemmego application's sessions and queue
	// commonly share this database, and FLUSHDB would take them with it.
	allowFlushDB bool
}

var (
	_ cache.Store        = (*Store)(nil)
	_ cache.LockProvider = (*Store)(nil)
	_ cache.TagIndex     = (*Store)(nil)
)

// Config configures a Redis store.
type Config struct {
	// Client is an existing client to use. When nil, one is built from Addr.
	Client goredis.UniversalClient

	Addr     string
	Password string
	DB       int
	PoolSize int

	// Prefix namespaces every key, and bounds what Flush removes.
	Prefix string

	// AllowFlushDB lets Flush call FLUSHDB. Only set this when the database is
	// dedicated to the cache.
	AllowFlushDB bool
}

// New returns a Redis store. It does not connect; call Ping to check
// reachability.
func New(cfg Config) (*Store, error) {
	client := cfg.Client
	if client == nil {
		if cfg.Addr == "" {
			return nil, errors.New("cache/redis: Addr or Client is required")
		}
		client = goredis.NewClient(&goredis.Options{
			Addr:     cfg.Addr,
			Password: cfg.Password,
			DB:       cfg.DB,
			PoolSize: cfg.PoolSize,
		})
	}
	prefix := cfg.Prefix
	if prefix == "" {
		return nil, errors.New("cache/redis: Prefix is required, so Flush can be scoped to this cache")
	}
	return &Store{client: client, prefix: prefix, allowFlushDB: cfg.AllowFlushDB}, nil
}

// Ping reports whether the server is reachable.
func (s *Store) Ping(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}

// Close releases the connection pool, unless the client was supplied by the
// caller, who then owns its lifetime.
func (s *Store) Close() error { return s.client.Close() }

// Client exposes the underlying client for driver-specific work.
func (s *Store) Client() goredis.UniversalClient { return s.client }

func (s *Store) Prefix() string { return s.prefix }

// namespaced is the key as it appears in Redis. Every key this store writes
// carries the prefix, which is what lets Flush find exactly its own entries.
func (s *Store) namespaced(key string) string { return s.prefix + key }

func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	value, err := s.client.Get(ctx, s.namespaced(key)).Bytes()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, cache.ErrMiss
		}
		return nil, err
	}
	return value, nil
}

func (s *Store) GetMany(ctx context.Context, keys []string) (map[string][]byte, error) {
	if len(keys) == 0 {
		return map[string][]byte{}, nil
	}

	namespaced := make([]string, len(keys))
	for i, key := range keys {
		namespaced[i] = s.namespaced(key)
	}

	values, err := s.client.MGet(ctx, namespaced...).Result()
	if err != nil {
		return nil, err
	}

	found := make(map[string][]byte, len(values))
	for i, value := range values {
		if value == nil {
			continue // absent, which in a bulk read is not an error
		}
		switch v := value.(type) {
		case string:
			found[keys[i]] = []byte(v)
		case []byte:
			found[keys[i]] = v
		default:
			return nil, fmt.Errorf("cache/redis: unexpected value type %T for %q", value, keys[i])
		}
	}
	return found, nil
}

// ttlFor converts a cache TTL to the one Redis wants. Redis treats zero as "no
// expiry", which matches this package's Forever.
func ttlFor(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return 0
	}
	return ttl
}

func (s *Store) Put(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return s.client.Set(ctx, s.namespaced(key), value, ttlFor(ttl)).Err()
}

func (s *Store) PutMany(ctx context.Context, values map[string][]byte, ttl time.Duration) error {
	if len(values) == 0 {
		return nil
	}
	pipe := s.client.Pipeline()
	for key, value := range values {
		pipe.Set(ctx, s.namespaced(key), value, ttlFor(ttl))
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (s *Store) Add(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	return s.client.SetNX(ctx, s.namespaced(key), value, ttlFor(ttl)).Result()
}

// Increment uses INCRBY, which preserves an existing key's expiry. A key it
// creates has no expiry, matching every other store here and Redis itself.
func (s *Store) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	next, err := s.client.IncrBy(ctx, s.namespaced(key), delta).Result()
	if err != nil {
		if isNotAnInteger(err) {
			return 0, cache.ErrNotNumeric
		}
		return 0, err
	}
	return next, nil
}

func (s *Store) Decrement(ctx context.Context, key string, delta int64) (int64, error) {
	return s.Increment(ctx, key, -delta)
}

// isNotAnInteger recognises the server's complaint about incrementing a value
// that is not a counter. Redis reports it as an error string rather than a
// distinguishable type.
func isNotAnInteger(err error) bool {
	return err != nil && (contains(err.Error(), "not an integer") || contains(err.Error(), "not a valid float"))
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (len(needle) == 0 || indexOf(haystack, needle) >= 0)
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}

func (s *Store) Forget(ctx context.Context, key string) (bool, error) {
	removed, err := s.client.Del(ctx, s.namespaced(key)).Result()
	if err != nil {
		return false, err
	}
	return removed > 0, nil
}

// Flush removes this cache's keys by scanning for its prefix.
//
// It deliberately does not call FLUSHDB. A Lemmego application typically shares
// one database between its cache, its sessions and its queue, so emptying the
// database would log every user out and drop queued work. AllowFlushDB opts in
// for a dedicated database.
func (s *Store) Flush(ctx context.Context) error {
	if s.allowFlushDB {
		return s.client.FlushDB(ctx).Err()
	}

	// Collect the whole keyspace first, then delete.
	//
	// Deleting inside the scan loop is the obvious implementation and it is
	// wrong: SCAN's cursor is a position in the keyspace, so removing the keys
	// just returned shifts what follows and the next cursor skips past them.
	// Against a server whose cursor is an index this drops exactly the keys
	// deleted so far — half of them, in practice.
	const batchSize = 500

	var (
		keys   []string
		cursor uint64
	)
	for {
		batch, next, err := s.client.Scan(ctx, cursor, s.prefix+"*", batchSize).Result()
		if err != nil {
			return err
		}
		keys = append(keys, batch...)
		if next == 0 {
			break
		}
		cursor = next
	}

	// UNLINK reclaims memory on a background thread, so flushing a large cache
	// does not stall the server the way DEL would.
	for start := 0; start < len(keys); start += batchSize {
		end := min(start+batchSize, len(keys))
		if err := s.client.Unlink(ctx, keys[start:end]...).Err(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) tagSetKey(tag string) string { return s.prefix + "tagset:" + tag }

func (s *Store) AddTagEntries(ctx context.Context, tag string, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	members := make([]any, len(keys))
	for i, key := range keys {
		members[i] = s.namespaced(key)
	}
	return s.client.SAdd(ctx, s.tagSetKey(tag), members...).Err()
}

func (s *Store) TagEntries(ctx context.Context, tag string) ([]string, error) {
	members, err := s.client.SMembers(ctx, s.tagSetKey(tag)).Result()
	if err != nil {
		return nil, err
	}
	// Hand back keys as the store's own callers spell them, without the
	// prefix that namespaced added.
	keys := make([]string, 0, len(members))
	for _, member := range members {
		keys = append(keys, trimPrefix(member, s.prefix))
	}
	return keys, nil
}

func trimPrefix(s, prefix string) string {
	if len(s) >= len(prefix) && s[:len(prefix)] == prefix {
		return s[len(prefix):]
	}
	return s
}

func (s *Store) ForgetTagEntries(ctx context.Context, tag string) error {
	return s.client.Del(ctx, s.tagSetKey(tag)).Err()
}

func init() {
	cache.Register("redis", func(ctx context.Context, cfg *cache.Config) (cache.Store, error) {
		store, err := New(Config{
			Addr:         cfg.Redis.Addr,
			Password:     cfg.Redis.Password,
			DB:           cfg.Redis.DB,
			PoolSize:     cfg.Redis.PoolSize,
			Prefix:       cfg.Prefix,
			AllowFlushDB: cfg.Redis.AllowFlushDB,
		})
		if err != nil {
			return nil, err
		}
		// Fail here rather than on the first request: a wrong address should
		// stop a deployment, not degrade it silently.
		if err := store.Ping(ctx); err != nil {
			_ = store.Close()
			return nil, fmt.Errorf("connecting to redis at %s: %w", cfg.Redis.Addr, err)
		}
		return store, nil
	})
}
