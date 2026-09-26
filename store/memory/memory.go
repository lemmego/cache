// Package memory provides an in-process cache store.
//
// It is the fastest store and the only one that survives nothing: entries live
// in the heap of one process, so a second process — including the CLI running
// alongside a server — sees an entirely separate cache. That makes it right for
// tests and for a single-process deployment, and wrong as a default.
package memory

import (
	"context"
	"hash/fnv"
	"sync"
	"time"

	"github.com/lemmego/cache"
)

// shardCount trades memory for contention. Every operation locks exactly one
// shard, so unrelated keys rarely block each other.
const shardCount = 16

type entry struct {
	value     []byte
	expiresAt time.Time // zero means it never expires
}

func (e entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && !now.Before(e.expiresAt)
}

type shard struct {
	mu      sync.RWMutex
	entries map[string]entry
}

// Store is an in-process cache.
//
// A map guarded by a mutex rather than a sync.Map: sync.Map cannot
// check-expiry-then-delete or read-modify-write a counter as one atomic step
// without a second per-entry lock, which gives back what it saves.
type Store struct {
	prefix string
	shards [shardCount]*shard

	// now is injectable so tests can advance time instead of sleeping.
	now func() time.Time

	stop     chan struct{}
	stopOnce sync.Once

	tagMu   sync.Mutex
	tagKeys map[string]map[string]struct{}
}

var (
	_ cache.Store        = (*Store)(nil)
	_ cache.LockProvider = (*Store)(nil)
	_ cache.TagIndex     = (*Store)(nil)
)

// Config configures a memory store.
type Config struct {
	Prefix string

	// GCInterval is how often expired entries are swept. Lazy expiry on read
	// is not enough on its own: an entry written once and never read again
	// would otherwise hold its memory until the process exits, and
	// write-once-never-read is a normal caching pattern. Zero disables the
	// sweeper.
	GCInterval time.Duration

	// Now overrides the clock, for tests.
	Now func() time.Time
}

// New returns a memory store and, unless GC is disabled, starts its sweeper.
// Call Close to stop it.
func New(cfg Config) *Store {
	s := &Store{prefix: cfg.Prefix, now: cfg.Now, stop: make(chan struct{}), tagKeys: map[string]map[string]struct{}{}}
	if s.now == nil {
		s.now = time.Now
	}
	for i := range s.shards {
		s.shards[i] = &shard{entries: map[string]entry{}}
	}
	if cfg.GCInterval > 0 {
		go s.sweep(cfg.GCInterval)
	}
	return s
}

// Close stops the sweeper. It is safe to call more than once.
func (s *Store) Close() error {
	s.stopOnce.Do(func() { close(s.stop) })
	return nil
}

func (s *Store) sweep(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			_ = s.Prune(context.Background())
		}
	}
}

// Prune drops every expired entry.
func (s *Store) Prune(context.Context) error {
	now := s.now()
	for _, sh := range s.shards {
		sh.mu.Lock()
		for key, e := range sh.entries {
			if e.expired(now) {
				delete(sh.entries, key)
			}
		}
		sh.mu.Unlock()
	}
	return nil
}

func (s *Store) shardFor(key string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return s.shards[h.Sum32()%shardCount]
}

// expiryFor converts a TTL into an absolute deadline. A non-positive TTL means
// forever, which is the zero time.
func (s *Store) expiryFor(ttl time.Duration) time.Time {
	if ttl <= 0 {
		return time.Time{}
	}
	return s.now().Add(ttl)
}

func (s *Store) Get(_ context.Context, key string) ([]byte, error) {
	sh := s.shardFor(key)

	sh.mu.RLock()
	e, ok := sh.entries[key]
	sh.mu.RUnlock()
	if !ok {
		return nil, cache.ErrMiss
	}
	if e.expired(s.now()) {
		sh.mu.Lock()
		// Re-check: another goroutine may have replaced it since the RUnlock.
		if current, still := sh.entries[key]; still && current.expired(s.now()) {
			delete(sh.entries, key)
		}
		sh.mu.Unlock()
		return nil, cache.ErrMiss
	}

	// Copy, so a caller mutating the result cannot corrupt the cache. The
	// network stores get this for free; this one has to pay for it.
	return append([]byte(nil), e.value...), nil
}

func (s *Store) GetMany(ctx context.Context, keys []string) (map[string][]byte, error) {
	found := make(map[string][]byte, len(keys))
	for _, key := range keys {
		value, err := s.Get(ctx, key)
		if err != nil {
			if err == cache.ErrMiss {
				continue
			}
			return nil, err
		}
		found[key] = value
	}
	return found, nil
}

func (s *Store) Put(_ context.Context, key string, value []byte, ttl time.Duration) error {
	sh := s.shardFor(key)
	sh.mu.Lock()
	sh.entries[key] = entry{value: append([]byte(nil), value...), expiresAt: s.expiryFor(ttl)}
	sh.mu.Unlock()
	return nil
}

func (s *Store) PutMany(ctx context.Context, values map[string][]byte, ttl time.Duration) error {
	for key, value := range values {
		if err := s.Put(ctx, key, value, ttl); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Add(_ context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if e, ok := sh.entries[key]; ok && !e.expired(s.now()) {
		return false, nil
	}
	sh.entries[key] = entry{value: append([]byte(nil), value...), expiresAt: s.expiryFor(ttl)}
	return true, nil
}

func (s *Store) Increment(_ context.Context, key string, delta int64) (int64, error) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	existing, ok := sh.entries[key]
	if !ok || existing.expired(s.now()) {
		next := delta
		sh.entries[key] = entry{value: cache.FormatCounter(next)}
		return next, nil
	}

	current, err := cache.ParseCounter(existing.value)
	if err != nil {
		return 0, err
	}
	next := current + delta

	// Keep the existing expiry. Resetting it would turn a rate-limit counter
	// with a one-minute window into one that never resets.
	sh.entries[key] = entry{value: cache.FormatCounter(next), expiresAt: existing.expiresAt}
	return next, nil
}

func (s *Store) Decrement(ctx context.Context, key string, delta int64) (int64, error) {
	return s.Increment(ctx, key, -delta)
}

func (s *Store) Forget(_ context.Context, key string) (bool, error) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, ok := sh.entries[key]
	if !ok || e.expired(s.now()) {
		delete(sh.entries, key)
		return false, nil
	}
	delete(sh.entries, key)
	return true, nil
}

func (s *Store) Flush(context.Context) error {
	for _, sh := range s.shards {
		sh.mu.Lock()
		sh.entries = map[string]entry{}
		sh.mu.Unlock()
	}
	s.tagMu.Lock()
	s.tagKeys = map[string]map[string]struct{}{}
	s.tagMu.Unlock()
	return nil
}

func (s *Store) Prefix() string { return s.prefix }

func (s *Store) AddTagEntries(_ context.Context, tag string, keys ...string) error {
	s.tagMu.Lock()
	defer s.tagMu.Unlock()
	set, ok := s.tagKeys[tag]
	if !ok {
		set = map[string]struct{}{}
		s.tagKeys[tag] = set
	}
	for _, key := range keys {
		set[key] = struct{}{}
	}
	return nil
}

func (s *Store) TagEntries(_ context.Context, tag string) ([]string, error) {
	s.tagMu.Lock()
	defer s.tagMu.Unlock()
	keys := make([]string, 0, len(s.tagKeys[tag]))
	for key := range s.tagKeys[tag] {
		keys = append(keys, key)
	}
	return keys, nil
}

func (s *Store) ForgetTagEntries(_ context.Context, tag string) error {
	s.tagMu.Lock()
	delete(s.tagKeys, tag)
	s.tagMu.Unlock()
	return nil
}

func init() {
	cache.Register("memory", func(_ context.Context, cfg *cache.Config) (cache.Store, error) {
		return New(Config{Prefix: cfg.Prefix, GCInterval: cache.MemoryGCInterval}), nil
	})
}
