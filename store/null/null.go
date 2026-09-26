// Package null provides a cache store that discards everything.
//
// It is what CACHE_STORE=null configures, what tests use when they want the
// cache out of the way, and what a lenient provider falls back to when its real
// backend is unreachable. Reads always miss, so callers exercise their
// cache-miss path on every request.
package null

import (
	"context"
	"time"

	"github.com/lemmego/cache"
)

type Store struct{ prefix string }

var _ cache.Store = (*Store)(nil)

// New returns a store that keeps nothing.
func New(prefix string) *Store { return &Store{prefix: prefix} }

func (s *Store) Get(context.Context, string) ([]byte, error) { return nil, cache.ErrMiss }

func (s *Store) GetMany(context.Context, []string) (map[string][]byte, error) {
	return map[string][]byte{}, nil
}

func (s *Store) Put(context.Context, string, []byte, time.Duration) error { return nil }

func (s *Store) PutMany(context.Context, map[string][]byte, time.Duration) error { return nil }

// Add always reports success. Nothing was stored, so the next Add succeeds too
// — which is exactly why a lock over this store provides no exclusion at all.
func (s *Store) Add(context.Context, string, []byte, time.Duration) (bool, error) {
	return true, nil
}

// Increment returns what the counter would have been without storing it.
func (s *Store) Increment(_ context.Context, _ string, delta int64) (int64, error) {
	return delta, nil
}

func (s *Store) Decrement(ctx context.Context, key string, delta int64) (int64, error) {
	return s.Increment(ctx, key, -delta)
}

func (s *Store) Forget(context.Context, string) (bool, error) { return false, nil }

func (s *Store) Flush(context.Context) error { return nil }

func (s *Store) Prefix() string { return s.prefix }

func init() {
	cache.Register("null", func(_ context.Context, cfg *cache.Config) (cache.Store, error) {
		return New(cfg.Prefix), nil
	})
}
