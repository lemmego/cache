package cache

import (
	"context"
	"errors"
	"time"
)

// ErrNotInitialized reports a package-level call made before the provider has
// installed a cache.
//
// The package functions return it rather than panicking, so an application that
// has not configured a cache fails the one call rather than the process — and
// so a caller can treat a missing cache as a permanent miss if it wants to.
var ErrNotInitialized = errors.New("cache: provider is not initialized")

func manager() (*Cache, error) {
	if c := Global(); c != nil {
		return c, nil
	}
	return nil, ErrNotInitialized
}

// Get returns the decoded value for key from the installed cache.
func Get[T any](ctx context.Context, key string) (T, bool, error) {
	var zero T
	c, err := manager()
	if err != nil {
		return zero, false, err
	}
	return c.GetAs[T](ctx, key)
}

// GetMany returns the decoded values present among keys.
func GetMany[T any](ctx context.Context, keys []string) (map[string]T, error) {
	c, err := manager()
	if err != nil {
		return nil, err
	}
	return c.GetManyAs[T](ctx, keys)
}

// Pull returns the decoded value for key and removes it.
func Pull[T any](ctx context.Context, key string) (T, bool, error) {
	var zero T
	c, err := manager()
	if err != nil {
		return zero, false, err
	}
	return c.PullAs[T](ctx, key)
}

// Remember returns the cached value for key, computing and storing it on a
// miss.
func Remember[T any](ctx context.Context, key string, ttl time.Duration, fn func(context.Context) (T, error), opts ...RememberOption) (T, error) {
	var zero T
	c, err := manager()
	if err != nil {
		return zero, err
	}
	return c.RememberAs(ctx, key, ttl, fn, opts...)
}

// RememberForever is Remember with no expiry.
func RememberForever[T any](ctx context.Context, key string, fn func(context.Context) (T, error), opts ...RememberOption) (T, error) {
	var zero T
	c, err := manager()
	if err != nil {
		return zero, err
	}
	return c.RememberForeverAs(ctx, key, fn, opts...)
}

// Put stores value under key for ttl.
func Put(ctx context.Context, key string, value any, ttl time.Duration) error {
	c, err := manager()
	if err != nil {
		return err
	}
	return c.Put(ctx, key, value, ttl)
}

// PutForever stores value under key with no expiry.
func PutForever(ctx context.Context, key string, value any) error {
	c, err := manager()
	if err != nil {
		return err
	}
	return c.Forever(ctx, key, value)
}

// Add stores value only if key is absent, and reports whether it did.
func Add(ctx context.Context, key string, value any, ttl time.Duration) (bool, error) {
	c, err := manager()
	if err != nil {
		return false, err
	}
	return c.Add(ctx, key, value, ttl)
}

// Has reports whether key is present.
func Has(ctx context.Context, key string) (bool, error) {
	c, err := manager()
	if err != nil {
		return false, err
	}
	return c.Has(ctx, key)
}

// Increment adds delta to a counter.
func Increment(ctx context.Context, key string, delta int64) (int64, error) {
	c, err := manager()
	if err != nil {
		return 0, err
	}
	return c.Increment(ctx, key, delta)
}

// Decrement subtracts delta from a counter.
func Decrement(ctx context.Context, key string, delta int64) (int64, error) {
	c, err := manager()
	if err != nil {
		return 0, err
	}
	return c.Decrement(ctx, key, delta)
}

// Forget removes key and reports whether it was present.
func Forget(ctx context.Context, key string) (bool, error) {
	c, err := manager()
	if err != nil {
		return false, err
	}
	return c.Forget(ctx, key)
}

// Flush empties the cache.
func Flush(ctx context.Context) error {
	c, err := manager()
	if err != nil {
		return err
	}
	return c.Flush(ctx)
}

// Tags returns a tagged view of the installed cache. With no cache installed it
// returns nil, and the methods on a nil *Cache would panic — so prefer the
// method form, cache.Global().Tags(...), where the cache may be absent.
func Tags(names ...string) *Cache {
	c, err := manager()
	if err != nil {
		return nil
	}
	return c.Tags(names...)
}

// NewLock returns an atomic lock from the installed cache.
//
// It is NewLock rather than Lock because Lock is the interface it returns. With
// no cache installed the lock refuses to acquire rather than pretending, so a
// caller that does not check never proceeds as though it held one.
func NewLock(name string, ttl time.Duration, opts ...LockOption) Lock {
	c, err := manager()
	if err != nil {
		return unsupportedLock{owner: resolveLockOptions(opts).owner}
	}
	return c.Lock(name, ttl, opts...)
}
