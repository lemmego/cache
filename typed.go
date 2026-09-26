package cache

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// The methods below declare their own type parameters, which Go 1.27 permits
// only on concrete types — see the note on Cache. The ...As suffix follows the
// convention set by session.GetAs and config.LookupAs: a typed variant of a
// method that also exists untyped. The package-level functions in facade.go
// take the plain names, matching app.Get.

// GetAs returns the decoded value for key.
//
// The bool distinguishes a miss from a stored zero value, which matters for
// every type whose zero is meaningful — 0, false, "". A miss is not an error:
// err is reserved for a store that failed or a value that would not decode, so
// callers can write `if err != nil { return err }` and mean it.
func (c *Cache) GetAs[T any](ctx context.Context, key string) (T, bool, error) {
	var zero T

	raw, err := c.store.Get(ctx, key)
	if err != nil {
		if errors.Is(err, ErrMiss) {
			if c.events.enabled(bitMissed) {
				c.events.dispatch(CacheMissed{Key: key, Tags: c.tags})
			}
			return zero, false, nil
		}
		return zero, false, err
	}

	var value T
	if err := c.codec.Unmarshal(raw, &value); err != nil {
		return zero, false, fmt.Errorf("cache: decoding %q with the %s codec: %w", key, c.codec.Name(), err)
	}
	if c.events.enabled(bitHit) {
		c.events.dispatch(CacheHit{Key: key, Tags: c.tags, Value: raw})
	}
	return value, true, nil
}

// GetManyAs returns the decoded values present among keys. Absent keys are
// omitted rather than reported.
func (c *Cache) GetManyAs[T any](ctx context.Context, keys []string) (map[string]T, error) {
	raw, err := c.store.GetMany(ctx, keys)
	if err != nil {
		return nil, err
	}

	values := make(map[string]T, len(raw))
	for key, encoded := range raw {
		var value T
		if err := c.codec.Unmarshal(encoded, &value); err != nil {
			return nil, fmt.Errorf("cache: decoding %q with the %s codec: %w", key, c.codec.Name(), err)
		}
		values[key] = value
	}

	if c.events.enabled(bitHit) || c.events.enabled(bitMissed) {
		for _, key := range keys {
			if encoded, ok := raw[key]; ok {
				if c.events.enabled(bitHit) {
					c.events.dispatch(CacheHit{Key: key, Tags: c.tags, Value: encoded})
				}
			} else if c.events.enabled(bitMissed) {
				c.events.dispatch(CacheMissed{Key: key, Tags: c.tags})
			}
		}
	}
	return values, nil
}

// PullAs returns the decoded value for key and removes it.
func (c *Cache) PullAs[T any](ctx context.Context, key string) (T, bool, error) {
	value, found, err := c.GetAs[T](ctx, key)
	if err != nil || !found {
		return value, found, err
	}
	if _, err := c.Forget(ctx, key); err != nil {
		return value, true, err
	}
	return value, true, nil
}

// RememberOption configures a Remember call.
type RememberOption func(*rememberOptions)

type rememberOptions struct {
	lockFor       time.Duration
	lockWait      time.Duration
	retryInterval time.Duration
}

// LockFor makes a rebuild take a lock, so that across processes only one
// rebuilds the value.
//
// Without it, Remember collapses concurrent rebuilds within one process but
// not between processes — four servers missing the same key at once run the
// callback four times. With it, three of them wait for the winner and then read
// what it wrote. Use it when the callback is expensive enough that duplicating
// it matters, and note that it is only as strong as the store's lock: on a
// memory store it still only spans one process.
func LockFor(ttl time.Duration) RememberOption {
	return func(o *rememberOptions) {
		o.lockFor = ttl
		if o.lockWait == 0 {
			o.lockWait = ttl
		}
	}
}

// WaitFor sets how long a loser waits for the rebuild to finish.
func WaitFor(d time.Duration) RememberOption {
	return func(o *rememberOptions) { o.lockWait = d }
}

// RememberAs returns the cached value for key, computing and storing it on a
// miss.
//
// Concurrent callers in this process collapse to a single call to fn. If fn
// fails, nothing is cached and the error is returned — a failed computation
// must not be remembered as though it had succeeded.
func (c *Cache) RememberAs[T any](
	ctx context.Context,
	key string,
	ttl time.Duration,
	fn func(context.Context) (T, error),
	opts ...RememberOption,
) (T, error) {
	return c.remember(ctx, key, ttl, fn, opts)
}

// RememberForeverAs is RememberAs with no expiry.
func (c *Cache) RememberForeverAs[T any](
	ctx context.Context,
	key string,
	fn func(context.Context) (T, error),
	opts ...RememberOption,
) (T, error) {
	return c.remember(ctx, key, Forever, fn, opts)
}

func (c *Cache) remember[T any](
	ctx context.Context,
	key string,
	ttl time.Duration,
	fn func(context.Context) (T, error),
	opts []RememberOption,
) (T, error) {
	var zero T

	if value, found, err := c.GetAs[T](ctx, key); err != nil || found {
		return value, err
	}

	options := rememberOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	// singleflight keys on the cache key, so N goroutines that missed the same
	// key run fn once between them.
	result, err, _ := c.flight.Do(key, func() (any, error) {
		// The winner re-reads: another goroutine may have filled the key
		// between our miss and our turn here.
		if value, found, err := c.GetAs[T](ctx, key); err != nil || found {
			return value, err
		}
		if options.lockFor > 0 {
			return c.rebuildUnderLock(ctx, key, ttl, fn, options)
		}
		return c.rebuild(ctx, key, ttl, fn)
	})
	if err != nil {
		return zero, err
	}

	value, ok := result.(T)
	if !ok {
		return zero, fmt.Errorf("cache: remember produced %T, want %T", result, zero)
	}
	return value, nil
}

func (c *Cache) rebuild[T any](
	ctx context.Context,
	key string,
	ttl time.Duration,
	fn func(context.Context) (T, error),
) (any, error) {
	value, err := fn(ctx)
	if err != nil {
		return nil, err
	}
	if err := c.put(ctx, key, value, ttl); err != nil {
		return nil, err
	}
	return value, nil
}

// rebuildUnderLock lets one process rebuild while the others wait and re-read.
func (c *Cache) rebuildUnderLock[T any](
	ctx context.Context,
	key string,
	ttl time.Duration,
	fn func(context.Context) (T, error),
	options rememberOptions,
) (any, error) {
	lockOpts := []LockOption{}
	if options.retryInterval > 0 {
		lockOpts = append(lockOpts, WithRetryInterval(options.retryInterval))
	}
	lock := c.Lock("remember:"+key, options.lockFor, lockOpts...)

	acquired, err := lock.Acquire(ctx)
	if err != nil {
		// A store with no locks degrades to singleflight rather than failing:
		// the caller asked for stronger coordination than the store can give,
		// and a slightly duplicated rebuild beats a failed request.
		if errors.Is(err, ErrUnsupported) {
			return c.rebuild(ctx, key, ttl, fn)
		}
		return nil, err
	}

	if acquired {
		defer func() { _, _ = lock.Release(ctx) }()
		return c.rebuild(ctx, key, ttl, fn)
	}

	// Someone else is rebuilding. Wait for them, then read what they wrote.
	if _, err := lock.Block(ctx, options.lockWait); err != nil {
		return nil, err
	}
	if value, found, err := c.GetAs[T](ctx, key); err != nil || found {
		return value, err
	}

	// The winner finished without writing — it failed, or its value expired
	// immediately. Build it ourselves rather than returning nothing.
	return c.rebuild(ctx, key, ttl, fn)
}
