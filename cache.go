package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"golang.org/x/sync/singleflight"
)

// ErrInvalidTTL reports a non-positive TTL passed to Put.
//
// At the Store layer a non-positive TTL means forever. Here it is refused, so
// that an unset duration — a zero struct field, a config key that did not parse
// — is a loud error rather than a silent write of a value that never expires.
// Forever is how you ask for that on purpose.
var ErrInvalidTTL = errors.New("cache: ttl must be positive; use Forever to store without expiry")

// Cache is the API almost everything should use. It owns encoding, key
// prefixing and events, and delegates storage to a Store.
//
// Cache is a concrete type and there is deliberately no interface over it.
// Several of its methods declare their own type parameters, which Go 1.27
// permits only on concrete types: a method with type parameters can never
// satisfy an interface, so a Cacher abstraction carrying GetAs[T] would not
// compile and adding one later would silently delete the typed API.
type Cache struct {
	store  Store
	codec  Codec
	events *dispatcher
	tags   []string

	// flight collapses concurrent rebuilds of the same key within this
	// process, so a cold popular key is computed once rather than by every
	// goroutine that missed it.
	flight singleflight.Group
}

// Option configures a Cache.
type Option func(*Cache)

// WithCodec sets how values are encoded. The default is JSON.
func WithCodec(c Codec) Option {
	return func(cache *Cache) {
		if c != nil {
			cache.codec = c
		}
	}
}

// WithEventSink registers a listener for every event, which is how cache events
// are bridged onto an application's event emitter.
func WithEventSink(sink func(Event)) Option {
	return func(cache *Cache) { cache.events.setSink(sink) }
}

// New returns a Cache over store.
func New(store Store, opts ...Option) *Cache {
	c := &Cache{store: store, codec: JSONCodec{}, events: newDispatcher()}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Store returns the underlying driver, for the rare caller that needs bytes or
// a driver-specific capability.
func (c *Cache) Store() Store { return c.store }

// Codec returns the configured codec.
func (c *Cache) Codec() Codec { return c.codec }

// Listen registers a listener for one event name.
func (c *Cache) Listen(name string, l Listener) { c.events.listen(name, l) }

// Has reports whether a key is present and unexpired.
func (c *Cache) Has(ctx context.Context, key string) (bool, error) {
	_, err := c.store.Get(ctx, key)
	if err != nil {
		if errors.Is(err, ErrMiss) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Missing is the negation of Has.
func (c *Cache) Missing(ctx context.Context, key string) (bool, error) {
	has, err := c.Has(ctx, key)
	return !has, err
}

// Put stores value under key for ttl. A non-positive ttl is an error; use
// Forever to store without expiry.
func (c *Cache) Put(ctx context.Context, key string, value any, ttl time.Duration) error {
	if ttl <= 0 {
		return ErrInvalidTTL
	}
	return c.put(ctx, key, value, ttl)
}

// Forever stores value under key with no expiry.
func (c *Cache) Forever(ctx context.Context, key string, value any) error {
	return c.put(ctx, key, value, Forever)
}

func (c *Cache) put(ctx context.Context, key string, value any, ttl time.Duration) error {
	encoded, err := c.codec.Marshal(value)
	if err != nil {
		return fmt.Errorf("cache: encoding %q with the %s codec: %w", key, c.codec.Name(), err)
	}
	if err := c.store.Put(ctx, key, encoded, ttl); err != nil {
		return err
	}
	if c.events.enabled(bitKeyWritten) {
		c.events.dispatch(KeyWritten{Key: key, Tags: c.tags, Value: encoded, TTL: ttl})
	}
	return nil
}

// Add stores value only if key is absent, and reports whether it did.
func (c *Cache) Add(ctx context.Context, key string, value any, ttl time.Duration) (bool, error) {
	encoded, err := c.codec.Marshal(value)
	if err != nil {
		return false, fmt.Errorf("cache: encoding %q with the %s codec: %w", key, c.codec.Name(), err)
	}
	added, err := c.store.Add(ctx, key, encoded, ttl)
	if err != nil || !added {
		return added, err
	}
	if c.events.enabled(bitKeyWritten) {
		c.events.dispatch(KeyWritten{Key: key, Tags: c.tags, Value: encoded, TTL: ttl})
	}
	return true, nil
}

// Increment adds delta to a counter, preserving any expiry it already has.
func (c *Cache) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	return c.store.Increment(ctx, key, delta)
}

// Decrement subtracts delta from a counter.
func (c *Cache) Decrement(ctx context.Context, key string, delta int64) (int64, error) {
	return c.store.Decrement(ctx, key, delta)
}

// Forget removes a key and reports whether it was present.
func (c *Cache) Forget(ctx context.Context, key string) (bool, error) {
	forgotten, err := c.store.Forget(ctx, key)
	if err != nil {
		return false, err
	}
	if forgotten && c.events.enabled(bitKeyForgotten) {
		c.events.dispatch(KeyForgotten{Key: key, Tags: c.tags})
	}
	return forgotten, nil
}

// Flush empties the cache — or, on a tagged cache, just that tag namespace.
func (c *Cache) Flush(ctx context.Context) error {
	if err := c.store.Flush(ctx); err != nil {
		return err
	}
	if c.events.enabled(bitFlushed) {
		c.events.dispatch(CacheFlushed{Tags: c.tags})
	}
	return nil
}

// Lock returns an atomic lock named name.
//
// The scope of the exclusion is the scope of the store: one process for memory,
// one host for file, the cluster for redis. A store that cannot lock returns a
// lock that refuses rather than one that pretends.
func (c *Cache) Lock(name string, ttl time.Duration, opts ...LockOption) Lock {
	resolved := resolveLockOptions(opts)
	provider, ok := c.store.(LockProvider)
	if !ok {
		return unsupportedLock{owner: resolved.owner}
	}
	return provider.NewLock(name, resolved.owner, ttl)
}

// RestoreLock rebinds a lock to a known owner token, so a lock taken in one
// place can be released in another — acquired in a web request, released by the
// job the request queued.
func (c *Cache) RestoreLock(name, owner string, ttl time.Duration) Lock {
	return c.Lock(name, ttl, WithOwner(owner))
}

// Tags returns a view of the cache namespaced by tags, so a group of entries
// can be invalidated together.
func (c *Cache) Tags(names ...string) *Cache {
	if len(names) == 0 {
		return c
	}
	tagged := newTaggedStore(c.store, c.codec, names)
	clone := &Cache{store: tagged, codec: c.codec, events: c.events, tags: tagged.tags}
	return clone
}
