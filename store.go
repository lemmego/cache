// Package cache provides a driver-backed cache with typed access, atomic locks,
// tagged invalidation and events.
//
// There are two layers. [Store] is the driver contract: it deals only in
// []byte, so a driver never has to know how a value is encoded. [Cache] sits
// above it and owns encoding, key prefixing, events and the generic API most
// callers want.
package cache

import (
	"context"
	"errors"
	"time"
)

// Forever is the TTL that means "this entry does not expire".
//
// Note the deliberate difference from Laravel, where a non-positive TTL deletes
// the key. A zero time.Duration is what an uninitialised struct field holds, and
// "unset config silently deletes your cache entry" is a trap. At this layer a
// non-positive TTL therefore stores forever; the [Cache] layer removes the
// ambiguity entirely by rejecting a non-positive TTL in Put and offering
// Cache.Forever as the only way to write a non-expiring entry.
const Forever time.Duration = 0

var (
	// ErrMiss reports that a key is not in the cache. Drivers return it from
	// Get rather than a nil value, so a cached empty value stays
	// distinguishable from an absent one, and so the distinction survives
	// error wrapping through decorators such as the tagged store.
	ErrMiss = errors.New("cache: key not found")

	// ErrNotNumeric reports an Increment or Decrement against a value that is
	// not a counter.
	ErrNotNumeric = errors.New("cache: value is not numeric")

	// ErrUnsupported reports that a driver cannot perform the operation. It is
	// always better than a silent no-op: a lock that pretends to have been
	// acquired is worse than one that refuses.
	ErrUnsupported = errors.New("cache: operation not supported by this store")
)

// Store is the driver contract.
//
// Implementations deal only in bytes. Every method takes a context so drivers
// that talk over a network can honour cancellation; drivers that cannot are
// free to ignore it, and say so in their documentation.
//
// A Store must be safe for concurrent use.
type Store interface {
	// Get returns the raw value for key, or ErrMiss if it is absent or expired.
	Get(ctx context.Context, key string) ([]byte, error)

	// GetMany returns the values present among keys. Absent keys are omitted
	// from the map rather than reported as ErrMiss: in a bulk read, some
	// missing is the normal case and not a failure.
	GetMany(ctx context.Context, keys []string) (map[string][]byte, error)

	// Put stores value under key. A non-positive ttl stores it forever.
	Put(ctx context.Context, key string, value []byte, ttl time.Duration) error

	// PutMany stores every pair under one shared expiry.
	PutMany(ctx context.Context, values map[string][]byte, ttl time.Duration) error

	// Add stores value only if key is absent, and reports whether it did.
	// It must be atomic: it is the primitive locks are built on.
	Add(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error)

	// Increment adds delta to the counter at key and returns the new value,
	// creating the key at delta if it is absent. An existing expiry is
	// preserved. A non-counter value returns ErrNotNumeric.
	Increment(ctx context.Context, key string, delta int64) (int64, error)

	// Decrement subtracts delta from the counter at key.
	Decrement(ctx context.Context, key string, delta int64) (int64, error)

	// Forget removes key and reports whether it was present.
	Forget(ctx context.Context, key string) (bool, error)

	// Flush removes every entry this store owns.
	//
	// It must not remove anything else. A store sharing its backing with other
	// data — a redis database that also holds sessions, a directory that also
	// holds other files — must scope the removal to its own prefix.
	Flush(ctx context.Context) error

	// Prefix returns the namespace every key this store writes lives under.
	Prefix() string
}

// The interfaces below are capabilities a driver opts into. Callers type-assert
// for them and fall back to ErrUnsupported, which is how a store that cannot
// lock refuses rather than pretending.

// LockProvider is implemented by stores that can back an atomic lock.
type LockProvider interface {
	NewLock(name, owner string, ttl time.Duration) Lock
}

// TagIndex is implemented by stores that can enumerate the keys written under a
// tag. Tag invalidation works without it by bumping a version, but only a store
// with an index can flush entries written with Forever.
type TagIndex interface {
	AddTagEntries(ctx context.Context, tag string, keys ...string) error
	TagEntries(ctx context.Context, tag string) ([]string, error)
	ForgetTagEntries(ctx context.Context, tag string) error
}

// Pruner is implemented by stores that need an explicit sweep to reclaim
// expired entries, rather than reclaiming them in the background.
type Pruner interface {
	Prune(ctx context.Context) error
}
