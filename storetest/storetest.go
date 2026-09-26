// Package storetest is a conformance suite every cache store must pass.
//
// It is exported rather than kept internal so that a store living outside this
// repository can prove itself against the same contract the built-in drivers
// are held to. Run it from a driver's own test file:
//
//	func TestConformance(t *testing.T) {
//	    storetest.Run(t, storetest.Capabilities{...}, func(t *testing.T) (cache.Store, storetest.Clock) {
//	        ...
//	    })
//	}
package storetest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lemmego/cache"
)

// Clock advances a store's view of time.
//
// The suite never sleeps to test expiry. Sleeping makes the suite slow and
// flaky, and a store whose clock cannot be advanced (redis under miniredis)
// would otherwise have its TTL cases pass vacuously.
type Clock interface {
	Advance(d time.Duration)
}

// Factory builds a store to test, and the clock that drives it.
type Factory func(t *testing.T) (cache.Store, Clock)

// Capabilities declares what the store under test can do, so the suite skips
// the cases that do not apply rather than failing them.
type Capabilities struct {
	// Locks: the store implements cache.LockProvider.
	Locks bool
	// TagIndex: the store implements cache.TagIndex.
	TagIndex bool
	// AtomicIncrement: concurrent increments do not lose updates.
	AtomicIncrement bool
	// HonoursContext: operations respect context cancellation.
	HonoursContext bool
	// ForeignKeySafe: Flush removes only this store's own entries.
	ForeignKeySafe bool
	// Persists: values written survive; false for the null store, which
	// turns the suite into a check that nothing panics.
	Persists bool
}

// Run executes the conformance suite.
func Run(t *testing.T, caps Capabilities, newStore Factory) {
	t.Helper()

	if !caps.Persists {
		runNonPersistent(t, newStore)
		return
	}

	t.Run("ByteFidelity", func(t *testing.T) { testByteFidelity(t, newStore) })
	t.Run("Miss", func(t *testing.T) { testMiss(t, newStore) })
	t.Run("Expiry", func(t *testing.T) { testExpiry(t, newStore) })
	t.Run("Forever", func(t *testing.T) { testForever(t, newStore) })
	t.Run("GetMany", func(t *testing.T) { testGetMany(t, newStore) })
	t.Run("PutMany", func(t *testing.T) { testPutMany(t, newStore) })
	t.Run("Add", func(t *testing.T) { testAdd(t, newStore) })
	t.Run("Counters", func(t *testing.T) { testCounters(t, newStore) })
	t.Run("IncrementPreservesTTL", func(t *testing.T) { testIncrementPreservesTTL(t, newStore) })
	t.Run("Forget", func(t *testing.T) { testForget(t, newStore) })
	t.Run("Flush", func(t *testing.T) { testFlush(t, newStore) })
	t.Run("Prefix", func(t *testing.T) { testPrefix(t, newStore) })

	if caps.ForeignKeySafe {
		t.Run("FlushLeavesForeignKeys", func(t *testing.T) { testFlushLeavesForeignKeys(t, newStore) })
	}
	if caps.AtomicIncrement {
		t.Run("ConcurrentIncrement", func(t *testing.T) { testConcurrentIncrement(t, newStore) })
	}
	if caps.Locks {
		t.Run("Locks", func(t *testing.T) { testLocks(t, newStore) })
	}
	if caps.TagIndex {
		t.Run("TagIndex", func(t *testing.T) { testTagIndex(t, newStore) })
	}
	if caps.HonoursContext {
		t.Run("ContextCancellation", func(t *testing.T) { testContextCancellation(t, newStore) })
	}
}

func runNonPersistent(t *testing.T, newStore Factory) {
	t.Run("ReadsAlwaysMiss", func(t *testing.T) {
		store, _ := newStore(t)
		ctx := context.Background()

		if err := store.Put(ctx, "k", []byte("v"), time.Minute); err != nil {
			t.Fatalf("Put() error = %v", err)
		}
		if _, err := store.Get(ctx, "k"); !errors.Is(err, cache.ErrMiss) {
			t.Fatalf("Get() error = %v, want ErrMiss", err)
		}
		if err := store.Flush(ctx); err != nil {
			t.Fatalf("Flush() error = %v", err)
		}
	})
}

// A cache must return exactly the bytes it was given. The empty slice is the
// interesting case: it must read back as a hit, not as a miss.
func testByteFidelity(t *testing.T, newStore Factory) {
	store, _ := newStore(t)
	ctx := context.Background()

	values := map[string][]byte{
		"plain":  []byte("hello"),
		"empty":  {},
		"binary": {0x00, 0xff, 0x00, 0x1b, '\n'},
		"json":   []byte(`{"a":1}`),
	}
	for key, want := range values {
		if err := store.Put(ctx, key, want, time.Minute); err != nil {
			t.Fatalf("Put(%q) error = %v", key, err)
		}
	}
	for key, want := range values {
		got, err := store.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get(%q) error = %v", key, err)
		}
		if string(got) != string(want) {
			t.Errorf("Get(%q) = %q, want %q", key, got, want)
		}
	}
}

func testMiss(t *testing.T, newStore Factory) {
	store, _ := newStore(t)

	value, err := store.Get(context.Background(), "absent")
	if !errors.Is(err, cache.ErrMiss) {
		t.Fatalf("Get() error = %v, want ErrMiss", err)
	}
	if value != nil {
		t.Errorf("Get() value = %q, want nil on a miss", value)
	}
}

func testExpiry(t *testing.T, newStore Factory) {
	store, clock := newStore(t)
	ctx := context.Background()

	if err := store.Put(ctx, "short", []byte("v"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, "short"); err != nil {
		t.Fatalf("Get() before expiry error = %v", err)
	}

	clock.Advance(61 * time.Second)

	if _, err := store.Get(ctx, "short"); !errors.Is(err, cache.ErrMiss) {
		t.Fatalf("Get() after expiry error = %v, want ErrMiss", err)
	}
}

func testForever(t *testing.T, newStore Factory) {
	store, clock := newStore(t)
	ctx := context.Background()

	if err := store.Put(ctx, "kept", []byte("v"), cache.Forever); err != nil {
		t.Fatal(err)
	}
	clock.Advance(365 * 24 * time.Hour)

	if _, err := store.Get(ctx, "kept"); err != nil {
		t.Fatalf("Get() after a year error = %v, want the value", err)
	}
}

// Absent keys are omitted rather than reported as an error: in a bulk read,
// some missing is the normal case.
func testGetMany(t *testing.T, newStore Factory) {
	store, _ := newStore(t)
	ctx := context.Background()

	if err := store.Put(ctx, "a", []byte("1"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, "c", []byte("3"), time.Minute); err != nil {
		t.Fatal(err)
	}

	found, err := store.GetMany(ctx, []string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("GetMany() error = %v", err)
	}
	if len(found) != 2 {
		t.Fatalf("GetMany() returned %d entries, want 2: %v", len(found), found)
	}
	if string(found["a"]) != "1" || string(found["c"]) != "3" {
		t.Errorf("GetMany() = %v", found)
	}
	if _, ok := found["b"]; ok {
		t.Error("GetMany() included an absent key")
	}
}

func testPutMany(t *testing.T, newStore Factory) {
	store, clock := newStore(t)
	ctx := context.Background()

	err := store.PutMany(ctx, map[string][]byte{"x": []byte("1"), "y": []byte("2")}, time.Minute)
	if err != nil {
		t.Fatalf("PutMany() error = %v", err)
	}
	found, err := store.GetMany(ctx, []string{"x", "y"})
	if err != nil || len(found) != 2 {
		t.Fatalf("GetMany() = %v, %v", found, err)
	}

	clock.Advance(61 * time.Second)
	if found, _ := store.GetMany(ctx, []string{"x", "y"}); len(found) != 0 {
		t.Errorf("entries outlived the shared expiry: %v", found)
	}
}

func testAdd(t *testing.T, newStore Factory) {
	store, clock := newStore(t)
	ctx := context.Background()

	added, err := store.Add(ctx, "once", []byte("first"), time.Minute)
	if err != nil || !added {
		t.Fatalf("Add() = %v, %v; want true", added, err)
	}

	added, err = store.Add(ctx, "once", []byte("second"), time.Minute)
	if err != nil || added {
		t.Fatalf("Add() on an existing key = %v, %v; want false", added, err)
	}
	if got, _ := store.Get(ctx, "once"); string(got) != "first" {
		t.Errorf("a refused Add overwrote the value: %q", got)
	}

	clock.Advance(61 * time.Second)
	added, err = store.Add(ctx, "once", []byte("third"), time.Minute)
	if err != nil || !added {
		t.Fatalf("Add() after expiry = %v, %v; want true", added, err)
	}
}

func testCounters(t *testing.T, newStore Factory) {
	store, _ := newStore(t)
	ctx := context.Background()

	got, err := store.Increment(ctx, "hits", 1)
	if err != nil || got != 1 {
		t.Fatalf("Increment() on an absent key = %d, %v; want 1", got, err)
	}
	if got, err = store.Increment(ctx, "hits", 4); err != nil || got != 5 {
		t.Fatalf("Increment() = %d, %v; want 5", got, err)
	}
	if got, err = store.Decrement(ctx, "hits", 2); err != nil || got != 3 {
		t.Fatalf("Decrement() = %d, %v; want 3", got, err)
	}
	if got, err = store.Decrement(ctx, "hits", 10); err != nil || got != -7 {
		t.Fatalf("Decrement() below zero = %d, %v; want -7", got, err)
	}

	// A non-counter must be reported, not panic. The previous file store
	// panicked here, which took the process down.
	if err := store.Put(ctx, "name", []byte("Ada"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Increment(ctx, "name", 1); !errors.Is(err, cache.ErrNotNumeric) {
		t.Fatalf("Increment() on a non-counter error = %v, want ErrNotNumeric", err)
	}
}

// Regression: incrementing a counter must not reset its expiry. A rate limiter
// built on a counter with a one-minute window would otherwise never reset.
func testIncrementPreservesTTL(t *testing.T, newStore Factory) {
	store, clock := newStore(t)
	ctx := context.Background()

	if err := store.Put(ctx, "window", cache.FormatCounter(1), time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Increment(ctx, "window", 1); err != nil {
		t.Fatal(err)
	}

	clock.Advance(61 * time.Second)

	if _, err := store.Get(ctx, "window"); !errors.Is(err, cache.ErrMiss) {
		t.Fatal("Increment() reset the expiry: the counter outlived its window")
	}
}

func testForget(t *testing.T, newStore Factory) {
	store, _ := newStore(t)
	ctx := context.Background()

	if err := store.Put(ctx, "gone", []byte("v"), time.Minute); err != nil {
		t.Fatal(err)
	}
	forgotten, err := store.Forget(ctx, "gone")
	if err != nil || !forgotten {
		t.Fatalf("Forget() = %v, %v; want true", forgotten, err)
	}
	if _, err := store.Get(ctx, "gone"); !errors.Is(err, cache.ErrMiss) {
		t.Fatalf("Get() after Forget error = %v, want ErrMiss", err)
	}
	if forgotten, err = store.Forget(ctx, "gone"); err != nil || forgotten {
		t.Fatalf("Forget() on an absent key = %v, %v; want false", forgotten, err)
	}
}

func testFlush(t *testing.T, newStore Factory) {
	store, _ := newStore(t)
	ctx := context.Background()

	for _, key := range []string{"a", "b", "c"} {
		if err := store.Put(ctx, key, []byte("v"), time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Flush(ctx); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	for _, key := range []string{"a", "b", "c"} {
		if _, err := store.Get(ctx, key); !errors.Is(err, cache.ErrMiss) {
			t.Errorf("Get(%q) after Flush error = %v, want ErrMiss", key, err)
		}
	}
}

func testPrefix(t *testing.T, newStore Factory) {
	store, _ := newStore(t)
	if store.Prefix() == "" {
		t.Error("Prefix() is empty; every store must namespace its keys")
	}
}

// Regression: Flush must remove only what the store itself owns. The previous
// file store deleted every file under its directory, and a redis store that
// reaches for FLUSHDB would destroy the sessions sharing its database.
func testFlushLeavesForeignKeys(t *testing.T, newStore Factory) {
	store, _ := newStore(t)
	ctx := context.Background()

	foreign, ok := store.(interface {
		PutForeign(ctx context.Context, key string, value []byte) error
		GetForeign(ctx context.Context, key string) ([]byte, error)
	})
	if !ok {
		t.Skip("store does not expose foreign-key helpers")
	}

	if err := foreign.PutForeign(ctx, "not-ours", []byte("keep me")); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, "ours", []byte("v"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := store.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	got, err := foreign.GetForeign(ctx, "not-ours")
	if err != nil {
		t.Fatalf("Flush() destroyed data the store does not own: %v", err)
	}
	if string(got) != "keep me" {
		t.Errorf("foreign value = %q, want %q", got, "keep me")
	}
}

func testConcurrentIncrement(t *testing.T, newStore Factory) {
	store, _ := newStore(t)
	ctx := context.Background()

	const goroutines, perGoroutine = 8, 250
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range perGoroutine {
				if _, err := store.Increment(ctx, "total", 1); err != nil {
					t.Errorf("Increment() error = %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	got, err := store.Increment(ctx, "total", 0)
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(goroutines * perGoroutine); got != want {
		t.Errorf("counter = %d, want %d: concurrent increments lost updates", got, want)
	}
}

func testLocks(t *testing.T, newStore Factory) {
	store, clock := newStore(t)
	provider, ok := store.(cache.LockProvider)
	if !ok {
		t.Fatal("Capabilities.Locks is set but the store is not a cache.LockProvider")
	}
	ctx := context.Background()

	t.Run("ExclusionAndRelease", func(t *testing.T) {
		first := provider.NewLock("job", "owner-a", time.Minute)
		second := provider.NewLock("job", "owner-b", time.Minute)

		acquired, err := first.Acquire(ctx)
		if err != nil || !acquired {
			t.Fatalf("Acquire() = %v, %v; want true", acquired, err)
		}
		acquired, err = second.Acquire(ctx)
		if err != nil || acquired {
			t.Fatalf("a second holder acquired the lock: %v, %v", acquired, err)
		}

		released, err := first.Release(ctx)
		if err != nil || !released {
			t.Fatalf("Release() = %v, %v; want true", released, err)
		}
		if acquired, err = second.Acquire(ctx); err != nil || !acquired {
			t.Fatalf("Acquire() after release = %v, %v; want true", acquired, err)
		}
	})

	// The reason locks carry an owner token at all. Without this check, a
	// holder whose TTL expired mid-work would release the lock that someone
	// else had already taken.
	t.Run("ReleaseByAnotherOwnerIsRefused", func(t *testing.T) {
		holder := provider.NewLock("owned", "owner-a", time.Minute)
		other := provider.NewLock("owned", "owner-b", time.Minute)

		if acquired, err := holder.Acquire(ctx); err != nil || !acquired {
			t.Fatalf("Acquire() = %v, %v", acquired, err)
		}
		released, err := other.Release(ctx)
		if err != nil {
			t.Fatalf("Release() error = %v", err)
		}
		if released {
			t.Fatal("a non-holder released the lock")
		}
		if acquired, _ := other.Acquire(ctx); acquired {
			t.Fatal("the refused Release still deleted the lock")
		}
	})

	t.Run("ExpiryReleases", func(t *testing.T) {
		holder := provider.NewLock("expiring", "owner-a", time.Minute)
		if acquired, err := holder.Acquire(ctx); err != nil || !acquired {
			t.Fatalf("Acquire() = %v, %v", acquired, err)
		}
		clock.Advance(61 * time.Second)

		next := provider.NewLock("expiring", "owner-b", time.Minute)
		if acquired, err := next.Acquire(ctx); err != nil || !acquired {
			t.Fatalf("Acquire() after expiry = %v, %v; want true", acquired, err)
		}
	})

	t.Run("GetReleasesOnPanic", func(t *testing.T) {
		holder := provider.NewLock("panicking", "owner-a", time.Minute)

		func() {
			defer func() { _ = recover() }()
			_, _ = holder.Get(ctx, func(context.Context) error { panic("boom") })
		}()

		next := provider.NewLock("panicking", "owner-b", time.Minute)
		if acquired, err := next.Acquire(ctx); err != nil || !acquired {
			t.Fatalf("the lock was still held after a panic: %v, %v", acquired, err)
		}
	})

	t.Run("GetSkipsWhenHeld", func(t *testing.T) {
		holder := provider.NewLock("busy", "owner-a", time.Minute)
		if acquired, _ := holder.Acquire(ctx); !acquired {
			t.Fatal("setup: could not acquire")
		}

		ran := false
		other := provider.NewLock("busy", "owner-b", time.Minute)
		acquired, err := other.Get(ctx, func(context.Context) error { ran = true; return nil })
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		if acquired || ran {
			t.Error("Get() ran the callback without holding the lock")
		}
	})

	// A busy lock is an expected outcome, so Block reports it as (false, nil)
	// rather than an error.
	t.Run("BlockTimesOutWithoutError", func(t *testing.T) {
		holder := provider.NewLock("contended", "owner-a", time.Minute)
		if acquired, _ := holder.Acquire(ctx); !acquired {
			t.Fatal("setup: could not acquire")
		}

		other := provider.NewLock("contended", "owner-b", time.Minute)
		start := time.Now()
		acquired, err := other.Block(ctx, 120*time.Millisecond)
		if err != nil {
			t.Fatalf("Block() error = %v, want nil on timeout", err)
		}
		if acquired {
			t.Fatal("Block() acquired a held lock")
		}
		if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
			t.Errorf("Block() returned after %v, want it to wait out the deadline", elapsed)
		}
	})
}

func testTagIndex(t *testing.T, newStore Factory) {
	store, _ := newStore(t)
	index, ok := store.(cache.TagIndex)
	if !ok {
		t.Fatal("Capabilities.TagIndex is set but the store is not a cache.TagIndex")
	}
	ctx := context.Background()

	if err := index.AddTagEntries(ctx, "posts", "a", "b"); err != nil {
		t.Fatalf("AddTagEntries() error = %v", err)
	}
	if err := index.AddTagEntries(ctx, "posts", "b", "c"); err != nil {
		t.Fatal(err)
	}

	entries, err := index.TagEntries(ctx, "posts")
	if err != nil {
		t.Fatalf("TagEntries() error = %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("TagEntries() = %v, want 3 distinct keys", entries)
	}

	if err := index.ForgetTagEntries(ctx, "posts"); err != nil {
		t.Fatal(err)
	}
	if entries, _ = index.TagEntries(ctx, "posts"); len(entries) != 0 {
		t.Errorf("TagEntries() after forget = %v, want none", entries)
	}
}

func testContextCancellation(t *testing.T, newStore Factory) {
	store, _ := newStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := store.Put(ctx, "k", []byte("v"), time.Minute); err == nil {
		t.Error("Put() with a cancelled context returned nil")
	}
	if _, err := store.Get(ctx, "k"); err == nil || errors.Is(err, cache.ErrMiss) {
		t.Errorf("Get() with a cancelled context error = %v, want a cancellation error", err)
	}
}

// KeysFor builds a set of distinct keys, for tests that need bulk data.
func KeysFor(prefix string, n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("%s%d", prefix, i)
	}
	return keys
}
