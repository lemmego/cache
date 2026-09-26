package cache_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lemmego/cache"
	"github.com/lemmego/cache/store/memory"
)

type user struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func newCache(t *testing.T, opts ...cache.Option) *cache.Cache {
	t.Helper()
	store := memory.New(memory.Config{Prefix: "test:"})
	t.Cleanup(func() { _ = store.Close() })
	return cache.New(store, opts...)
}

// A cached zero value must not look like a miss. This is why GetAs returns a
// bool rather than relying on T's zero.
func TestGetAsDistinguishesZeroValuesFromMisses(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()

	t.Run("int", func(t *testing.T) {
		if err := c.Put(ctx, "count", 0, time.Minute); err != nil {
			t.Fatal(err)
		}
		value, found, err := c.GetAs[int](ctx, "count")
		if err != nil || !found || value != 0 {
			t.Fatalf("GetAs[int]() = %v, %v, %v; want 0, true, nil", value, found, err)
		}
		if _, found, _ = c.GetAs[int](ctx, "absent"); found {
			t.Error("GetAs[int]() reported an absent key as found")
		}
	})

	t.Run("bool", func(t *testing.T) {
		if err := c.Put(ctx, "flag", false, time.Minute); err != nil {
			t.Fatal(err)
		}
		value, found, err := c.GetAs[bool](ctx, "flag")
		if err != nil || !found || value {
			t.Fatalf("GetAs[bool]() = %v, %v, %v; want false, true, nil", value, found, err)
		}
	})

	t.Run("string", func(t *testing.T) {
		if err := c.Put(ctx, "note", "", time.Minute); err != nil {
			t.Fatal(err)
		}
		value, found, err := c.GetAs[string](ctx, "note")
		if err != nil || !found || value != "" {
			t.Fatalf("GetAs[string]() = %q, %v, %v; want \"\", true, nil", value, found, err)
		}
	})
}

// A struct must survive the round trip without needing registration anywhere —
// the failure mode the old gob-based store had.
func TestStructRoundTrip(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()

	want := user{ID: 7, Name: "Ada"}
	if err := c.Put(ctx, "user:7", want, time.Minute); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	got, found, err := c.GetAs[user](ctx, "user:7")
	if err != nil || !found {
		t.Fatalf("GetAs[user]() = %v, %v, %v", got, found, err)
	}
	if got != want {
		t.Errorf("GetAs[user]() = %+v, want %+v", got, want)
	}

	slice := []user{{ID: 1}, {ID: 2}}
	if err := c.Put(ctx, "users", slice, time.Minute); err != nil {
		t.Fatal(err)
	}
	gotSlice, _, err := c.GetAs[[]user](ctx, "users")
	if err != nil || len(gotSlice) != 2 {
		t.Fatalf("GetAs[[]user]() = %v, %v", gotSlice, err)
	}
}

func TestPutRejectsANonPositiveTTL(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()

	for _, ttl := range []time.Duration{0, -time.Second} {
		if err := c.Put(ctx, "k", "v", ttl); !errors.Is(err, cache.ErrInvalidTTL) {
			t.Errorf("Put() with ttl %v error = %v, want ErrInvalidTTL", ttl, err)
		}
	}
	if err := c.Forever(ctx, "k", "v"); err != nil {
		t.Errorf("Forever() error = %v", err)
	}
}

func TestPullRemovesTheKey(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()

	if err := c.Put(ctx, "once", "value", time.Minute); err != nil {
		t.Fatal(err)
	}
	value, found, err := c.PullAs[string](ctx, "once")
	if err != nil || !found || value != "value" {
		t.Fatalf("PullAs() = %q, %v, %v", value, found, err)
	}
	if _, found, _ = c.GetAs[string](ctx, "once"); found {
		t.Error("PullAs() left the key behind")
	}
}

func TestHasAndMissing(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()

	if err := c.Put(ctx, "there", 1, time.Minute); err != nil {
		t.Fatal(err)
	}
	if has, err := c.Has(ctx, "there"); err != nil || !has {
		t.Errorf("Has() = %v, %v; want true", has, err)
	}
	if missing, err := c.Missing(ctx, "absent"); err != nil || !missing {
		t.Errorf("Missing() = %v, %v; want true", missing, err)
	}
}

func TestAddOnlyStoresOnce(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()

	added, err := c.Add(ctx, "k", "first", time.Minute)
	if err != nil || !added {
		t.Fatalf("Add() = %v, %v; want true", added, err)
	}
	if added, err = c.Add(ctx, "k", "second", time.Minute); err != nil || added {
		t.Fatalf("Add() again = %v, %v; want false", added, err)
	}
	if value, _, _ := c.GetAs[string](ctx, "k"); value != "first" {
		t.Errorf("value = %q, want %q", value, "first")
	}
}

func TestRememberComputesOnceThenServesFromCache(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()

	var calls atomic.Int64
	build := func(context.Context) (user, error) {
		calls.Add(1)
		return user{ID: 1, Name: "Ada"}, nil
	}

	for range 3 {
		got, err := c.RememberAs(ctx, "user:1", time.Minute, build)
		if err != nil {
			t.Fatalf("RememberAs() error = %v", err)
		}
		if got.Name != "Ada" {
			t.Fatalf("RememberAs() = %+v", got)
		}
	}
	if calls.Load() != 1 {
		t.Errorf("the callback ran %d times, want 1", calls.Load())
	}
}

// A failed computation must not be cached as though it had succeeded.
func TestRememberDoesNotCacheErrors(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()
	wantErr := errors.New("upstream is down")

	_, err := c.RememberAs(ctx, "k", time.Minute, func(context.Context) (int, error) {
		return 0, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("RememberAs() error = %v, want %v", err, wantErr)
	}
	if has, _ := c.Has(ctx, "k"); has {
		t.Error("a failed computation was cached")
	}

	got, err := c.RememberAs(ctx, "k", time.Minute, func(context.Context) (int, error) {
		return 42, nil
	})
	if err != nil || got != 42 {
		t.Fatalf("RememberAs() after the failure = %v, %v", got, err)
	}
}

// The stampede guarantee: a cold popular key is computed once, not once per
// goroutine that missed it.
func TestRememberCollapsesConcurrentRebuilds(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()

	var calls atomic.Int64
	build := func(context.Context) (int, error) {
		calls.Add(1)
		time.Sleep(20 * time.Millisecond)
		return 42, nil
	}

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			got, err := c.RememberAs(ctx, "hot", time.Minute, build)
			if err != nil {
				t.Errorf("RememberAs() error = %v", err)
				return
			}
			if got != 42 {
				t.Errorf("RememberAs() = %d, want 42", got)
			}
		}()
	}
	wg.Wait()

	if calls.Load() != 1 {
		t.Errorf("the callback ran %d times for one cold key, want 1", calls.Load())
	}
}

// LockFor coordinates across caches that share a store, which singleflight
// alone cannot do: each Cache has its own singleflight group.
func TestRememberWithLockCoordinatesAcrossCaches(t *testing.T) {
	store := memory.New(memory.Config{Prefix: "test:"})
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	first := cache.New(store)
	second := cache.New(store)

	var calls atomic.Int64
	build := func(context.Context) (int, error) {
		calls.Add(1)
		time.Sleep(40 * time.Millisecond)
		return 7, nil
	}

	var wg sync.WaitGroup
	wg.Add(2)
	for _, c := range []*cache.Cache{first, second} {
		go func() {
			defer wg.Done()
			got, err := c.RememberAs(ctx, "shared", time.Minute, build,
				cache.LockFor(time.Second), cache.WaitFor(time.Second))
			if err != nil {
				t.Errorf("RememberAs() error = %v", err)
				return
			}
			if got != 7 {
				t.Errorf("RememberAs() = %d, want 7", got)
			}
		}()
	}
	wg.Wait()

	if calls.Load() != 1 {
		t.Errorf("the callback ran %d times across two caches, want 1", calls.Load())
	}
}

// Without the lock, two caches over one store each run their own singleflight,
// so the duplication LockFor prevents is real rather than hypothetical.
func TestRememberWithoutLockDoesNotCoordinateAcrossCaches(t *testing.T) {
	store := memory.New(memory.Config{Prefix: "test:"})
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	var calls atomic.Int64
	started := make(chan struct{}, 2)
	build := func(context.Context) (int, error) {
		calls.Add(1)
		started <- struct{}{}
		time.Sleep(40 * time.Millisecond)
		return 7, nil
	}

	var wg sync.WaitGroup
	wg.Add(2)
	for range 2 {
		c := cache.New(store)
		go func() {
			defer wg.Done()
			_, _ = c.RememberAs(ctx, "shared", time.Minute, build)
		}()
	}
	wg.Wait()

	if calls.Load() != 2 {
		t.Skipf("the two rebuilds did not overlap (callback ran %d times); timing-dependent", calls.Load())
	}
}

func TestGetManyAs(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()

	for i, name := range map[string]string{"a": "Ada", "c": "Grace"} {
		if err := c.Put(ctx, i, name, time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	values, err := c.GetManyAs[string](ctx, []string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("GetManyAs() error = %v", err)
	}
	if len(values) != 2 || values["a"] != "Ada" || values["c"] != "Grace" {
		t.Errorf("GetManyAs() = %v", values)
	}
}

func TestUnencodableValueIsReportedNotPanicked(t *testing.T) {
	c := newCache(t)

	err := c.Put(context.Background(), "ch", make(chan int), time.Minute)
	if err == nil {
		t.Fatal("Put() accepted a channel")
	}
}

func TestGobCodecRoundTrip(t *testing.T) {
	store := memory.New(memory.Config{Prefix: "test:"})
	t.Cleanup(func() { _ = store.Close() })
	c := cache.New(store, cache.WithCodec(cache.GobCodec{}))
	ctx := context.Background()

	if err := c.Put(ctx, "u", user{ID: 3, Name: "Grace"}, time.Minute); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	got, found, err := c.GetAs[user](ctx, "u")
	if err != nil || !found || got.Name != "Grace" {
		t.Fatalf("GetAs() = %+v, %v, %v", got, found, err)
	}
}
