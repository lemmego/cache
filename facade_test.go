package cache_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lemmego/cache"
	"github.com/lemmego/cache/store/memory"
)

// An application that has not configured a cache must get an error from the
// package functions, never a panic and never a silent success that looks like
// a permanent miss.
func TestFacadeReportsAnUninitializedCache(t *testing.T) {
	cache.SetGlobal(nil)
	t.Cleanup(func() { cache.SetGlobal(nil) })
	ctx := context.Background()

	t.Run("Get", func(t *testing.T) {
		if _, _, err := cache.Get[int](ctx, "k"); !errors.Is(err, cache.ErrNotInitialized) {
			t.Errorf("error = %v, want ErrNotInitialized", err)
		}
	})
	t.Run("Put", func(t *testing.T) {
		if err := cache.Put(ctx, "k", 1, time.Minute); !errors.Is(err, cache.ErrNotInitialized) {
			t.Errorf("error = %v, want ErrNotInitialized", err)
		}
	})
	t.Run("Remember", func(t *testing.T) {
		_, err := cache.Remember(ctx, "k", time.Minute, func(context.Context) (int, error) {
			t.Error("the callback ran without a cache")
			return 0, nil
		})
		if !errors.Is(err, cache.ErrNotInitialized) {
			t.Errorf("error = %v, want ErrNotInitialized", err)
		}
	})
	t.Run("Forget", func(t *testing.T) {
		if _, err := cache.Forget(ctx, "k"); !errors.Is(err, cache.ErrNotInitialized) {
			t.Errorf("error = %v, want ErrNotInitialized", err)
		}
	})
	t.Run("Flush", func(t *testing.T) {
		if err := cache.Flush(ctx); !errors.Is(err, cache.ErrNotInitialized) {
			t.Errorf("error = %v, want ErrNotInitialized", err)
		}
	})
	t.Run("Increment", func(t *testing.T) {
		if _, err := cache.Increment(ctx, "k", 1); !errors.Is(err, cache.ErrNotInitialized) {
			t.Errorf("error = %v, want ErrNotInitialized", err)
		}
	})
	t.Run("Has", func(t *testing.T) {
		if _, err := cache.Has(ctx, "k"); !errors.Is(err, cache.ErrNotInitialized) {
			t.Errorf("error = %v, want ErrNotInitialized", err)
		}
	})

	// A lock with no cache must refuse rather than report success. A caller
	// that ignores the error must not proceed believing it holds one.
	t.Run("NewLock refuses", func(t *testing.T) {
		lock := cache.NewLock("job", time.Minute)
		acquired, err := lock.Acquire(ctx)
		if acquired {
			t.Error("a lock was acquired with no cache configured")
		}
		if !errors.Is(err, cache.ErrUnsupported) {
			t.Errorf("error = %v, want ErrUnsupported", err)
		}
	})
}

func TestFacadeUsesTheInstalledCache(t *testing.T) {
	store := memory.New(memory.Config{Prefix: "test:"})
	t.Cleanup(func() { _ = store.Close() })

	cache.SetGlobal(cache.New(store))
	t.Cleanup(func() { cache.SetGlobal(nil) })
	ctx := context.Background()

	if err := cache.Put(ctx, "greeting", "hello", time.Minute); err != nil {
		t.Fatal(err)
	}
	got, found, err := cache.Get[string](ctx, "greeting")
	if err != nil || !found || got != "hello" {
		t.Fatalf("Get() = %q, %v, %v", got, found, err)
	}

	built := 0
	value, err := cache.Remember(ctx, "computed", time.Minute, func(context.Context) (int, error) {
		built++
		return 99, nil
	})
	if err != nil || value != 99 {
		t.Fatalf("Remember() = %d, %v", value, err)
	}
	if _, err = cache.Remember(ctx, "computed", time.Minute, func(context.Context) (int, error) {
		built++
		return 0, nil
	}); err != nil {
		t.Fatal(err)
	}
	if built != 1 {
		t.Errorf("the callback ran %d times, want 1", built)
	}

	if forgotten, err := cache.Forget(ctx, "greeting"); err != nil || !forgotten {
		t.Errorf("Forget() = %v, %v", forgotten, err)
	}
}

func TestFacadeTagsAreIsolated(t *testing.T) {
	store := memory.New(memory.Config{Prefix: "test:"})
	t.Cleanup(func() { _ = store.Close() })
	cache.SetGlobal(cache.New(store))
	t.Cleanup(func() { cache.SetGlobal(nil) })
	ctx := context.Background()

	if err := cache.Tags("posts").Put(ctx, "1", "tagged", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := cache.Get[string](ctx, "1"); found {
		t.Error("a tagged entry was visible to an untagged read")
	}
	got, found, err := cache.Tags("posts").GetAs[string](ctx, "1")
	if err != nil || !found || got != "tagged" {
		t.Fatalf("tagged read = %q, %v, %v", got, found, err)
	}
}
