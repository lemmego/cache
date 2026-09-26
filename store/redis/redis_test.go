package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/go-redis/redis/v8"
	"github.com/lemmego/cache"
	rediscache "github.com/lemmego/cache/store/redis"
	"github.com/lemmego/cache/storetest"
)

// miniredisClock drives miniredis's clock, which does not advance on its own.
// Without it the TTL cases would have to sleep, and would be slow and flaky.
type miniredisClock struct{ server *miniredis.Miniredis }

func (c miniredisClock) Advance(d time.Duration) { c.server.FastForward(d) }

// foreignAware lets the conformance suite check that Flush spares keys the
// cache does not own — the guard against reaching for FLUSHDB.
type foreignAware struct {
	*rediscache.Store
	client goredis.UniversalClient
}

func (f foreignAware) PutForeign(ctx context.Context, key string, value []byte) error {
	return f.client.Set(ctx, key, value, 0).Err()
}

func (f foreignAware) GetForeign(ctx context.Context, key string) ([]byte, error) {
	return f.client.Get(ctx, key).Bytes()
}

func newStore(t *testing.T) (cache.Store, storetest.Clock) {
	t.Helper()

	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	store, err := rediscache.New(rediscache.Config{Client: client, Prefix: "test:"})
	if err != nil {
		t.Fatalf("redis.New() error = %v", err)
	}
	return foreignAware{Store: store, client: client}, miniredisClock{server: server}
}

func TestConformance(t *testing.T) {
	storetest.Run(t, storetest.Capabilities{
		Locks:           true,
		TagIndex:        true,
		AtomicIncrement: true,
		ForeignKeySafe:  true,
		HonoursContext:  true,
		Persists:        true,

		SharedBacking: func(t *testing.T) (cache.Store, cache.Store) {
			server := miniredis.RunT(t)
			client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
			t.Cleanup(func() { _ = client.Close() })

			first, err := rediscache.New(rediscache.Config{Client: client, Prefix: "alpha:"})
			if err != nil {
				t.Fatal(err)
			}
			second, err := rediscache.New(rediscache.Config{Client: client, Prefix: "beta:"})
			if err != nil {
				t.Fatal(err)
			}
			return first, second
		},
	}, newStore)
}

func TestNewRequiresAPrefix(t *testing.T) {
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	if _, err := rediscache.New(rediscache.Config{Client: client}); err == nil {
		t.Fatal("redis.New() accepted an empty prefix, which would make Flush unscoped")
	}
}

func TestNewRequiresAnAddressOrClient(t *testing.T) {
	if _, err := rediscache.New(rediscache.Config{Prefix: "test:"}); err == nil {
		t.Fatal("redis.New() accepted neither an Addr nor a Client")
	}
}

// The regression that matters most on a shared database: flushing the cache
// must not take the sessions with it.
func TestFlushScansRatherThanEmptyingTheDatabase(t *testing.T) {
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()

	store, err := rediscache.New(rediscache.Config{Client: client, Prefix: "cache:"})
	if err != nil {
		t.Fatal(err)
	}

	if err := client.Set(ctx, "session:abc", "a logged-in user", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, "tasker:job:1", "queued work", 0).Err(); err != nil {
		t.Fatal(err)
	}
	for _, key := range storetest.KeysFor("k", 50) {
		if err := store.Put(ctx, key, []byte("v"), time.Minute); err != nil {
			t.Fatal(err)
		}
	}

	if err := store.Flush(ctx); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	for _, key := range []string{"session:abc", "tasker:job:1"} {
		if err := client.Get(ctx, key).Err(); err != nil {
			t.Errorf("Flush() destroyed %q, which the cache does not own: %v", key, err)
		}
	}
	found, err := store.GetMany(ctx, storetest.KeysFor("k", 50))
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 0 {
		t.Errorf("Flush() left %d cache entries", len(found))
	}
}

// Flushing has to page through the cursor; a single SCAN call returns only the
// first batch.
func TestFlushPagesPastOneBatch(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()

	keys := storetest.KeysFor("bulk", 2000)
	for _, key := range keys {
		if err := store.Put(ctx, key, []byte("v"), time.Hour); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	found, err := store.GetMany(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 0 {
		t.Errorf("%d entries survived a flush, so the scan stopped early", len(found))
	}
}

// INCRBY keeps an existing key's expiry, which is what makes a windowed
// counter — a rate limiter — actually reset.
func TestIncrementKeepsTheWindow(t *testing.T) {
	store, clock := newStore(t)
	ctx := context.Background()

	if err := store.Put(ctx, "requests", cache.FormatCounter(1), time.Minute); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		if _, err := store.Increment(ctx, "requests", 1); err != nil {
			t.Fatal(err)
		}
	}

	clock.Advance(61 * time.Second)
	if _, err := store.Get(ctx, "requests"); err == nil {
		t.Fatal("the counter outlived its window")
	}
}

// A lock must only be released by its holder. Without the compare-and-delete,
// a holder whose lease expired mid-work would free a lock someone else had
// already taken, and two workers would run at once.
func TestReleaseIsScopedToTheOwner(t *testing.T) {
	store, _ := newStore(t)
	provider := store.(interface {
		NewLock(name, owner string, ttl time.Duration) cache.Lock
	})
	ctx := context.Background()

	holder := provider.NewLock("import", "worker-a", time.Minute)
	if acquired, err := holder.Acquire(ctx); err != nil || !acquired {
		t.Fatalf("Acquire() = %v, %v", acquired, err)
	}

	impostor := provider.NewLock("import", "worker-b", time.Minute)
	released, err := impostor.Release(ctx)
	if err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if released {
		t.Fatal("a non-holder released the lock")
	}
	if acquired, _ := impostor.Acquire(ctx); acquired {
		t.Fatal("the refused release still deleted the lock")
	}

	if released, err = holder.Release(ctx); err != nil || !released {
		t.Fatalf("the holder could not release: %v, %v", released, err)
	}
}

func TestExtendRefreshesOnlyForTheOwner(t *testing.T) {
	store, clock := newStore(t)
	provider := store.(interface {
		NewLock(name, owner string, ttl time.Duration) cache.Lock
	})
	ctx := context.Background()

	holder := provider.NewLock("slow", "worker-a", time.Minute).(interface {
		cache.Lock
		Extend(context.Context, time.Duration) (bool, error)
	})
	if acquired, _ := holder.Acquire(ctx); !acquired {
		t.Fatal("setup: could not acquire")
	}

	clock.Advance(50 * time.Second)
	extended, err := holder.Extend(ctx, 2*time.Minute)
	if err != nil || !extended {
		t.Fatalf("Extend() = %v, %v; want true", extended, err)
	}

	clock.Advance(30 * time.Second) // past the original lease, inside the new one
	other := provider.NewLock("slow", "worker-b", time.Minute)
	if acquired, _ := other.Acquire(ctx); acquired {
		t.Error("the lock expired despite being extended")
	}
}
