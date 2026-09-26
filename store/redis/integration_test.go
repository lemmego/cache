package redis_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	goredis "github.com/go-redis/redis/v8"
	"github.com/lemmego/cache"
	rediscache "github.com/lemmego/cache/store/redis"
)

// Integration tests run against a real Redis server. They are skipped unless
// CACHE_REDIS_ADDR is set, per the repository's policy that a test needing an
// external service uses explicit configuration and an isolated namespace.
//
//	CACHE_REDIS_ADDR=127.0.0.1:6379 go test ./store/redis/
//
// They cover what miniredis cannot: real expiry against the wall clock, the
// server's own Lua engine and EVALSHA caching, SCAN paging under a live
// keyspace, and two clients genuinely contending for a lock. Locks in
// particular should never be trusted on the strength of a simulator.
func integrationStore(t *testing.T) (*rediscache.Store, goredis.UniversalClient) {
	t.Helper()

	addr := os.Getenv("CACHE_REDIS_ADDR")
	if addr == "" {
		t.Skip("set CACHE_REDIS_ADDR to run the redis integration tests")
	}

	client := goredis.NewClient(&goredis.Options{
		Addr:     addr,
		Password: os.Getenv("CACHE_REDIS_PASSWORD"),
	})
	t.Cleanup(func() { _ = client.Close() })

	// A prefix unique to this run, so the tests cannot disturb anything else
	// living in the database.
	prefix := fmt.Sprintf("cachetest:%d:", time.Now().UnixNano())
	store, err := rediscache.New(rediscache.Config{Client: client, Prefix: prefix})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Ping(context.Background()); err != nil {
		t.Skipf("redis at %s is not reachable: %v", addr, err)
	}
	t.Cleanup(func() { _ = store.Flush(context.Background()) })

	return store, client
}

func TestIntegrationRoundTrip(t *testing.T) {
	store, _ := integrationStore(t)
	ctx := context.Background()

	if err := store.Put(ctx, "k", []byte("value"), time.Minute); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(ctx, "k")
	if err != nil || string(got) != "value" {
		t.Fatalf("Get() = %q, %v", got, err)
	}
}

// Real expiry, against the server's own clock rather than a simulated one.
func TestIntegrationExpiryAgainstTheWallClock(t *testing.T) {
	store, _ := integrationStore(t)
	ctx := context.Background()

	if err := store.Put(ctx, "brief", []byte("v"), 900*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, "brief"); err != nil {
		t.Fatalf("Get() before expiry = %v", err)
	}

	time.Sleep(1200 * time.Millisecond)

	if _, err := store.Get(ctx, "brief"); err != cache.ErrMiss {
		t.Fatalf("Get() after expiry = %v, want ErrMiss", err)
	}
}

// The compare-and-delete release, run by the server's real Lua engine and
// through EVALSHA on the second call.
func TestIntegrationLockReleaseIsScopedToTheOwner(t *testing.T) {
	store, _ := integrationStore(t)
	ctx := context.Background()

	holder := store.NewLock("import", "worker-a", 30*time.Second)
	impostor := store.NewLock("import", "worker-b", 30*time.Second)

	if acquired, err := holder.Acquire(ctx); err != nil || !acquired {
		t.Fatalf("Acquire() = %v, %v", acquired, err)
	}
	t.Cleanup(func() { _ = holder.ForceRelease(ctx) })

	if acquired, err := impostor.Acquire(ctx); err != nil || acquired {
		t.Fatalf("a second holder acquired the lock: %v, %v", acquired, err)
	}

	// Twice, so the second call goes through EVALSHA against the server's
	// script cache rather than EVAL.
	for range 2 {
		released, err := impostor.Release(ctx)
		if err != nil {
			t.Fatalf("Release() error = %v", err)
		}
		if released {
			t.Fatal("a non-holder released the lock")
		}
	}

	released, err := holder.Release(ctx)
	if err != nil || !released {
		t.Fatalf("the holder could not release: %v, %v", released, err)
	}
}

// Two clients contending for real, on separate connections.
func TestIntegrationOnlyOneClientWinsTheLock(t *testing.T) {
	store, _ := integrationStore(t)
	ctx := context.Background()

	const contenders = 8
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		winners int
		inside  int
		maxSeen int
	)

	wg.Add(contenders)
	for i := range contenders {
		go func(i int) {
			defer wg.Done()
			lock := store.NewLock("exclusive", fmt.Sprintf("worker-%d", i), 10*time.Second)

			acquired, err := lock.Get(ctx, func(context.Context) error {
				mu.Lock()
				inside++
				if inside > maxSeen {
					maxSeen = inside
				}
				mu.Unlock()

				time.Sleep(20 * time.Millisecond)

				mu.Lock()
				inside--
				mu.Unlock()
				return nil
			})
			if err != nil {
				t.Errorf("Get() error = %v", err)
				return
			}
			if acquired {
				mu.Lock()
				winners++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	if maxSeen > 1 {
		t.Errorf("%d workers were inside the lock at once", maxSeen)
	}
	if winners == 0 {
		t.Error("no worker ever acquired the lock")
	}
}

// Blocking against a lock a second client is really holding.
func TestIntegrationBlockWaitsForTheHolder(t *testing.T) {
	store, _ := integrationStore(t)
	ctx := context.Background()

	holder := store.NewLock("handover", "worker-a", 10*time.Second)
	if acquired, err := holder.Acquire(ctx); err != nil || !acquired {
		t.Fatalf("Acquire() = %v, %v", acquired, err)
	}

	go func() {
		time.Sleep(150 * time.Millisecond)
		_, _ = holder.Release(ctx)
	}()

	waiter := store.NewLock("handover", "worker-b", 10*time.Second)
	start := time.Now()
	acquired, err := waiter.Block(ctx, 3*time.Second)
	if err != nil {
		t.Fatalf("Block() error = %v", err)
	}
	if !acquired {
		t.Fatal("Block() gave up while the lock was released in time")
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Errorf("Block() returned after %v, before the holder released", elapsed)
	}
	_, _ = waiter.Release(ctx)
}

// SCAN paging across more keys than one batch, in a live keyspace that also
// holds data the cache does not own.
func TestIntegrationFlushPagesAndSparesForeignKeys(t *testing.T) {
	store, client := integrationStore(t)
	ctx := context.Background()

	foreign := fmt.Sprintf("not-the-cache:%d", time.Now().UnixNano())
	if err := client.Set(ctx, foreign, "keep me", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Del(ctx, foreign).Err() })

	const count = 2500
	values := make(map[string][]byte, count)
	for i := range count {
		values[fmt.Sprintf("bulk%d", i)] = []byte("v")
	}
	if err := store.PutMany(ctx, values, time.Hour); err != nil {
		t.Fatal(err)
	}

	if err := store.Flush(ctx); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	keys := make([]string, 0, count)
	for key := range values {
		keys = append(keys, key)
	}
	found, err := store.GetMany(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 0 {
		t.Errorf("%d of %d entries survived the flush", len(found), count)
	}

	if err := client.Get(ctx, foreign).Err(); err != nil {
		t.Errorf("Flush() destroyed a key outside the cache prefix: %v", err)
	}
}

// Pipelined MGET against a real server.
func TestIntegrationGetManyIsPipelined(t *testing.T) {
	store, _ := integrationStore(t)
	ctx := context.Background()

	values := map[string][]byte{"a": []byte("1"), "b": []byte("2"), "c": []byte("3")}
	if err := store.PutMany(ctx, values, time.Minute); err != nil {
		t.Fatal(err)
	}

	found, err := store.GetMany(ctx, []string{"a", "b", "missing", "c"})
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 3 {
		t.Fatalf("GetMany() = %v, want three entries", found)
	}
	if _, ok := found["missing"]; ok {
		t.Error("GetMany() invented an absent key")
	}
}
