package cache_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/lemmego/cache"
	_ "github.com/lemmego/cache/drivers"
)

// An unknown driver must name what is actually available, since the set
// depends on which driver packages were imported.
func TestUnknownDriverNamesTheRegisteredOnes(t *testing.T) {
	_, err := cache.Build(context.Background(), &cache.Config{Driver: "memcached", Prefix: "t:"})
	if err == nil {
		t.Fatal("cache.Build() accepted an unregistered driver")
	}
	for _, want := range []string{"memcached", "memory", "file", "redis"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

// An unreachable backend fails the boot by default, so a wrong address stops a
// deployment rather than quietly degrading it.
func TestAnUnreachableBackendFailsByDefault(t *testing.T) {
	_, err := cache.Build(context.Background(), &cache.Config{
		Driver: "redis",
		Prefix: "t:",
		Redis:  cache.RedisConfig{Addr: "127.0.0.1:1"},
	})
	if err == nil {
		t.Fatal("cache.Build() returned a store for an unreachable redis")
	}
}

// Lenient trades that for staying up: every read misses, which is slow but
// serving.
func TestLenientFallsBackToAStoreThatKeepsNothing(t *testing.T) {
	store, err := cache.Build(context.Background(), &cache.Config{
		Driver:  "redis",
		Prefix:  "t:",
		Lenient: true,
		Redis:   cache.RedisConfig{Addr: "127.0.0.1:1"},
	})
	if err != nil {
		t.Fatalf("cache.Build() with Lenient error = %v", err)
	}

	ctx := context.Background()
	if err := store.Put(ctx, "k", []byte("v"), time.Minute); err != nil {
		t.Fatalf("Put() on the fallback error = %v", err)
	}
	if _, err := store.Get(ctx, "k"); err != cache.ErrMiss {
		t.Errorf("Get() on the fallback = %v, want a miss", err)
	}
}
