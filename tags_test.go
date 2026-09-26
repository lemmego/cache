package cache_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lemmego/cache"
	"github.com/lemmego/cache/store/file"
	"github.com/lemmego/cache/store/memory"
)

func TestTaggedEntriesAreIsolated(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()

	if err := c.Tags("posts").Put(ctx, "1", "post one", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := c.Tags("users").Put(ctx, "1", "user one", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "1", "untagged", time.Minute); err != nil {
		t.Fatal(err)
	}

	cases := map[string]struct {
		cache *cache.Cache
		want  string
	}{
		"posts":    {c.Tags("posts"), "post one"},
		"users":    {c.Tags("users"), "user one"},
		"untagged": {c, "untagged"},
	}
	for name, tc := range cases {
		got, found, err := tc.cache.GetAs[string](ctx, "1")
		if err != nil || !found {
			t.Fatalf("%s: GetAs() = %q, %v, %v", name, got, found, err)
		}
		if got != tc.want {
			t.Errorf("%s: GetAs() = %q, want %q", name, got, tc.want)
		}
	}
}

func TestTagFlushLeavesOtherTagsAlone(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()

	if err := c.Tags("posts").Put(ctx, "1", "post", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := c.Tags("users").Put(ctx, "1", "user", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "1", "untagged", time.Minute); err != nil {
		t.Fatal(err)
	}

	if err := c.Tags("posts").Flush(ctx); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	if _, found, _ := c.Tags("posts").GetAs[string](ctx, "1"); found {
		t.Error("the flushed tag still resolves")
	}
	if got, found, _ := c.Tags("users").GetAs[string](ctx, "1"); !found || got != "user" {
		t.Errorf("flushing one tag removed another: %q, %v", got, found)
	}
	if got, found, _ := c.GetAs[string](ctx, "1"); !found || got != "untagged" {
		t.Errorf("flushing a tag removed an untagged entry: %q, %v", got, found)
	}
}

// Tags name a set, so the order they are given in cannot matter.
func TestTagOrderIsIrrelevant(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()

	if err := c.Tags("a", "b").Put(ctx, "k", "value", time.Minute); err != nil {
		t.Fatal(err)
	}
	got, found, err := c.Tags("b", "a").GetAs[string](ctx, "k")
	if err != nil || !found || got != "value" {
		t.Fatalf("GetAs() with reordered tags = %q, %v, %v", got, found, err)
	}
}

// An entry under several tags is invalidated by flushing any one of them.
func TestFlushingAnyTagInvalidatesTheEntry(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()

	if err := c.Tags("a", "b").Put(ctx, "k", "value", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := c.Tags("a").Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := c.Tags("a", "b").GetAs[string](ctx, "k"); found {
		t.Error("flushing one of the entry's tags left it reachable")
	}
}

func TestTaggedRememberWorks(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()

	calls := 0
	build := func(context.Context) (int, error) { calls++; return 9, nil }

	for range 2 {
		got, err := c.Tags("posts").RememberAs(ctx, "count", time.Minute, build)
		if err != nil || got != 9 {
			t.Fatalf("RememberAs() = %d, %v", got, err)
		}
	}
	if calls != 1 {
		t.Errorf("the callback ran %d times, want 1", calls)
	}

	if err := c.Tags("posts").Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Tags("posts").RememberAs(ctx, "count", time.Minute, build); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Errorf("the callback ran %d times after a flush, want 2", calls)
	}
}

// A store with a tag index can delete members, so a non-expiring tagged entry
// is safe there and is actually removed by a flush.
func TestForeverUnderTagsOnAnIndexedStore(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()

	if err := c.Tags("posts").Forever(ctx, "k", "value"); err != nil {
		t.Fatalf("Forever() under tags error = %v", err)
	}
	if _, found, _ := c.Tags("posts").GetAs[string](ctx, "k"); !found {
		t.Fatal("the value was not stored")
	}
	if err := c.Tags("posts").Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := c.Tags("posts").GetAs[string](ctx, "k"); found {
		t.Error("Flush() left a non-expiring tagged entry reachable")
	}
}

// Without an index, a flushed entry is only unreachable, never removed — so a
// non-expiring one would occupy space forever with no way to reach it. The
// store refuses rather than leaking.
func TestForeverUnderTagsIsRefusedWithoutAnIndex(t *testing.T) {
	store, err := file.New(file.Config{Dir: t.TempDir(), Prefix: "test:"})
	if err != nil {
		t.Fatal(err)
	}
	c := cache.New(store)
	ctx := context.Background()

	err = c.Tags("posts").Forever(ctx, "k", "value")
	if !errors.Is(err, cache.ErrTagsRequireTTL) {
		t.Fatalf("Forever() under tags error = %v, want ErrTagsRequireTTL", err)
	}

	// The same entry with a TTL is fine.
	if err := c.Tags("posts").Put(ctx, "k", "value", time.Minute); err != nil {
		t.Errorf("Put() with a ttl under tags error = %v", err)
	}
}

// Flushing must not scan: it is one write per tag regardless of cache size.
func TestTagFlushCostDoesNotGrowWithTheCache(t *testing.T) {
	store := memory.New(memory.Config{Prefix: "test:"})
	t.Cleanup(func() { _ = store.Close() })
	c := cache.New(store)
	ctx := context.Background()

	// Many untagged entries, which a scan-based flush would have to walk.
	for i := range 5000 {
		if err := c.Put(ctx, string(rune('a'+i%26))+string(rune(i)), i, time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Tags("posts").Put(ctx, "k", "v", time.Minute); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	if err := c.Tags("posts").Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("flushing one tag took %v against a 5000-entry cache; it should not scan", elapsed)
	}
}
