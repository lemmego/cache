package memory_test

import (
	"context"
	"testing"
	"time"

	"github.com/lemmego/cache"
	"github.com/lemmego/cache/store/memory"
	"github.com/lemmego/cache/storetest"
)

// testClock advances a store's view of time without sleeping.
type testClock struct{ offset time.Duration }

func (c *testClock) Advance(d time.Duration) { c.offset += d }
func (c *testClock) now() time.Time          { return time.Now().Add(c.offset) }

func newStore(t *testing.T) (cache.Store, storetest.Clock) {
	clock := &testClock{}
	store := memory.New(memory.Config{Prefix: "test:", Now: clock.now})
	t.Cleanup(func() { _ = store.Close() })
	return store, clock
}

func TestConformance(t *testing.T) {
	storetest.Run(t, storetest.Capabilities{
		Locks:           true,
		TagIndex:        true,
		AtomicIncrement: true,
		Persists:        true,
		// Operations are local map accesses, so there is nothing to cancel.
		HonoursContext: false,
		// A memory store owns its whole map; there is no foreign data to spare.
		ForeignKeySafe: false,
	}, newStore)
}

// A caller mutating a returned slice must not be able to corrupt the cache.
func TestReturnedValuesAreCopies(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()

	original := []byte("hello")
	if err := store.Put(ctx, "k", original, time.Minute); err != nil {
		t.Fatal(err)
	}
	original[0] = 'H' // mutate what we handed in

	got, err := store.Get(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello" {
		t.Errorf("mutating the caller's slice changed the cache: %q", got)
	}

	got[0] = 'J' // mutate what we got back
	again, err := store.Get(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != "hello" {
		t.Errorf("mutating a returned slice changed the cache: %q", again)
	}
}

// Lazy expiry alone leaks: an entry written once and never read again would
// hold its memory until the process exits.
func TestSweeperReclaimsExpiredEntries(t *testing.T) {
	clock := &testClock{}
	store := memory.New(memory.Config{Prefix: "test:", Now: clock.now})
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	for _, key := range storetest.KeysFor("k", 50) {
		if err := store.Put(ctx, key, []byte("v"), time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	clock.Advance(61 * time.Second)

	if err := store.Prune(ctx); err != nil {
		t.Fatalf("Prune() error = %v", err)
	}
	found, err := store.GetMany(ctx, storetest.KeysFor("k", 50))
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 0 {
		t.Errorf("%d entries survived the sweep", len(found))
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	store := memory.New(memory.Config{Prefix: "test:", GCInterval: time.Millisecond})
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}
