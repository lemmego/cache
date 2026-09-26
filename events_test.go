package cache_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lemmego/cache"
	"github.com/lemmego/cache/store/memory"
)

// recorder collects events in order.
type recorder struct {
	mu     sync.Mutex
	events []cache.Event
}

func (r *recorder) listen(e cache.Event) {
	r.mu.Lock()
	r.events = append(r.events, e)
	r.mu.Unlock()
}

func (r *recorder) names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	names := make([]string, len(r.events))
	for i, e := range r.events {
		names[i] = e.EventName()
	}
	return names
}

func TestEventsFireWithTheRightDetail(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()

	var rec recorder
	for _, name := range []string{
		cache.EventHit, cache.EventMissed,
		cache.EventKeyWritten, cache.EventKeyForgotten, cache.EventFlushed,
	} {
		c.Listen(name, rec.listen)
	}

	if _, _, err := c.GetAs[int](ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "k", 1, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.GetAs[int](ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Forget(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if err := c.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	want := []string{
		cache.EventMissed, cache.EventKeyWritten,
		cache.EventHit, cache.EventKeyForgotten, cache.EventFlushed,
	}
	got := rec.names()
	if len(got) != len(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("event %d = %s, want %s", i, got[i], want[i])
		}
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if hit, ok := rec.events[2].(cache.CacheHit); !ok || hit.Key != "k" {
		t.Errorf("hit event = %+v", rec.events[2])
	}
	if written, ok := rec.events[1].(cache.KeyWritten); !ok || written.Key != "k" || written.TTL != time.Minute {
		t.Errorf("written event = %+v", rec.events[1])
	}
}

func TestEventsCarryTags(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()

	var rec recorder
	c.Listen(cache.EventKeyWritten, rec.listen)

	if err := c.Tags("posts", "featured").Put(ctx, "k", 1, time.Minute); err != nil {
		t.Fatal(err)
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.events) != 1 {
		t.Fatalf("events = %v, want one", rec.events)
	}
	written := rec.events[0].(cache.KeyWritten)
	if len(written.Tags) != 2 || written.Tags[0] != "featured" || written.Tags[1] != "posts" {
		t.Errorf("tags = %v, want the sorted tag set", written.Tags)
	}
}

// Forgetting a key that was not there is not an event: nothing happened.
func TestForgettingAnAbsentKeyRaisesNoEvent(t *testing.T) {
	c := newCache(t)

	var rec recorder
	c.Listen(cache.EventKeyForgotten, rec.listen)

	if _, err := c.Forget(context.Background(), "never-there"); err != nil {
		t.Fatal(err)
	}
	if names := rec.names(); len(names) != 0 {
		t.Errorf("events = %v, want none", names)
	}
}

// A listener that panics must not take down the request that read the cache.
func TestAPanickingListenerDoesNotBreakTheCache(t *testing.T) {
	c := newCache(t)
	ctx := context.Background()

	c.Listen(cache.EventHit, func(cache.Event) { panic("listener is broken") })

	if err := c.Put(ctx, "k", 42, time.Minute); err != nil {
		t.Fatal(err)
	}
	got, found, err := c.GetAs[int](ctx, "k")
	if err != nil || !found || got != 42 {
		t.Fatalf("GetAs() = %v, %v, %v; a listener panic reached the caller", got, found, err)
	}
}

// The reason events are guarded by an atomic bitmask rather than a map lookup:
// Get sits on the hot path of whatever it caches, so events must cost nothing
// when nobody is listening.
//
// The measurement is comparative rather than absolute: decoding the value
// allocates either way, so what matters is that observing costs more than not
// observing, and that the difference is exactly the event.
func TestUnobservedEventsCostNothing(t *testing.T) {
	store := memory.New(memory.Config{Prefix: "test:"})
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	quiet := cache.New(store)
	if err := quiet.Put(ctx, "k", 42, time.Minute); err != nil {
		t.Fatal(err)
	}
	observed := cache.New(store)
	observed.Listen(cache.EventHit, func(cache.Event) {})

	unobserved := testing.AllocsPerRun(500, func() { _, _, _ = quiet.GetAs[int](ctx, "k") })
	withListener := testing.AllocsPerRun(500, func() { _, _, _ = observed.GetAs[int](ctx, "k") })

	if unobserved >= withListener {
		t.Errorf("a read with no listeners allocated %v, one with a listener %v; "+
			"the event is being built whether or not anyone wants it", unobserved, withListener)
	}
	t.Logf("allocations per read: %v unobserved, %v observed", unobserved, withListener)
}

// A read that cannot hit an event must allocate the same either way, which
// shows the bitmask guard and not some accident of the read path is doing the
// work.
func TestTheGuardIsWhatSkipsTheEvent(t *testing.T) {
	store := memory.New(memory.Config{Prefix: "test:"})
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	quiet := cache.New(store)
	if err := quiet.Put(ctx, "k", 42, time.Minute); err != nil {
		t.Fatal(err)
	}

	// Listening for an unrelated event must not make reads pay: the mask bit
	// for a hit is still clear.
	unrelated := cache.New(store)
	unrelated.Listen(cache.EventFlushed, func(cache.Event) {})

	base := testing.AllocsPerRun(500, func() { _, _, _ = quiet.GetAs[int](ctx, "k") })
	other := testing.AllocsPerRun(500, func() { _, _, _ = unrelated.GetAs[int](ctx, "k") })

	if base != other {
		t.Errorf("listening for an unrelated event changed the cost of a read: %v vs %v", base, other)
	}
}

func BenchmarkGetWithoutListeners(b *testing.B) {
	store := memory.New(memory.Config{Prefix: "bench:"})
	b.Cleanup(func() { _ = store.Close() })
	c := cache.New(store)
	ctx := context.Background()
	_ = c.Put(ctx, "k", 42, time.Minute)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _, _ = c.GetAs[int](ctx, "k")
	}
}

func BenchmarkGetWithListener(b *testing.B) {
	store := memory.New(memory.Config{Prefix: "bench:"})
	b.Cleanup(func() { _ = store.Close() })
	c := cache.New(store)
	c.Listen(cache.EventHit, func(cache.Event) {})
	ctx := context.Background()
	_ = c.Put(ctx, "k", 42, time.Minute)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _, _ = c.GetAs[int](ctx, "k")
	}
}
