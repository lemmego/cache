package cache

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Event names, usable as the key when cache events are bridged onto the
// application's event emitter.
const (
	EventHit          = "cache.hit"
	EventMissed       = "cache.missed"
	EventKeyWritten   = "cache.key_written"
	EventKeyForgotten = "cache.key_forgotten"
	EventFlushed      = "cache.flushed"
)

// Event is one thing that happened to the cache.
//
// Events are concrete types rather than a map or an any payload, so a listener
// reads e.Key instead of asserting its way to it.
type Event interface{ EventName() string }

// CacheHit reports a key that was found. Value is the stored bytes, before
// decoding.
type CacheHit struct {
	Key   string
	Tags  []string
	Value []byte
}

// CacheMissed reports a key that was not found.
type CacheMissed struct {
	Key  string
	Tags []string
}

// KeyWritten reports a value stored. A zero TTL means it was stored forever.
type KeyWritten struct {
	Key   string
	Tags  []string
	Value []byte
	TTL   time.Duration
}

// KeyForgotten reports a key removed.
type KeyForgotten struct {
	Key  string
	Tags []string
}

// CacheFlushed reports a whole cache, or one tag namespace, being emptied.
type CacheFlushed struct {
	Tags []string
}

func (CacheHit) EventName() string     { return EventHit }
func (CacheMissed) EventName() string  { return EventMissed }
func (KeyWritten) EventName() string   { return EventKeyWritten }
func (KeyForgotten) EventName() string { return EventKeyForgotten }
func (CacheFlushed) EventName() string { return EventFlushed }

// Listener receives events. It is called on the goroutine that caused the
// event, so a slow listener slows the cache operation that triggered it.
type Listener func(Event)

// Event bits, one per event name, held in a single atomic word so the hot path
// can ask "is anyone listening for this?" with one load.
const (
	bitHit uint64 = 1 << iota
	bitMissed
	bitKeyWritten
	bitKeyForgotten
	bitFlushed
)

func bitFor(name string) uint64 {
	switch name {
	case EventHit:
		return bitHit
	case EventMissed:
		return bitMissed
	case EventKeyWritten:
		return bitKeyWritten
	case EventKeyForgotten:
		return bitKeyForgotten
	case EventFlushed:
		return bitFlushed
	default:
		return 0
	}
}

// dispatcher holds listeners and answers, cheaply, whether there are any.
//
// The cost of events when nobody is listening has to be nothing, because Get is
// on the hot path of whatever it is caching. A map lookup or a mutex per read
// would not qualify, so subscriptions are summarised into one atomic word.
type dispatcher struct {
	mask atomic.Uint64

	mu        sync.RWMutex
	listeners map[string][]Listener
	sink      func(Event)
}

func newDispatcher() *dispatcher {
	return &dispatcher{listeners: map[string][]Listener{}}
}

// enabled reports whether anything is listening for an event. One atomic load
// and an AND, small enough to inline.
func (d *dispatcher) enabled(bit uint64) bool {
	return d.mask.Load()&bit != 0
}

func (d *dispatcher) listen(name string, l Listener) {
	bit := bitFor(name)
	if bit == 0 || l == nil {
		return
	}
	d.mu.Lock()
	d.listeners[name] = append(d.listeners[name], l)
	d.mu.Unlock()
	d.mask.Or(bit)
}

// setSink registers a listener for every event, used by the bridge onto the
// application emitter.
func (d *dispatcher) setSink(sink func(Event)) {
	d.mu.Lock()
	d.sink = sink
	d.mu.Unlock()
	if sink == nil {
		return
	}
	d.mask.Or(bitHit | bitMissed | bitKeyWritten | bitKeyForgotten | bitFlushed)
}

// dispatch delivers an event. Callers must check enabled first — the event
// value is built at the call site inside that check, so that constructing it
// costs nothing when nobody is listening.
func (d *dispatcher) dispatch(e Event) {
	d.mu.RLock()
	listeners := d.listeners[e.EventName()]
	sink := d.sink
	d.mu.RUnlock()

	for _, l := range listeners {
		d.deliver(l, e)
	}
	if sink != nil {
		d.deliver(sink, e)
	}
}

// deliver isolates a listener's panic. A listener that panics must not take
// down the request that happened to read the cache.
func (d *dispatcher) deliver(l Listener, e Event) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("cache: event listener panicked", "event", e.EventName(), "panic", r)
		}
	}()
	l(e)
}
