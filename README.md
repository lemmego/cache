# lemmego/cache

A cache for Lemmego applications: several stores behind one API, with typed
access, atomic locks, tagged invalidation and events.

```go
import (
    "github.com/lemmego/cache"
    _ "github.com/lemmego/cache/drivers"
)

users, err := cache.Remember(ctx, "users:active", 5*time.Minute,
    func(ctx context.Context) ([]User, error) {
        return repo.ActiveUsers(ctx)
    })
```

## Two layers

`Store` is the driver contract and deals only in `[]byte`, so a driver never
has to know how a value is encoded. `Cache` sits above it and owns encoding,
events and the generic API.

`Cache` is a concrete type, and there is deliberately **no interface over it**.
Several of its methods declare their own type parameters, which Go permits only
on concrete types — a method with type parameters can never satisfy an
interface. Adding a `Cacher` interface later would silently delete the typed
API.

## Stores

| | memory | file | redis | null |
|---|---|---|---|---|
| Survives a restart | no | yes | yes | — |
| Shared between processes | no | same host | yes | — |
| Locks | this process | this host | **cluster-wide** | none (see below) |
| Tag flush removes entries | yes | on expiry | yes | — |
| Stores `Forever` under tags | yes | refused | yes | — |

The default is **file**, not memory: the CLI and the server are separate
processes, so a memory cache would leave `cache:clear` unable to reach the
cache the server is using.

`null` keeps nothing. Reads always miss, so it exercises the cache-miss path on
every request — useful in tests, and what `lenient` falls back to when the real
backend is unreachable. Its locks always "succeed" and therefore exclude
nothing; that is Laravel's behaviour too, and it is worth knowing before you
rely on a lock in a test.

Only the drivers you import are compiled in. `_ "github.com/lemmego/cache/drivers"`
registers all four; import individual packages under `store/` to keep a Redis
client out of a binary that will never use one.

## Typed access

```go
user, found, err := c.GetAs[User](ctx, "user:7")
```

The bool is not redundant with the error. A miss is not a failure, so `err`
stays reserved for a store that broke or a value that would not decode — and
the bool is what separates a miss from a cached `0`, `false` or `""`.

## Remember

`Remember` computes a value on a miss and caches it. Concurrent callers in one
process collapse to a single computation. If the callback fails, nothing is
cached: a failed computation must not be remembered as though it had worked.

Across processes, add `cache.LockFor(d)` — one process rebuilds while the
others wait and then read what it wrote. It is opt-in because it costs a lock
round trip, and because it is only as strong as the store's lock.

## Locks

```go
lock := c.Lock("import", time.Minute)
ran, err := lock.Get(ctx, func(ctx context.Context) error {
    return importEverything(ctx)
})
```

Locks carry an owner token, so only the holder can release one. Without that, a
holder whose lease expired mid-work would release a lock someone else had
already taken, and two workers would run at once. On Redis the release is a
single Lua compare-and-delete for the same reason — reading the owner and then
deleting are two operations with a window between them.

`Block` returns `(false, nil)` at its deadline. A lock that was busy is an
expected outcome, not an error.

A store that cannot lock returns a lock that refuses, rather than one that
pretends to have been acquired.

## Tags

```go
c.Tags("posts").Put(ctx, "1", post, time.Hour)
c.Tags("posts").Flush(ctx)
```

`Tags` returns a `*Cache`, so everything — including `Remember` — works on a
tagged cache with no second API.

Flushing mints a new version for each tag, which is one write per tag and no
scan. Entries under the old version become unreachable immediately and then
expire on their own. On a store that can enumerate tag members (memory, redis)
they are deleted as well, which is what makes a non-expiring tagged entry safe;
on one that cannot (file), writing `Forever` under tags is refused rather than
leaking an entry nothing can reach.

## Events

```go
c.Listen(cache.EventMissed, func(e cache.Event) {
    slog.Info("cache miss", "key", e.(cache.CacheMissed).Key)
})
```

Events cost nothing when nobody is listening: subscriptions are summarised into
one atomic word, and the event value is built inside that check. Listeners run
synchronously on the goroutine that touched the cache, and a listener that
panics is contained rather than allowed to fail the request.

Set `events: true` in configuration to also publish them on the application's
event emitter.

## Configuration

`lemmego publish --tags=config` writes `internal/configs/cache.go`. The
settings that matter most:

| Key | Default | |
|---|---|---|
| `driver` | `file` | `memory`, `file`, `redis`, `null` |
| `prefix` | `lemmego_cache:` | Namespaces keys, and bounds what a flush removes |
| `ttl` | `3600` | Seconds |
| `codec` | `json` | `json` or `gob` |
| `events` | `false` | Publish events on the app emitter |
| `lenient` | `false` | Serve without a cache rather than failing to boot |

An unreachable backend fails the boot by default, so a wrong address stops a
deployment instead of quietly degrading it. `lenient` turns that into a warning
and a cache that keeps nothing.

`prefix` is load-bearing on Redis. A Lemmego application usually shares one
Redis database between its cache, its sessions and its queue, so `Flush` scans
for the prefix rather than calling `FLUSHDB` — which would log every user out
and drop queued work.

## Commands

    lemmego run cache:clear      empty the cache
    lemmego run cache:forget k   remove one key
    lemmego run cache:prune      reclaim expired entries (file store)

## Testing

    go test ./...
    go test -race ./...
    GOWORK=off go test ./...

`storetest` is exported so a store outside this repository can prove itself
against the same contract the built-in drivers are held to:

```go
storetest.Run(t, storetest.Capabilities{...}, newStore)
```

Redis has a second suite against a real server, skipped unless configured:

    CACHE_REDIS_ADDR=127.0.0.1:6379 go test ./store/redis/

It covers what a simulator cannot — real expiry, the server's own Lua engine,
`SCAN` paging, and clients genuinely contending for a lock. Locks should not be
trusted on the strength of a simulator.
