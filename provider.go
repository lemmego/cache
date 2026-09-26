package cache

import (
	"context"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/lemmego/api/app"
	"github.com/lemmego/api/config"
)

// Provider wires a cache into an application.
//
// Register it in bootstrap/providers.go, and import the drivers you want:
//
//	import _ "github.com/lemmego/cache/drivers"
//
//	func LoadProviders() []app.Provider {
//	    return []app.Provider{ &cache.Provider{} }
//	}
type Provider struct {
	// Config overrides what the application's cache config says. Fields left
	// zero fall through to it.
	Config *Config

	mu       sync.RWMutex
	resolved *Config
	cache    *Cache
	store    Store

	shutdownOnce sync.Once
	shutdownErr  error
}

var _ app.Provider = (*Provider)(nil)

// Provide builds the cache and registers it.
//
// An application with no cache configuration gets a working cache on the
// defaults rather than an error: returning one from Provide aborts the boot,
// and a project that has not published a cache config has not asked for the
// app to stop. Configuration that is present but wrong is a different matter
// and is reported.
func (p *Provider) Provide(a app.App) error {
	appConfig, _ := a.Config().Get("cache").(config.M)

	// An application usually has one Redis. Reading the shared connection
	// block means a project that configured Redis once does not have to
	// repeat the address here, while cache.stores.redis still wins when it
	// says something.
	sharedRedis, _ := a.Config().Get("keyvalue.connections.redis").(config.M)

	cfg, err := resolveConfig(p.Config, appConfig, sharedRedis)
	if err != nil {
		return err
	}

	store, err := Build(context.Background(), cfg)
	if err != nil {
		return err
	}

	codec, _ := codecFor(cfg.Codec)
	opts := []Option{WithCodec(codec)}
	if cfg.Events {
		// Bridge typed cache events onto the application emitter, so listeners
		// registered with a.On see them. It is opt-in because dispatch is
		// synchronous: a listener runs on the goroutine that read the cache.
		opts = append(opts, WithEventSink(func(e Event) { a.Dispatch(e.EventName(), e) }))
	}

	c := New(store, opts...)

	p.mu.Lock()
	p.resolved, p.cache, p.store = cfg, c, store
	p.mu.Unlock()

	SetGlobal(c)
	a.AddService(c)
	a.AddService(cfg)

	slog.Debug("cache: ready", "driver", cfg.Driver, "codec", cfg.Codec, "prefix", cfg.Prefix)
	return nil
}

// AddCommands contributes the cache CLI commands.
func (p *Provider) AddCommands() []app.Command {
	return []app.Command{
		clearCommand,
		forgetCommand,
		pruneCommand,
	}
}

// AddPublishables offers the cache config file to `lemmego publish`.
func (p *Provider) AddPublishables() []*app.Publishable {
	return []*app.Publishable{
		// The tag is namespaced because --tags is a real selector: a bare
		// "config" collides with every other package that publishes one, so
		// asking for this module's meant getting all of them.
		{FilePath: "internal/configs/cache.go", Content: []byte(ConfigStub), Tag: TagConfig},
	}
}

// Shutdown releases the store's resources. It is safe to call more than once.
func (p *Provider) Shutdown(context.Context) error {
	p.shutdownOnce.Do(func() {
		p.mu.RLock()
		store, c := p.store, p.cache
		p.mu.RUnlock()

		// Only clear the global if it is still ours: a second provider may
		// have replaced it, and stomping that would break the live cache.
		if c != nil && Global() == c {
			SetGlobal(nil)
		}
		if closer, ok := store.(interface{ Close() error }); ok {
			p.shutdownErr = closer.Close()
		}
	})
	return p.shutdownErr
}

// Cache returns the cache this provider built, or nil before Provide runs.
func (p *Provider) Cache() *Cache {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.cache
}

// FromApp resolves the cache from the service container.
//
// Other providers in this framework spell this Get, but here that name belongs
// to the generic facade function callers reach for constantly —
// cache.Get[User](ctx, key) — so the resolver takes the longer name rather
// than the API you type every day.
func FromApp(a app.App) *Cache { return app.Get[*Cache](a) }

// resolveConfig layers the defaults, the application's config, and the
// provider's own fields, in that order of increasing priority.
func resolveConfig(explicit *Config, appConfig config.M, sharedRedis ...config.M) (*Config, error) {
	cfg := DefaultConfig()

	// Order matters: the shared connection is a weaker source than anything
	// written under cache, which is weaker than what the provider was given
	// directly.
	for _, shared := range sharedRedis {
		cfg.applySharedRedis(shared)
	}
	cfg.applyOverrides(appConfig)
	cfg.applyExplicit(explicit)

	if err := cfg.normalizeAndValidate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// applyOverrides copies every key present in m, including zero values — a
// configured false or 0 is a decision, not an absence.
//
// Every read passes a default, because config.M's typed accessors index their
// variadic default unconditionally and panic without one.
func (c *Config) applyOverrides(m config.M) {
	if m == nil {
		return
	}

	c.Driver = m.String("driver", c.Driver)
	c.Prefix = m.String("prefix", c.Prefix)
	c.Codec = m.String("codec", c.Codec)
	c.Events = m.Bool("events", c.Events)
	c.Lenient = m.Bool("lenient", c.Lenient)

	// A TTL may arrive as a duration or as a count of seconds, depending on
	// how the config file spells it.
	if ttl, ok := m.Lookup("ttl"); ok {
		switch value := ttl.(type) {
		case time.Duration:
			c.DefaultTTL = value
		case int:
			c.DefaultTTL = time.Duration(value) * time.Second
		case int64:
			c.DefaultTTL = time.Duration(value) * time.Second
		}
	}

	if stores, ok := config.Lookup[config.M](m, "stores"); ok {
		if fileCfg, ok := config.Lookup[config.M](stores, "file"); ok {
			c.File.Path = fileCfg.String("path", c.File.Path)
		}
		if redisCfg, ok := config.Lookup[config.M](stores, "redis"); ok {
			c.Redis.Addr = redisCfg.String("addr", c.Redis.Addr)
			c.Redis.Password = redisCfg.String("password", c.Redis.Password)
			c.Redis.DB = redisCfg.Int("db", c.Redis.DB)
			c.Redis.PoolSize = redisCfg.Int("pool_size", c.Redis.PoolSize)
			c.Redis.AllowFlushDB = redisCfg.Bool("allow_flush_db", c.Redis.AllowFlushDB)
		}
	}
}

// applyExplicit lets the provider's own fields win over configuration.
func (c *Config) applyExplicit(explicit *Config) {
	if explicit == nil {
		return
	}
	if explicit.Driver != "" {
		c.Driver = explicit.Driver
	}
	if explicit.Prefix != "" {
		c.Prefix = explicit.Prefix
	}
	if explicit.Codec != "" {
		c.Codec = explicit.Codec
	}
	if explicit.DefaultTTL > 0 {
		c.DefaultTTL = explicit.DefaultTTL
	}
	if explicit.Events {
		c.Events = true
	}
	if explicit.Lenient {
		c.Lenient = true
	}
	if explicit.File.Path != "" {
		c.File.Path = explicit.File.Path
	}
	if explicit.Redis.Addr != "" {
		c.Redis.Addr = explicit.Redis.Addr
	}
	if explicit.Redis.Password != "" {
		c.Redis.Password = explicit.Redis.Password
	}
	if explicit.Redis.DB != 0 {
		c.Redis.DB = explicit.Redis.DB
	}
	if explicit.Redis.PoolSize != 0 {
		c.Redis.PoolSize = explicit.Redis.PoolSize
	}
	if explicit.Redis.AllowFlushDB {
		c.Redis.AllowFlushDB = true
	}
}

// applySharedRedis takes the address and password from the application's shared
// Redis connection, so one configured Redis serves the cache too.
//
// It is applied before the cache's own settings, so cache.stores.redis still
// overrides it. The shared block spells the address as separate host and port,
// which is the shape the session provider already reads.
func (c *Config) applySharedRedis(shared config.M) {
	if shared == nil {
		return
	}

	host := shared.String("host")
	if host == "" {
		return
	}
	port := shared.Int("port", 6379)
	c.Redis.Addr = net.JoinHostPort(host, strconv.Itoa(port))

	if password := shared.String("password"); password != "" {
		c.Redis.Password = password
	}
}

// TagConfig is what `lemmego publish --tags` selects this module's
// configuration on.
const TagConfig = "cache-config"
