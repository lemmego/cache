package cache

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
)

// Factory builds a store from a resolved configuration.
type Factory func(ctx context.Context, cfg *Config) (Store, error)

var (
	driverMu sync.RWMutex
	drivers  = map[string]Factory{}
)

// Register makes a driver available to Build under name.
//
// Drivers register themselves from an init function and are selected by a blank
// import, the way database/sql drivers are. The indirection is what lets the
// drivers depend on this package — they need Store and ErrMiss — without this
// package depending on them, and it means an application that only caches in
// memory does not compile a Redis client into its binary.
//
// Importing github.com/lemmego/cache/drivers registers all four built-in
// stores at once.
func Register(name string, factory Factory) {
	if name == "" || factory == nil {
		panic("cache: Register needs a name and a factory")
	}
	driverMu.Lock()
	defer driverMu.Unlock()
	if _, taken := drivers[name]; taken {
		panic("cache: driver " + name + " is registered twice")
	}
	drivers[name] = factory
}

// SupportedDrivers lists the registered driver names.
func SupportedDrivers() []string {
	driverMu.RLock()
	defer driverMu.RUnlock()

	names := make([]string, 0, len(drivers))
	for name := range drivers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func lookupDriver(name string) (Factory, bool) {
	driverMu.RLock()
	defer driverMu.RUnlock()
	factory, ok := drivers[name]
	return factory, ok
}

// Build creates the store a Config describes.
//
// A backend that cannot be reached is reported here rather than on first use,
// so a wrong address fails the boot instead of surfacing later as a failed
// request. Config.Lenient turns that into a warning and a store that keeps
// nothing: every read then misses, which is slow but still serving.
func Build(ctx context.Context, cfg *Config) (Store, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	if err := cfg.normalizeAndValidate(); err != nil {
		return nil, err
	}

	factory, ok := lookupDriver(cfg.Driver)
	if !ok {
		return nil, fmt.Errorf(
			"cache: no driver registered for %q (registered: %v); "+
				"import github.com/lemmego/cache/drivers to register the built-in stores",
			cfg.Driver, SupportedDrivers())
	}

	store, err := factory(ctx, cfg)
	if err != nil {
		return degrade(cfg, err)
	}
	return store, nil
}

func degrade(cfg *Config, err error) (Store, error) {
	if !cfg.Lenient {
		return nil, fmt.Errorf("cache: %w", err)
	}

	fallback, ok := lookupDriver("null")
	if !ok {
		return nil, fmt.Errorf("cache: %w (and no null driver is registered to fall back to)", err)
	}
	slog.Warn("cache: the configured store is unavailable; falling back to one that keeps nothing, so every read will miss",
		"driver", cfg.Driver, "error", err)
	return fallback(context.Background(), cfg)
}
