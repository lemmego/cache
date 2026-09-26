package cache

import (
	"fmt"
	"strings"
	"time"
)

// Config describes how to build a cache.
type Config struct {
	// Driver selects the store: memory, file, redis or null.
	Driver string

	// Prefix namespaces every key. It also bounds what Flush removes, which is
	// why a store sharing its backing with other data insists on one.
	Prefix string

	// DefaultTTL is used where a caller does not give one.
	DefaultTTL time.Duration

	// Codec selects the encoding: json or gob.
	Codec string

	// Events enables bridging cache events onto the application's emitter.
	// Off by default: dispatching is synchronous, so a listener runs on the
	// goroutine that read the cache.
	Events bool

	// Lenient degrades to the null store when the configured backend cannot be
	// reached, rather than failing the boot. The cache then misses on every
	// read, which is slow but serving.
	Lenient bool

	File  FileConfig
	Redis RedisConfig
}

// FileConfig configures the file driver.
type FileConfig struct {
	Path string
}

// RedisConfig configures the redis driver.
type RedisConfig struct {
	Addr     string
	Password string
	DB       int
	PoolSize int

	// AllowFlushDB lets Flush empty the whole database instead of scanning for
	// the prefix. Only safe when the database holds nothing but this cache.
	AllowFlushDB bool
}

// MemoryGCInterval is how often the memory driver sweeps expired entries.
const MemoryGCInterval = time.Minute

// DefaultConfig returns the configuration used when nothing overrides it.
//
// The driver is file rather than memory on purpose: the CLI and the server are
// separate processes, so a memory cache would make `cache:clear` a silent
// no-op and leave the two with different views of the same data.
func DefaultConfig() *Config {
	return &Config{
		Driver:     "file",
		Prefix:     "lemmego_cache:",
		DefaultTTL: time.Hour,
		Codec:      "json",
		File:       FileConfig{Path: "storage/framework/cache"},
		Redis:      RedisConfig{Addr: "127.0.0.1:6379", DB: 1},
	}
}

// normalizeAndValidate fills in blanks and rejects what cannot work.
func (c *Config) normalizeAndValidate() error {
	c.Driver = strings.ToLower(strings.TrimSpace(c.Driver))
	switch c.Driver {
	case "":
		c.Driver = "file"
	case "array":
		// Laravel's name for an in-process cache.
		c.Driver = "memory"
	}

	if c.Prefix == "" {
		c.Prefix = DefaultConfig().Prefix
	}
	if c.DefaultTTL <= 0 {
		c.DefaultTTL = DefaultConfig().DefaultTTL
	}
	if _, ok := codecFor(c.Codec); !ok {
		return fmt.Errorf("cache: unsupported codec %q (supported: json, gob)", c.Codec)
	}

	// Driver-specific blanks. An unknown driver is not rejected here: Build
	// reports it against the set actually registered, which is the set that
	// matters.
	if c.File.Path == "" {
		c.File.Path = DefaultConfig().File.Path
	}
	if c.Redis.Addr == "" {
		c.Redis.Addr = DefaultConfig().Redis.Addr
	}
	return nil
}
