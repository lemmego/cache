package cache

import (
	"testing"
	"time"

	"github.com/lemmego/api/config"
)

// The scaffold default is file, not memory: the CLI and the server are
// separate processes, so a memory cache would make cache:clear a silent no-op.
func TestDefaultDriverIsShareableBetweenProcesses(t *testing.T) {
	if got := DefaultConfig().Driver; got != "file" {
		t.Errorf("default driver = %q, want file", got)
	}
}

func TestResolveConfigLayersDefaultsThenConfigThenExplicit(t *testing.T) {
	appConfig := config.M{
		"driver": "memory",
		"prefix": "from-config:",
		"codec":  "gob",
		"ttl":    120,
	}

	t.Run("defaults alone", func(t *testing.T) {
		cfg, err := resolveConfig(nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Driver != "file" || cfg.Codec != "json" {
			t.Errorf("cfg = %+v, want the defaults", cfg)
		}
	})

	t.Run("config overrides the defaults", func(t *testing.T) {
		cfg, err := resolveConfig(nil, appConfig)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Driver != "memory" || cfg.Prefix != "from-config:" || cfg.Codec != "gob" {
			t.Errorf("cfg = %+v", cfg)
		}
		if cfg.DefaultTTL != 2*time.Minute {
			t.Errorf("ttl = %v, want 2m from a seconds value", cfg.DefaultTTL)
		}
	})

	t.Run("explicit overrides the config", func(t *testing.T) {
		cfg, err := resolveConfig(&Config{Driver: "null", Prefix: "explicit:"}, appConfig)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Driver != "null" || cfg.Prefix != "explicit:" {
			t.Errorf("cfg = %+v", cfg)
		}
		// Untouched fields still come from the config below it.
		if cfg.Codec != "gob" {
			t.Errorf("codec = %q, want the configured gob", cfg.Codec)
		}
	})
}

func TestResolveConfigReadsNestedStoreSettings(t *testing.T) {
	cfg, err := resolveConfig(nil, config.M{
		"driver": "redis",
		"stores": config.M{
			"file":  config.M{"path": "/var/cache/app"},
			"redis": config.M{"addr": "10.0.0.5:6379", "db": 4, "password": "secret"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Redis.Addr != "10.0.0.5:6379" || cfg.Redis.DB != 4 || cfg.Redis.Password != "secret" {
		t.Errorf("redis config = %+v", cfg.Redis)
	}
	if cfg.File.Path != "/var/cache/app" {
		t.Errorf("file path = %q", cfg.File.Path)
	}
}

// keyvalue-style config is gated behind a redis flag in the scaffold, so a
// project without redis has no such section at all. That must not error.
func TestResolveConfigToleratesMissingSections(t *testing.T) {
	cfg, err := resolveConfig(nil, config.M{"driver": "memory"})
	if err != nil {
		t.Fatalf("resolveConfig() error = %v", err)
	}
	if cfg.Redis.Addr == "" {
		t.Error("the redis default was not filled in")
	}
}

// Laravel calls an in-process cache "array"; accept the name.
func TestArrayIsAnAliasForMemory(t *testing.T) {
	cfg, err := resolveConfig(&Config{Driver: "array"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Driver != "memory" {
		t.Errorf("driver = %q, want memory", cfg.Driver)
	}
}

func TestUnknownCodecIsRejected(t *testing.T) {
	if _, err := resolveConfig(&Config{Codec: "protobuf"}, nil); err == nil {
		t.Fatal("resolveConfig() accepted an unknown codec")
	}
}

// An application usually has one Redis, configured once. Before this, the
// cache had its own address that defaulted to 127.0.0.1:6379 while the session
// read keyvalue.connections.redis and the queue read tasker.redis_addr — three
// independent settings for one server, and pointing them all at a real Redis
// meant saying so three times.
func TestSharedRedisConnectionIsUsedWhenTheCacheDoesNotSayOtherwise(t *testing.T) {
	shared := config.M{"host": "10.0.0.7", "port": 6380, "password": "hunter2"}

	cfg, err := resolveConfig(nil, config.M{"driver": "redis"}, shared)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Redis.Addr != "10.0.0.7:6380" {
		t.Errorf("addr = %q, want the shared connection", cfg.Redis.Addr)
	}
	if cfg.Redis.Password != "hunter2" {
		t.Errorf("password = %q, want the shared one", cfg.Redis.Password)
	}
}

// A cache that names its own Redis still wins, so an application can point its
// cache at a different server or database.
func TestTheCacheOverridesTheSharedRedisConnection(t *testing.T) {
	shared := config.M{"host": "10.0.0.7", "port": 6380}
	own := config.M{
		"driver": "redis",
		"stores": config.M{"redis": config.M{"addr": "cache-only:6390", "db": 3}},
	}

	cfg, err := resolveConfig(nil, own, shared)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Redis.Addr != "cache-only:6390" {
		t.Errorf("addr = %q, want the cache's own", cfg.Redis.Addr)
	}
	if cfg.Redis.DB != 3 {
		t.Errorf("db = %d, want 3", cfg.Redis.DB)
	}
}

// No shared block is the common case and must change nothing.
func TestAbsentSharedRedisLeavesTheDefault(t *testing.T) {
	cfg, err := resolveConfig(nil, config.M{"driver": "redis"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Redis.Addr != DefaultConfig().Redis.Addr {
		t.Errorf("addr = %q, want the default", cfg.Redis.Addr)
	}
}

// A shared block with no host is not a configuration at all.
func TestSharedRedisWithoutAHostIsIgnored(t *testing.T) {
	cfg, err := resolveConfig(nil, config.M{"driver": "redis"}, config.M{"password": "x"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Redis.Addr != DefaultConfig().Redis.Addr {
		t.Errorf("addr = %q, want the default", cfg.Redis.Addr)
	}
}
