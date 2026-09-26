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
