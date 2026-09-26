package cache_test

import (
	"context"
	"go/format"
	"go/parser"
	"go/token"
	"testing"
	"time"

	"github.com/lemmego/api/app"
	"github.com/lemmego/api/config"
	"github.com/lemmego/cache"
	_ "github.com/lemmego/cache/drivers"
)

// A project that has never published a cache config must still get a working
// cache. Provide returning an error aborts the boot, and "you did not
// configure me" is not a reason to stop an application starting.
func TestProvideWithoutConfigurationStillWorks(t *testing.T) {
	a := newTestApp(t, nil)

	provider := &cache.Provider{Config: &cache.Config{Driver: "memory"}}
	if err := provider.Provide(a); err != nil {
		t.Fatalf("Provide() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	c := provider.Cache()
	if c == nil {
		t.Fatal("Provide() registered no cache")
	}

	ctx := context.Background()
	if err := c.Put(ctx, "k", 1, time.Minute); err != nil {
		t.Fatalf("the cache does not work: %v", err)
	}
}

// Configuration that is present but wrong is a different matter: it is a
// mistake someone made, and it should stop the boot rather than be guessed at.
func TestProvideRejectsInvalidConfiguration(t *testing.T) {
	a := newTestApp(t, config.M{"driver": "memcached"})

	provider := &cache.Provider{}
	err := provider.Provide(a)
	if err == nil {
		t.Fatal("Provide() accepted an unregistered driver")
	}
}

func TestProvideInstallsTheGlobalAndTheService(t *testing.T) {
	cache.SetGlobal(nil)
	t.Cleanup(func() { cache.SetGlobal(nil) })

	a := newTestApp(t, config.M{"driver": "memory", "prefix": "app:"})
	provider := &cache.Provider{}
	if err := provider.Provide(a); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	if cache.Global() == nil {
		t.Error("Provide() did not install the global used by the package functions")
	}
	if cache.FromApp(a) == nil {
		t.Error("Provide() did not register the cache as a service")
	}

	ctx := context.Background()
	if err := cache.Put(ctx, "via-facade", "value", time.Minute); err != nil {
		t.Fatalf("the facade does not reach the provided cache: %v", err)
	}
	if got, found, _ := cache.FromApp(a).GetAs[string](ctx, "via-facade"); !found || got != "value" {
		t.Error("the facade and the service are not the same cache")
	}
}

// Shutting down must not clear a global another provider has since installed.
func TestShutdownOnlyClearsItsOwnGlobal(t *testing.T) {
	a := newTestApp(t, config.M{"driver": "memory"})
	first := &cache.Provider{}
	if err := first.Provide(a); err != nil {
		t.Fatal(err)
	}

	replacement := cache.New(cache.Global().Store())
	cache.SetGlobal(replacement)
	t.Cleanup(func() { cache.SetGlobal(nil) })

	if err := first.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if cache.Global() != replacement {
		t.Error("Shutdown() cleared a global it did not install")
	}
}

func TestShutdownIsIdempotent(t *testing.T) {
	a := newTestApp(t, config.M{"driver": "memory"})
	provider := &cache.Provider{}
	if err := provider.Provide(a); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := provider.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := provider.Shutdown(ctx); err != nil {
		t.Errorf("second Shutdown() error = %v", err)
	}
}

func TestProviderPublishesItsConfigFile(t *testing.T) {
	provider := &cache.Provider{}
	publishables := provider.AddPublishables()
	if len(publishables) != 1 {
		t.Fatalf("AddPublishables() = %d entries, want 1", len(publishables))
	}
	if publishables[0].FilePath != "internal/configs/cache.go" {
		t.Errorf("FilePath = %q", publishables[0].FilePath)
	}
	if publishables[0].Tag != "config" {
		t.Errorf("Tag = %q, want config", publishables[0].Tag)
	}
}

func TestProviderContributesCommands(t *testing.T) {
	provider := &cache.Provider{}
	if got := len(provider.AddCommands()); got != 3 {
		t.Errorf("AddCommands() = %d, want 3 (clear, forget, prune)", got)
	}
}

// The stub is a Go source file written into someone's project. A syntax error
// in it is only discovered by whoever publishes it, so parse it here.
func TestConfigStubIsValidGo(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "cache.go", cache.ConfigStub, parser.AllErrors)
	if err != nil {
		t.Fatalf("the published config stub does not parse: %v", err)
	}
	if file.Name.Name != "configs" {
		t.Errorf("stub package = %q, want configs", file.Name.Name)
	}

	// gofmt disagreeing means the scaffold writes unformatted code into a
	// project that is expected to be gofmt-clean.
	formatted, err := format.Source([]byte(cache.ConfigStub))
	if err != nil {
		t.Fatalf("the stub cannot be formatted: %v", err)
	}
	if string(formatted) != cache.ConfigStub {
		t.Error("the published config stub is not gofmt-clean")
	}
}

// The stub must set the keys the provider reads back, or configuration would
// silently do nothing.
func TestConfigStubSetsWhatTheProviderReads(t *testing.T) {
	stub := cache.ConfigStub
	for _, want := range []string{
		"package configs", "config.Set(\"cache\"",
		"\"driver\"", "\"prefix\"", "\"ttl\"", "\"codec\"", "\"events\"", "\"lenient\"",
		"\"stores\"", "\"file\"", "\"redis\"",
		"CACHE_DRIVER", "CACHE_PREFIX", "CACHE_REDIS_ADDR",
	} {
		if !contains(stub, want) {
			t.Errorf("the config stub does not mention %q", want)
		}
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// newTestApp builds an app carrying just the cache configuration.
func newTestApp(t *testing.T, cacheConfig config.M) app.App {
	t.Helper()

	settings := config.M{}
	if cacheConfig != nil {
		settings["cache"] = cacheConfig
	}
	return app.Configure(app.WithConfig(settings))
}
