package file_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lemmego/cache"
	"github.com/lemmego/cache/store/file"
	"github.com/lemmego/cache/storetest"
)

type testClock struct{ offset time.Duration }

func (c *testClock) Advance(d time.Duration) { c.offset += d }
func (c *testClock) now() time.Time          { return time.Now().Add(c.offset) }

// foreignAware lets the conformance suite check that Flush spares data the
// store does not own. The helpers live on the test wrapper rather than on the
// store, so production code carries no test-only surface.
type foreignAware struct {
	*file.Store
	dir string
}

func (f foreignAware) PutForeign(_ context.Context, key string, value []byte) error {
	return os.WriteFile(filepath.Join(f.dir, key), value, 0o600)
}

func (f foreignAware) GetForeign(_ context.Context, key string) ([]byte, error) {
	return os.ReadFile(filepath.Join(f.dir, key))
}

func newStore(t *testing.T) (cache.Store, storetest.Clock) {
	t.Helper()
	clock := &testClock{}
	dir := t.TempDir()
	store, err := file.New(file.Config{Dir: dir, Prefix: "test:", Now: clock.now})
	if err != nil {
		t.Fatalf("file.New() error = %v", err)
	}
	return foreignAware{Store: store, dir: dir}, clock
}

func TestConformance(t *testing.T) {
	storetest.Run(t, storetest.Capabilities{
		Locks:           true,
		AtomicIncrement: true,
		ForeignKeySafe:  true,
		Persists:        true,
		// Reads and writes are synchronous syscalls with nothing to interrupt.
		HonoursContext: false,
		// Tag membership would need an index the filesystem does not give
		// cheaply; tagged caches fall back to version bumping.
		TagIndex: false,
	}, newStore)
}

// An unwritable location must be reported. The previous implementation panicked
// here, taking the process down at construction.
func TestNewReportsAnUnusableDirectory(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := file.New(file.Config{Dir: filepath.Join(blocker, "cache")}); err == nil {
		t.Fatal("file.New() returned nil for a path that cannot be created")
	}
}

func TestNewRequiresADirectory(t *testing.T) {
	if _, err := file.New(file.Config{}); err == nil {
		t.Fatal("file.New() accepted an empty Dir")
	}
}

// A half-written entry must read as a miss. Failing a request because a cache
// file was truncated defeats the point of caching.
func TestCorruptEntryReadsAsAMiss(t *testing.T) {
	clock := &testClock{}
	dir := t.TempDir()
	store, err := file.New(file.Config{Dir: dir, Prefix: "test:", Now: clock.now})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if err := store.Put(ctx, "k", []byte("value"), time.Minute); err != nil {
		t.Fatal(err)
	}

	// Truncate every entry to less than a header.
	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		return os.WriteFile(path, []byte{0x00}, 0o600)
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := store.Get(ctx, "k"); !errors.Is(err, cache.ErrMiss) {
		t.Fatalf("Get() on a truncated entry error = %v, want ErrMiss", err)
	}
}

// Regression: Flush used to RemoveAll every directory entry, so a cache
// directory shared with anything else lost that data too.
func TestFlushLeavesUnrelatedFiles(t *testing.T) {
	dir := t.TempDir()
	store, err := file.New(file.Config{Dir: dir, Prefix: "test:"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	keep := filepath.Join(dir, ".gitignore")
	if err := os.WriteFile(keep, []byte("*\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(dir, "uploads")
	if err := os.MkdirAll(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	keepNested := filepath.Join(nested, "receipt.pdf")
	if err := os.WriteFile(keepNested, []byte("%PDF"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := store.Put(ctx, "k", []byte("v"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := store.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	for _, path := range []string{keep, keepNested} {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("Flush() removed %s, which the cache does not own", path)
		}
	}
	if _, err := store.Get(ctx, "k"); !errors.Is(err, cache.ErrMiss) {
		t.Error("Flush() left the cache entry behind")
	}
}

func TestPruneRemovesOnlyExpiredEntries(t *testing.T) {
	clock := &testClock{}
	dir := t.TempDir()
	store, err := file.New(file.Config{Dir: dir, Prefix: "test:", Now: clock.now})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if err := store.Put(ctx, "short", []byte("v"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, "long", []byte("v"), time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, "kept", []byte("v"), cache.Forever); err != nil {
		t.Fatal(err)
	}

	clock.Advance(61 * time.Second)
	if err := store.Prune(ctx); err != nil {
		t.Fatalf("Prune() error = %v", err)
	}

	if _, err := store.Get(ctx, "short"); !errors.Is(err, cache.ErrMiss) {
		t.Error("Prune() left an expired entry")
	}
	for _, key := range []string{"long", "kept"} {
		if _, err := store.Get(ctx, key); err != nil {
			t.Errorf("Prune() removed %q, which had not expired: %v", key, err)
		}
	}
}

// Two stores over the same directory must see each other's writes — the
// property that makes this store usable from the CLI and the server at once.
func TestEntriesAreVisibleAcrossStores(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	writer, err := file.New(file.Config{Dir: dir, Prefix: "test:"})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Put(ctx, "shared", []byte("value"), time.Minute); err != nil {
		t.Fatal(err)
	}

	reader, err := file.New(file.Config{Dir: dir, Prefix: "test:"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := reader.Get(ctx, "shared")
	if err != nil {
		t.Fatalf("Get() from a second store error = %v", err)
	}
	if string(got) != "value" {
		t.Errorf("Get() = %q, want %q", got, "value")
	}
}
