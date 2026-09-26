package cache

import "sync"

var (
	globalMu sync.RWMutex
	global   *Cache
)

// SetGlobal installs the cache the package-level functions use. The Provider
// calls it at boot; tests call it with nil to tear it down.
func SetGlobal(c *Cache) {
	globalMu.Lock()
	global = c
	globalMu.Unlock()
}

// Global returns the installed cache, or nil.
func Global() *Cache {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return global
}
