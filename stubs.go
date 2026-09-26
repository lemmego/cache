package cache

// ConfigStub is the cache configuration published into a project by
// `lemmego publish --tags=config`, and written by `lemmego new`.
const ConfigStub = `package configs

import "github.com/lemmego/api/config"

func init() {
	config.Set("cache", config.M{
		// file keeps the cache on disk, so the CLI and the server share it.
		// memory is faster but private to one process, which makes
		// ` + "`lemmego run cache:clear`" + ` unable to reach the server's cache.
		"driver": config.MustEnv("CACHE_DRIVER", "file"),

		// Every key is written under this prefix, and it is what bounds a
		// flush — important on a Redis database shared with sessions.
		"prefix": config.MustEnv("CACHE_PREFIX", "lemmego_cache:"),

		// Seconds.
		"ttl": config.MustEnv("CACHE_TTL", 3600),

		// json is portable and readable; gob preserves Go types exactly.
		"codec": config.MustEnv("CACHE_CODEC", "json"),

		// Publish cache events onto the application emitter. Listeners run
		// synchronously on the goroutine that touched the cache.
		"events": config.MustEnv("CACHE_EVENTS", false),

		// Serve without a cache rather than failing to boot when the
		// configured backend is unreachable. Every read then misses.
		"lenient": config.MustEnv("CACHE_LENIENT", false),

		"stores": config.M{
			"file": config.M{
				"path": config.MustEnv("CACHE_FILE_PATH", "storage/framework/cache"),
			},
			"redis": config.M{
				"addr":     config.MustEnv("CACHE_REDIS_ADDR", "127.0.0.1:6379"),
				"password": config.MustEnv("CACHE_REDIS_PASSWORD", ""),
				"db":       config.MustEnv("CACHE_REDIS_DB", 1),
			},
		},
	})
}
`
