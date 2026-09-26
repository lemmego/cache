// Package drivers registers every built-in cache store.
//
// Import it for its side effects when an application wants all of them
// available to configuration:
//
//	import _ "github.com/lemmego/cache/drivers"
//
// To keep a Redis client out of a binary that will never use one, import only
// the stores you need instead:
//
//	import _ "github.com/lemmego/cache/store/memory"
//	import _ "github.com/lemmego/cache/store/file"
package drivers

import (
	_ "github.com/lemmego/cache/store/file"
	_ "github.com/lemmego/cache/store/memory"
	_ "github.com/lemmego/cache/store/null"
	_ "github.com/lemmego/cache/store/redis"
)
