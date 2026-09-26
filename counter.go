package cache

import (
	"strconv"
	"strings"
)

// Counters are stored as their decimal ASCII form rather than through the
// codec. That is not an arbitrary choice: it is exactly what redis INCRBY
// reads and writes, so a counter incremented natively by redis and one
// incremented by the memory or file store have the same bytes, and a value
// written by Put can be incremented afterwards. Encoding counters as JSON
// would make the redis driver unable to use INCRBY at all.

// FormatCounter renders a counter as a store writes it.
func FormatCounter(n int64) []byte {
	return []byte(strconv.FormatInt(n, 10))
}

// ParseCounter reads a counter, returning ErrNotNumeric for anything else.
func ParseCounter(value []byte) (int64, error) {
	n, err := strconv.ParseInt(strings.TrimSpace(string(value)), 10, 64)
	if err != nil {
		return 0, ErrNotNumeric
	}
	return n, nil
}
