package null_test

import (
	"testing"
	"time"

	"github.com/lemmego/cache"
	"github.com/lemmego/cache/store/null"
	"github.com/lemmego/cache/storetest"
)

type frozenClock struct{}

func (frozenClock) Advance(time.Duration) {}

func TestConformance(t *testing.T) {
	storetest.Run(t, storetest.Capabilities{}, func(t *testing.T) (cache.Store, storetest.Clock) {
		return null.New("test:"), frozenClock{}
	})
}
