package cache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	mathrand "math/rand/v2"
	"time"
)

// Lock is an atomic lock held across whatever scope its store spans: a process
// for the memory store, a host for the file store, a cluster for redis. The
// store's documentation says which, and the distinction matters — a
// process-local lock guarding work that runs on three machines guards nothing.
//
// A lock carries an owner token so that only the holder can release it. Without
// that, a lock whose TTL expired mid-work would be released by its original
// holder after someone else had already acquired it.
type Lock interface {
	// Acquire takes the lock if it is free, and reports whether it did.
	Acquire(ctx context.Context) (bool, error)

	// Block waits up to wait for the lock, polling with backoff. Reaching the
	// deadline returns (false, nil): a lock that was busy is an expected
	// outcome, not a failure.
	Block(ctx context.Context, wait time.Duration) (bool, error)

	// Get acquires the lock, runs fn, and releases it — including if fn
	// panics. It reports whether the lock was acquired; fn does not run if it
	// was not.
	Get(ctx context.Context, fn func(context.Context) error) (bool, error)

	// Release gives up the lock if this owner still holds it, and reports
	// whether it did. A false return means the lock had already expired and
	// been taken by someone else.
	Release(ctx context.Context) (bool, error)

	// ForceRelease removes the lock whoever holds it.
	ForceRelease(ctx context.Context) error

	// Owner returns this holder's token, which RestoreLock can rebind so a
	// lock taken in one place is released in another.
	Owner() string
}

// LockOption configures a lock.
type LockOption func(*lockOptions)

type lockOptions struct {
	owner         string
	retryInterval time.Duration
}

// WithOwner binds the lock to a known owner token instead of a fresh one.
func WithOwner(token string) LockOption {
	return func(o *lockOptions) { o.owner = token }
}

// WithRetryInterval sets how often Block re-checks the lock.
func WithRetryInterval(d time.Duration) LockOption {
	return func(o *lockOptions) { o.retryInterval = d }
}

const defaultRetryInterval = 100 * time.Millisecond

func resolveLockOptions(opts []LockOption) lockOptions {
	resolved := lockOptions{retryInterval: defaultRetryInterval}
	for _, opt := range opts {
		opt(&resolved)
	}
	if resolved.owner == "" {
		resolved.owner = newOwnerToken()
	}
	if resolved.retryInterval <= 0 {
		resolved.retryInterval = defaultRetryInterval
	}
	return resolved
}

// newOwnerToken returns a token no other holder will guess or collide with.
func newOwnerToken() string {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand does not fail on any supported platform, and a
		// predictable owner token would make release-by-owner meaningless.
		panic("cache: could not generate a lock owner token: " + err.Error())
	}
	return hex.EncodeToString(buf)
}

// BlockUntil is the shared Block implementation: poll acquire with jittered
// backoff until the deadline. Jitter matters because without it every waiter
// wakes on the same schedule and they retry in lockstep forever.
func BlockUntil(ctx context.Context, wait, interval time.Duration, acquire func(context.Context) (bool, error)) (bool, error) {
	if interval <= 0 {
		interval = defaultRetryInterval
	}
	deadline := time.Now().Add(wait)
	for {
		acquired, err := acquire(ctx)
		if err != nil || acquired {
			return acquired, err
		}
		if !time.Now().Before(deadline) {
			return false, nil
		}

		jittered := interval/2 + time.Duration(mathrand.Int64N(int64(interval)))
		if remaining := time.Until(deadline); jittered > remaining {
			jittered = remaining
		}
		timer := time.NewTimer(jittered)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false, ctx.Err()
		case <-timer.C:
		}
	}
}

// RunLocked is the shared Get implementation. The release is deferred so a
// panic in fn still frees the lock rather than holding it until its TTL.
func RunLocked(ctx context.Context, l Lock, fn func(context.Context) error) (bool, error) {
	acquired, err := l.Acquire(ctx)
	if err != nil || !acquired {
		return false, err
	}
	defer func() { _, _ = l.Release(ctx) }()
	return true, fn(ctx)
}

// unsupportedLock is what a store with no lock support hands back. Every
// operation refuses; nothing ever reports a lock it does not hold.
type unsupportedLock struct{ owner string }

func (l unsupportedLock) Acquire(context.Context) (bool, error) { return false, ErrUnsupported }
func (l unsupportedLock) Block(context.Context, time.Duration) (bool, error) {
	return false, ErrUnsupported
}
func (l unsupportedLock) Get(context.Context, func(context.Context) error) (bool, error) {
	return false, ErrUnsupported
}
func (l unsupportedLock) Release(context.Context) (bool, error) { return false, ErrUnsupported }
func (l unsupportedLock) ForceRelease(context.Context) error    { return ErrUnsupported }
func (l unsupportedLock) Owner() string                         { return l.owner }
