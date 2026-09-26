package cache

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// ErrTagsRequireTTL reports an attempt to store a non-expiring value under tags
// on a store that cannot enumerate tag members.
//
// Tag invalidation works by changing the namespace keys are written under,
// which makes the old entries unreachable but leaves them to expire on their
// own. An entry stored forever would never expire, so it would occupy space
// permanently with no way to reach or remove it. Refusing is better than a leak
// nobody can find; a store implementing TagIndex can delete the members and has
// no such limit.
var ErrTagsRequireTTL = errors.New("cache: this store cannot store a value forever under tags; give it a ttl or use a store with a tag index")

// taggedStore namespaces every key by the current version of its tags.
//
// Flushing a tag means minting it a new version, which is one write per tag and
// no scan — the alternative, enumerating every key to find the tagged ones, is
// O(cache size) on a data structure with no index.
type taggedStore struct {
	inner Store
	codec Codec
	tags  []string
}

var _ Store = (*taggedStore)(nil)

func newTaggedStore(inner Store, codec Codec, tags []string) *taggedStore {
	// Sort and dedupe, so Tags("a","b") and Tags("b","a") name one namespace.
	normalized := slices.Clone(tags)
	slices.Sort(normalized)
	normalized = slices.Compact(normalized)

	// Tagging a tagged cache should union the tags, not nest the decorators.
	if existing, ok := inner.(*taggedStore); ok {
		merged := append(slices.Clone(existing.tags), normalized...)
		slices.Sort(merged)
		return &taggedStore{inner: existing.inner, codec: codec, tags: slices.Compact(merged)}
	}
	return &taggedStore{inner: inner, codec: codec, tags: normalized}
}

// versionKey is a logical key: the underlying store applies its own prefix, so
// prepending one here would namespace it twice.
func (s *taggedStore) versionKey(tag string) string {
	return "tag:" + tag + ":v"
}

// namespace returns the prefix keys are currently written under, minting a
// version for any tag that does not have one.
func (s *taggedStore) namespace(ctx context.Context) (string, error) {
	keys := make([]string, len(s.tags))
	for i, tag := range s.tags {
		keys[i] = s.versionKey(tag)
	}

	found, err := s.inner.GetMany(ctx, keys)
	if err != nil {
		return "", err
	}

	versions := make([]string, len(s.tags))
	for i, tag := range s.tags {
		if value, ok := found[keys[i]]; ok {
			versions[i] = string(value)
			continue
		}
		// Add rather than Put: two processes minting the same tag at once
		// must converge on one version, not overwrite each other.
		minted := newVersionID()
		added, err := s.inner.Add(ctx, keys[i], []byte(minted), Forever)
		if err != nil {
			return "", err
		}
		if !added {
			current, err := s.inner.Get(ctx, keys[i])
			if err != nil {
				return "", fmt.Errorf("cache: reading the version of tag %q: %w", tag, err)
			}
			minted = string(current)
		}
		versions[i] = minted
	}

	sum := sha256.Sum256([]byte(strings.Join(versions, "|")))
	return hex.EncodeToString(sum[:10]) + ":", nil
}

func newVersionID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		panic("cache: could not generate a tag version: " + err.Error())
	}
	return hex.EncodeToString(buf)
}

func (s *taggedStore) key(ctx context.Context, key string) (string, error) {
	namespace, err := s.namespace(ctx)
	if err != nil {
		return "", err
	}
	return namespace + key, nil
}

// index returns the underlying store's tag index, if it has one.
func (s *taggedStore) index() (TagIndex, bool) {
	index, ok := s.inner.(TagIndex)
	return index, ok
}

func (s *taggedStore) Get(ctx context.Context, key string) ([]byte, error) {
	namespaced, err := s.key(ctx, key)
	if err != nil {
		return nil, err
	}
	return s.inner.Get(ctx, namespaced)
}

func (s *taggedStore) GetMany(ctx context.Context, keys []string) (map[string][]byte, error) {
	namespace, err := s.namespace(ctx)
	if err != nil {
		return nil, err
	}

	namespaced := make([]string, len(keys))
	for i, key := range keys {
		namespaced[i] = namespace + key
	}
	found, err := s.inner.GetMany(ctx, namespaced)
	if err != nil {
		return nil, err
	}

	// Hand back the caller's keys, not the namespaced ones.
	result := make(map[string][]byte, len(found))
	for i, key := range keys {
		if value, ok := found[namespaced[i]]; ok {
			result[key] = value
		}
	}
	return result, nil
}

func (s *taggedStore) Put(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	index, indexed := s.index()
	if ttl <= 0 && !indexed {
		return ErrTagsRequireTTL
	}

	namespaced, err := s.key(ctx, key)
	if err != nil {
		return err
	}
	if err := s.inner.Put(ctx, namespaced, value, ttl); err != nil {
		return err
	}
	return s.record(ctx, index, indexed, namespaced)
}

func (s *taggedStore) PutMany(ctx context.Context, values map[string][]byte, ttl time.Duration) error {
	index, indexed := s.index()
	if ttl <= 0 && !indexed {
		return ErrTagsRequireTTL
	}

	namespace, err := s.namespace(ctx)
	if err != nil {
		return err
	}

	namespaced := make(map[string][]byte, len(values))
	keys := make([]string, 0, len(values))
	for key, value := range values {
		full := namespace + key
		namespaced[full] = value
		keys = append(keys, full)
	}
	if err := s.inner.PutMany(ctx, namespaced, ttl); err != nil {
		return err
	}
	return s.record(ctx, index, indexed, keys...)
}

func (s *taggedStore) Add(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	index, indexed := s.index()
	if ttl <= 0 && !indexed {
		return false, ErrTagsRequireTTL
	}

	namespaced, err := s.key(ctx, key)
	if err != nil {
		return false, err
	}
	added, err := s.inner.Add(ctx, namespaced, value, ttl)
	if err != nil || !added {
		return added, err
	}
	return added, s.record(ctx, index, indexed, namespaced)
}

// record notes a key against every tag, so Flush can delete it rather than
// orphaning it until its TTL.
func (s *taggedStore) record(ctx context.Context, index TagIndex, indexed bool, keys ...string) error {
	if !indexed || len(keys) == 0 {
		return nil
	}
	for _, tag := range s.tags {
		if err := index.AddTagEntries(ctx, tag, keys...); err != nil {
			return err
		}
	}
	return nil
}

func (s *taggedStore) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	namespaced, err := s.key(ctx, key)
	if err != nil {
		return 0, err
	}
	return s.inner.Increment(ctx, namespaced, delta)
}

func (s *taggedStore) Decrement(ctx context.Context, key string, delta int64) (int64, error) {
	return s.Increment(ctx, key, -delta)
}

func (s *taggedStore) Forget(ctx context.Context, key string) (bool, error) {
	namespaced, err := s.key(ctx, key)
	if err != nil {
		return false, err
	}
	return s.inner.Forget(ctx, namespaced)
}

// Flush invalidates every entry under these tags by minting each a new version.
//
// Entries written under the old versions become unreachable at once. Where the
// store keeps a tag index they are also deleted; where it does not they remain
// until their own TTL, which is why a non-expiring value is refused at write
// time on such a store.
func (s *taggedStore) Flush(ctx context.Context) error {
	index, indexed := s.index()

	for _, tag := range s.tags {
		if indexed {
			keys, err := index.TagEntries(ctx, tag)
			if err != nil {
				return err
			}
			for _, key := range keys {
				if _, err := s.inner.Forget(ctx, key); err != nil {
					return err
				}
			}
			if err := index.ForgetTagEntries(ctx, tag); err != nil {
				return err
			}
		}
		if err := s.inner.Put(ctx, s.versionKey(tag), []byte(newVersionID()), Forever); err != nil {
			return err
		}
	}
	return nil
}

// Prefix reports the underlying prefix rather than the namespace, so a
// scan-based flush on the inner store still finds tagged entries.
func (s *taggedStore) Prefix() string { return s.inner.Prefix() }
