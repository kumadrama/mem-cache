package caches

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// A policy owns only the eviction index. store serializes changes to that
// index and its corresponding entries with the same lock.
type evictionPolicy interface {
	admit(key any) (victim any, evicted bool)
	touch(key any)
	replace(key any)
	forget(key any)
	reset()
}

type entry struct {
	value    any
	deadline time.Time
}

func (e entry) expired(now time.Time) bool {
	return !e.deadline.IsZero() && now.After(e.deadline)
}

type store struct {
	stats
	cfg     CacheBuilder
	mu      sync.RWMutex
	entries map[any]entry
	policy  evictionPolicy
	flights Group
	signals *signals
}

func (s *store) Set(k, v any) error { return s.put(context.Background(), k, v, nil) }

func (s *store) SetWithExpire(k, v any, ttl time.Duration) error {
	return s.put(context.Background(), k, v, &ttl)
}

func (s *store) put(ctx context.Context, k, v any, ttl *time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.putLocked(ctx, k, v, ttl)
}

func (s *store) putLocked(ctx context.Context, k, v any, ttl *time.Duration) error {
	if encode := s.cfg.serializeFunc; encode != nil {
		var err error
		v, err = encode(k, v)
		if err != nil {
			return err
		}
	}
	e, exists := s.entries[k]
	if exists {
		s.policy.replace(k)
		if h := s.cfg.replacedFunc; h != nil {
			h(k, e.value)
		}
	} else if victim, ok := s.policy.admit(k); ok {
		s.discardLocked(ctx, victim, "capacity")
	}
	if ttl == nil {
		ttl = s.cfg.expiration
	}
	if ttl != nil {
		e.deadline = s.cfg.clock.Now().Add(*ttl)
	}
	e.value = v
	s.entries[k] = e
	if !exists {
		s.signals.size(ctx, len(s.entries))
	}
	if h := s.cfg.addedFunc; h != nil {
		h(k, v)
	}
	return nil
}

// The eviction index has already removed k when this is called.
func (s *store) discardLocked(ctx context.Context, k any, reason string) {
	e := s.entries[k]
	delete(s.entries, k)
	s.signals.removed(ctx, reason, 1)
	if h := s.cfg.evictedFunc; h != nil {
		h(k, e.value)
	}
}

func (s *store) MSet(keys, values []any) error {
	return s.putBatch(context.Background(), keys, values, nil)
}

func (s *store) MSetWithExpire(keys, values []any, ttl time.Duration) error {
	return s.putBatch(context.Background(), keys, values, &ttl)
}

func (s *store) putBatch(ctx context.Context, keys, values []any, ttl *time.Duration) error {
	if len(keys) == 0 {
		return KeysSizeError
	}
	if len(keys) != len(values) {
		return KeysAndValuesNotMatchError
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, k := range keys {
		if err := s.putLocked(ctx, k, values[i], ttl); err != nil {
			return err
		}
	}
	return nil
}

func (s *store) lookupLocked(ctx context.Context, k any) (any, bool) {
	e, ok := s.entries[k]
	if !ok {
		return nil, false
	}
	if e.expired(s.cfg.clock.Now()) {
		s.policy.forget(k)
		s.discardLocked(ctx, k, "expired")
		s.signals.size(ctx, len(s.entries))
		return nil, false
	}
	s.policy.touch(k)
	return e.value, true
}

func (s *store) lookup(ctx context.Context, k any) (any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lookupLocked(ctx, k)
}

func (s *store) decoded(k, v any) (any, error) {
	if decode := s.cfg.deserializeFunc; decode != nil {
		return decode(k, v)
	}
	return v, nil
}

func (s *store) Get(k any) (any, error) { return s.GetContext(context.Background(), k) }

func (s *store) GetContext(ctx context.Context, k any) (any, error) { return s.readOne(ctx, k, true) }

func (s *store) GetIFPresent(k any) (any, error) {
	return s.GetIFPresentContext(context.Background(), k)
}

func (s *store) GetIFPresentContext(ctx context.Context, k any) (any, error) {
	return s.readOne(ctx, k, false)
}

func (s *store) readOne(ctx context.Context, k any, wait bool) (any, error) {
	if v, ok := s.lookup(ctx, k); ok {
		s.countLookups(ctx, 1, 0)
		return s.decoded(k, v)
	}
	s.countLookups(ctx, 0, 1)
	if s.cfg.load == nil {
		return nil, KeyNotFoundError
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	loadCtx := ctx
	op := "get"
	if !wait {
		loadCtx = context.WithoutCancel(ctx)
		op = "refresh"
	}
	v, _, err := s.flights.do(ctx, k, func() (any, error) {
		// A previous flight may have filled the entry after our first lookup.
		if cached, ok := s.lookup(loadCtx, k); ok {
			return s.decoded(k, cached)
		}
		var loaded any
		err := s.runLoad(loadCtx, op, 1, func() error {
			var ttl *time.Duration
			var err error
			loaded, ttl, err = s.cfg.load(loadCtx, k)
			if err != nil {
				return err
			}
			return s.put(loadCtx, k, loaded, ttl)
		})
		if err != nil {
			return nil, err
		}
		return loaded, nil
	}, wait)
	return v, err
}

func (s *store) runLoad(ctx context.Context, op string, n int, fn func() error) (err error) {
	start := time.Now()
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("cache loader panic: %v", p)
		}
		s.signals.loaded(ctx, op, n, time.Since(start), err)
	}()
	return fn()
}

func (s *store) MGet(keys []any) ([]any, []int, error) {
	return s.MGetContext(context.Background(), keys)
}

func (s *store) MGetContext(ctx context.Context, keys []any) ([]any, []int, error) {
	if len(keys) == 0 {
		return nil, nil, KeysSizeError
	}
	values := make([]any, len(keys))
	misses := s.readBatch(ctx, keys, values)
	s.countLookups(ctx, len(keys)-len(misses), len(misses))
	if s.cfg.deserializeFunc != nil {
		next := 0
		for i, k := range keys {
			if next < len(misses) && misses[next] == i {
				next++
				continue
			}
			v, err := s.decoded(k, values[i])
			values[i] = v
			if err != nil {
				return values, misses, err
			}
		}
	}
	if len(misses) == 0 || s.cfg.loadBatch == nil {
		return values, misses, nil
	}
	if err := ctx.Err(); err != nil {
		return values, misses, err
	}
	missingKeys := make([]any, len(misses))
	for i, slot := range misses {
		missingKeys[i] = keys[slot]
	}
	err := s.runLoad(ctx, "mget", len(misses), func() error {
		loaded, ttl, err := s.cfg.loadBatch(ctx, missingKeys)
		if err != nil {
			return err
		}
		if len(loaded) != len(misses) {
			return KeysAndValuesNotMatchError
		}
		for i, slot := range misses {
			values[slot] = loaded[i]
		}
		return s.putBatch(ctx, missingKeys, loaded, ttl)
	})
	return values, misses, err
}

func (s *store) readBatch(ctx context.Context, keys, values []any) []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	misses := make([]int, 0)
	for i, k := range keys {
		v, ok := s.lookupLocked(ctx, k)
		if !ok {
			misses = append(misses, i)
		}
		values[i] = v
	}
	return misses
}

func (s *store) countLookups(ctx context.Context, hits, misses int) {
	s.stats.add(hits, misses)
	s.signals.lookup(ctx, hits, misses)
}

func (s *store) Has(k any) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[k]
	return ok && !e.expired(s.cfg.clock.Now())
}

func (s *store) Remove(k any) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.entries[k]; !ok {
		return false
	}
	s.policy.forget(k)
	ctx := context.Background()
	s.discardLocked(ctx, k, "remove")
	s.signals.size(ctx, len(s.entries))
	return true
}

func (s *store) Purge() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if h := s.cfg.purgeVisitorFunc; h != nil {
		for k, e := range s.entries {
			h(k, e.value)
		}
	}
	ctx := context.Background()
	s.signals.removed(ctx, "purge", len(s.entries))
	clear(s.entries)
	s.policy.reset()
	s.signals.size(ctx, 0)
}

func (s *store) GetALL(checkExpired bool) map[any]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.cfg.clock.Now()
	values := make(map[any]any, len(s.entries))
	for k, e := range s.entries {
		if !checkExpired || !e.expired(now) {
			values[k] = e.value
		}
	}
	return values
}

func (s *store) Keys(checkExpired bool) []any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.cfg.clock.Now()
	keys := make([]any, 0, len(s.entries))
	for k, e := range s.entries {
		if !checkExpired || !e.expired(now) {
			keys = append(keys, k)
		}
	}
	return keys
}

func (s *store) Len(checkExpired bool) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !checkExpired {
		return len(s.entries)
	}
	now := s.cfg.clock.Now()
	n := 0
	for _, e := range s.entries {
		if !e.expired(now) {
			n++
		}
	}
	return n
}

func (s *store) HotKeys(limit int) ([]any, int) { panic("HotKeys is only supported by ARC") }
