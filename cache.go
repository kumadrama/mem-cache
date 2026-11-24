package caches

import (
	"context"
	"errors"
	"strings"
	"time"
)

const (
	TYPE_SIMPLE = "simple"
	TYPE_LRU    = "lru"
	TYPE_LFU    = "lfu"
	TYPE_ARC    = "arc"
)

var KeyNotFoundError = errors.New("key not found")

var KeysAndValuesNotMatchError = errors.New("keys not match with values")

var KeysSizeError = errors.New("keys should have at least one")

type Cache interface {
	Set(key, value interface{}) error
	MSet(keys, values []interface{}) error
	SetWithExpire(key, value interface{}, expiration time.Duration) error
	MSetWithExpire(keys, values []interface{}, expiration time.Duration) error
	Get(key interface{}) (interface{}, error)
	MGet(keys []interface{}) ([]interface{}, []int, error)
	GetIFPresent(key interface{}) (interface{}, error)
	GetALL(checkExpired bool) map[interface{}]interface{}
	GetContext(context.Context, any) (any, error)
	MGetContext(context.Context, []any) ([]any, []int, error)
	GetIFPresentContext(context.Context, any) (any, error)
	Remove(key interface{}) bool
	Purge()
	Keys(checkExpired bool) []interface{}
	Len(checkExpired bool) int
	Has(key interface{}) bool
	HotKeys(limit int) (keys []interface{}, total int)

	statsAccessor
}

type (
	LoaderFunc        func(interface{}) (interface{}, error)
	MLoaderFunc       func([]interface{}) ([]interface{}, error)
	LoaderExpireFunc  func(interface{}) (interface{}, *time.Duration, error)
	MLoaderExpireFunc func([]interface{}) ([]interface{}, *time.Duration, error)
	EvictedFunc       func(interface{}, interface{})
	ReplacedFunc      func(interface{}, interface{})
	PurgeVisitorFunc  func(interface{}, interface{})
	AddedFunc         func(interface{}, interface{})
	DeserializeFunc   func(interface{}, interface{}) (interface{}, error)
	SerializeFunc     func(interface{}, interface{}) (interface{}, error)
)

type CacheBuilder struct {
	clock            Clock
	name, tp         string
	size             int
	load             func(context.Context, any) (any, *time.Duration, error)
	loadBatch        func(context.Context, []any) ([]any, *time.Duration, error)
	evictedFunc      EvictedFunc
	replacedFunc     ReplacedFunc
	purgeVisitorFunc PurgeVisitorFunc
	addedFunc        AddedFunc
	expiration       *time.Duration
	deserializeFunc  DeserializeFunc
	serializeFunc    SerializeFunc
}

func New(size int) *CacheBuilder {
	return &CacheBuilder{
		clock: NewRealClock(),
		tp:    TYPE_SIMPLE,
		name:  "default",
		size:  size,
	}
}

func (cb *CacheBuilder) Clock(clock Clock) *CacheBuilder {
	cb.clock = clock
	return cb
}

// Name identifies this cache in Doku metrics and logs. Use a stable business name.
func (cb *CacheBuilder) Name(name string) *CacheBuilder {
	if strings.TrimSpace(name) != "" {
		cb.name = strings.TrimSpace(name)
	}
	return cb
}

func (cb *CacheBuilder) LoaderFunc(fn LoaderFunc) *CacheBuilder {
	return cb.LoaderExpireFunc(func(k any) (any, *time.Duration, error) {
		v, err := fn(k)
		return v, nil, err
	})
}

func (cb *CacheBuilder) MLoaderFunc(fn MLoaderFunc) *CacheBuilder {
	return cb.MLoaderExpireFunc(func(keys []any) ([]any, *time.Duration, error) {
		values, err := fn(keys)
		return values, nil, err
	})
}

func (cb *CacheBuilder) LoaderExpireFunc(fn LoaderExpireFunc) *CacheBuilder {
	cb.load = func(_ context.Context, k any) (any, *time.Duration, error) { return fn(k) }
	return cb
}

func (cb *CacheBuilder) MLoaderExpireFunc(fn MLoaderExpireFunc) *CacheBuilder {
	cb.loadBatch = func(_ context.Context, keys []any) ([]any, *time.Duration, error) { return fn(keys) }
	return cb
}

// LoaderContextFunc receives the caller context and may override the configured TTL.
func (cb *CacheBuilder) LoaderContextFunc(fn func(context.Context, any) (any, *time.Duration, error)) *CacheBuilder {
	cb.load = fn
	return cb
}

func (cb *CacheBuilder) MLoaderContextFunc(fn func(context.Context, []any) ([]any, *time.Duration, error)) *CacheBuilder {
	cb.loadBatch = fn
	return cb
}

func (cb *CacheBuilder) EvictType(tp string) *CacheBuilder {
	cb.tp = tp
	return cb
}

func (cb *CacheBuilder) Simple() *CacheBuilder {
	return cb.EvictType(TYPE_SIMPLE)
}

func (cb *CacheBuilder) LRU() *CacheBuilder {
	return cb.EvictType(TYPE_LRU)
}

func (cb *CacheBuilder) LFU() *CacheBuilder {
	return cb.EvictType(TYPE_LFU)
}

func (cb *CacheBuilder) ARC() *CacheBuilder {
	return cb.EvictType(TYPE_ARC)
}

func (cb *CacheBuilder) EvictedFunc(evictedFunc EvictedFunc) *CacheBuilder {
	cb.evictedFunc = evictedFunc
	return cb
}

func (cb *CacheBuilder) ReplacedFunc(replacedFunc ReplacedFunc) *CacheBuilder {
	cb.replacedFunc = replacedFunc
	return cb
}

func (cb *CacheBuilder) PurgeVisitorFunc(purgeVisitorFunc PurgeVisitorFunc) *CacheBuilder {
	cb.purgeVisitorFunc = purgeVisitorFunc
	return cb
}

func (cb *CacheBuilder) AddedFunc(addedFunc AddedFunc) *CacheBuilder {
	cb.addedFunc = addedFunc
	return cb
}

func (cb *CacheBuilder) DeserializeFunc(deserializeFunc DeserializeFunc) *CacheBuilder {
	cb.deserializeFunc = deserializeFunc
	return cb
}

func (cb *CacheBuilder) SerializeFunc(serializeFunc SerializeFunc) *CacheBuilder {
	cb.serializeFunc = serializeFunc
	return cb
}

func (cb *CacheBuilder) Expiration(expiration time.Duration) *CacheBuilder {
	cb.expiration = &expiration
	return cb
}

func (cb *CacheBuilder) Build() Cache {
	if cb.size <= 0 && cb.tp != TYPE_SIMPLE {
		panic("cache: Cache size <= 0")
	}
	s := &store{cfg: *cb, entries: make(map[any]entry), signals: newSignals(cb.name, cb.tp)}
	switch cb.tp {
	case TYPE_SIMPLE:
		c := &SimpleCache{store: s}
		s.policy = c
		return c
	case TYPE_LRU:
		c := &LRUCache{store: s}
		c.reset()
		s.policy = c
		return c
	case TYPE_LFU:
		c := &LFUCache{store: s}
		c.reset()
		s.policy = c
		return c
	case TYPE_ARC:
		c := &ARC{store: s}
		c.reset()
		s.policy = c
		return c
	default:
		panic("cache: Unknown type " + cb.tp)
	}
}
