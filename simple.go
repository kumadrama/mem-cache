package caches

import (
	"time"
)

// SimpleCache has no clear priority for evict cache. It depends on key-value map order.
type SimpleCache struct {
	baseCache
	items map[interface{}]*simpleItem
}

func newSimpleCache(cb *CacheBuilder) *SimpleCache {
	c := &SimpleCache{}
	buildCache(&c.baseCache, cb)

	c.init()
	c.loadGroup.cache = c
	return c
}

func (c *SimpleCache) init() {
	if c.size <= 0 {
		c.items = make(map[interface{}]*simpleItem)
	} else {
		c.items = make(map[interface{}]*simpleItem, c.size)
	}
}

// Set a new key-value pair
func (c *SimpleCache) Set(key, value interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.set(key, value)
	return err
}

// Set a set of new key-value pairs
func (c *SimpleCache) MSet(keys, values []interface{}) error {
	if len(keys) <= 0 {
		return KeysSizeError
	}
	if len(keys) != len(values) {
		return KeysAndValuesNotMatchError
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range keys {
		_, err := c.set(keys[i], values[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// Set a new key-value pair with an expiration time
func (c *SimpleCache) SetWithExpire(key, value interface{}, expiration time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, err := c.set(key, value)
	if err != nil {
		return err
	}

	t := c.clock.Now().Add(expiration)
	item.(*simpleItem).expiration = &t
	return nil
}

// Set a set of new key-value pairs with an expiration time
func (c *SimpleCache) MSetWithExpire(keys, values []interface{}, expiration time.Duration) error {
	if len(keys) <= 0 {
		return KeysSizeError
	}
	if len(keys) != len(values) {
		return KeysAndValuesNotMatchError
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.clock.Now().Add(expiration)
	for i := range keys {
		item, err := c.set(keys[i], values[i])
		if err != nil {
			return err
		}
		item.(*simpleItem).expiration = &t
	}
	return nil
}

func (c *SimpleCache) set(key, value interface{}) (interface{}, error) {
	var err error
	if c.serializeFunc != nil {
		value, err = c.serializeFunc(key, value)
		if err != nil {
			return nil, err
		}
	}

	// Check for existing item
	item, ok := c.items[key]
	if ok {
		if c.replacedFunc != nil {
			c.replacedFunc(key, item.value)
		}
		item.value = value
	} else {
		// Verify size not exceeded
		if (len(c.items) >= c.size) && c.size > 0 {
			c.evictOne()
		}
		item = &simpleItem{
			clock: c.clock,
			value: value,
		}
		c.items[key] = item
	}

	if c.expiration != nil {
		t := c.clock.Now().Add(*c.expiration)
		item.expiration = &t
	}

	if c.addedFunc != nil {
		c.addedFunc(key, value)
	}

	return item, nil
}

// Get a value from cache pool using key if it exists.
// If it dose not exists key and has LoaderFunc,
// generate a value using `LoaderFunc` method returns value.
func (c *SimpleCache) Get(key interface{}) (interface{}, error) {
	v, err := c.get(key, false)
	if err == KeyNotFoundError {
		return c.getWithLoader(key, true)
	}
	return v, err
}

func (c *SimpleCache) MGet(keys []interface{}) ([]interface{}, []int, error) {
	if len(keys) <= 0 {
		return nil, nil, KeysSizeError
	}

	values, missIds, err := c.mget(keys, false)
	if err != nil {
		return values, missIds, err
	}
	if len(missIds) == 0 || c.mLoaderExpireFunc == nil {
		return values, missIds, nil
	}

	missKeys := make([]interface{}, len(missIds))
	for i, id := range missIds {
		missKeys[i] = keys[id]
	}

	missValues, timeout, err := c.mLoaderExpireFunc(missKeys)
	if err != nil {
		return values, missIds, err
	}

	if len(missValues) != len(missKeys) {
		return values, missIds, KeysAndValuesNotMatchError
	}

	for i, id := range missIds {
		values[id] = missValues[i]
	}

	if timeout == nil {
		err := c.MSet(missKeys, missValues)
		if err != nil {
			return values, missIds, err
		}
	} else {
		err := c.MSetWithExpire(missKeys, missValues, *timeout)
		if err != nil {
			return values, missIds, err
		}
	}

	return values, missIds, nil
}

// GetIFPresent gets a value from cache pool using key if it exists.
// If it dose not exists key, returns KeyNotFoundError.
// And send a request which refresh value for specified key if cache object has LoaderFunc.
func (c *SimpleCache) GetIFPresent(key interface{}) (interface{}, error) {
	v, err := c.get(key, false)
	if err == KeyNotFoundError {
		return c.getWithLoader(key, false)
	}
	return v, err
}

func (c *SimpleCache) get(key interface{}, onLoad bool) (interface{}, error) {
	c.mu.Lock()
	v, err := c.getValue(key, onLoad)
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if c.deserializeFunc != nil {
		return c.deserializeFunc(key, v)
	}
	return v, nil
}

func (c *SimpleCache) mget(keys []interface{}, onLoad bool) ([]interface{}, []int, error) {
	values := make([]interface{}, len(keys))
	missIds := make([]int, len(keys))
	missNum := 0
	var err error

	c.mu.Lock()
	for i, key := range keys {
		values[i], err = c.getValue(key, onLoad)
		if err != nil {
			if err == KeyNotFoundError {
				missIds[missNum] = i
				missNum++
			} else {
				return values, missIds[:missNum], err
			}
		}
	}
	c.mu.Unlock()

	if c.deserializeFunc != nil {
		tmpIndex := 0
		for i := range keys {
			if tmpIndex < missNum && i == missIds[tmpIndex] {
				tmpIndex++
				continue
			}
			values[i], err = c.deserializeFunc(keys[i], values[i])
			if err != nil {
				return values, missIds[:missNum], err
			}
		}
	}
	return values, missIds[:missNum], nil
}

func (c *SimpleCache) getValue(key interface{}, onLoad bool) (interface{}, error) {
	item, ok := c.items[key]
	if ok {
		if !item.IsExpired(nil) {
			v := item.value
			if !onLoad {
				c.stats.IncrHitCount()
			}
			return v, nil
		}
		c.remove(key)
	}
	if !onLoad {
		c.stats.IncrMissCount()
	}
	return nil, KeyNotFoundError
}

func (c *SimpleCache) getWithLoader(key interface{}, isWait bool) (interface{}, error) {
	if c.loaderExpireFunc == nil {
		return nil, KeyNotFoundError
	}
	value, _, err := c.load(key, func(v interface{}, expiration *time.Duration, e error) (interface{}, error) {
		if e != nil {
			return nil, e
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		item, err := c.set(key, v)
		if err != nil {
			return nil, err
		}
		if expiration != nil {
			t := c.clock.Now().Add(*expiration)
			item.(*simpleItem).expiration = &t
		}
		return v, nil
	}, isWait)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (c *SimpleCache) evictOne() {
	var victim interface{}
	now := c.clock.Now()
	for key, item := range c.items {
		victim = key
		if item.expiration == nil || item.IsExpired(&now) {
			break
		}
	}
	c.remove(victim)
}

// Has checks if key exists in cache
func (c *SimpleCache) Has(key interface{}) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	now := c.clock.Now()
	return c.has(key, &now)
}

func (c *SimpleCache) has(key interface{}, now *time.Time) bool {
	item, ok := c.items[key]
	if !ok {
		return false
	}
	return !item.IsExpired(now)
}

// Remove removes the provided key from the cache.
func (c *SimpleCache) Remove(key interface{}) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.remove(key)
}

func (c *SimpleCache) remove(key interface{}) bool {
	item, ok := c.items[key]
	if ok {
		delete(c.items, key)
		if c.evictedFunc != nil {
			c.evictedFunc(key, item.value)
		}
		return true
	}
	return false
}

// GetALL returns all key-value pairs in the cache.
func (c *SimpleCache) GetALL(checkExpired bool) map[interface{}]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	items := make(map[interface{}]interface{}, len(c.items))
	now := c.clock.Now()
	for k, item := range c.items {
		if !checkExpired || c.has(k, &now) {
			items[k] = item.value
		}
	}
	return items
}

// Keys returns a slice of the keys in the cache.
func (c *SimpleCache) Keys(checkExpired bool) []interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]interface{}, 0, len(c.items))
	now := c.clock.Now()
	for k := range c.items {
		if !checkExpired || c.has(k, &now) {
			keys = append(keys, k)
		}
	}
	return keys
}

// Len returns the number of items in the cache.
func (c *SimpleCache) Len(checkExpired bool) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !checkExpired {
		return len(c.items)
	}
	var length int
	now := c.clock.Now()
	for k := range c.items {
		if c.has(k, &now) {
			length++
		}
	}
	return length
}

// Completely clear the cache
func (c *SimpleCache) Purge() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.purgeVisitorFunc != nil {
		for key, item := range c.items {
			c.purgeVisitorFunc(key, item.value)
		}
	}

	c.init()
}

func (c *SimpleCache) HotKeys(int) ([]interface{}, int) {
	panic("SimpleCache HotKeys func not implementation")
}

type simpleItem struct {
	clock      Clock
	value      interface{}
	expiration *time.Time
}

// IsExpired returns boolean value whether this item is expired or not.
func (si *simpleItem) IsExpired(now *time.Time) bool {
	if si.expiration == nil {
		return false
	}
	if now == nil {
		t := si.clock.Now()
		now = &t
	}
	return si.expiration.Before(*now)
}
