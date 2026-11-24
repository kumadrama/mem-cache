package caches

import (
	"container/list"
	"time"
)

// Discards the least recently used items first.
type LRUCache struct {
	baseCache
	items     map[interface{}]*list.Element
	evictList *list.List
}

func newLRUCache(cb *CacheBuilder) *LRUCache {
	c := &LRUCache{}
	buildCache(&c.baseCache, cb)

	c.init()
	c.loadGroup.cache = c
	return c
}

func (c *LRUCache) init() {
	c.evictList = list.New()
	c.items = make(map[interface{}]*list.Element, c.size+1)
}

func (c *LRUCache) set(key, value interface{}) (interface{}, error) {
	var err error
	if c.serializeFunc != nil {
		value, err = c.serializeFunc(key, value)
		if err != nil {
			return nil, err
		}
	}

	// Check for existing item
	var item *lruItem
	if it, ok := c.items[key]; ok {
		c.evictList.MoveToFront(it)
		item = it.Value.(*lruItem)
		if c.replacedFunc != nil {
			c.replacedFunc(key, item.value)
		}
		item.value = value
	} else {
		// Verify size not exceeded
		if c.evictList.Len() >= c.size {
			c.evict(1)
		}
		item = &lruItem{
			clock: c.clock,
			key:   key,
			value: value,
		}
		c.items[key] = c.evictList.PushFront(item)
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

// set a new key-value pair
func (c *LRUCache) Set(key, value interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.set(key, value)
	return err
}

// Set a set of new key-value pairs
func (c *LRUCache) MSet(keys, values []interface{}) error {
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
func (c *LRUCache) SetWithExpire(key, value interface{}, expiration time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, err := c.set(key, value)
	if err != nil {
		return err
	}

	t := c.clock.Now().Add(expiration)
	item.(*lruItem).expiration = &t
	return nil
}

// Set a set of new key-value pairs with an expiration time
func (c *LRUCache) MSetWithExpire(keys, values []interface{}, expiration time.Duration) error {
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
		item.(*lruItem).expiration = &t
	}
	return nil
}

// Get a value from cache pool using key if it exists.
// If it dose not exists key and has LoaderFunc,
// generate a value using `LoaderFunc` method returns value.
func (c *LRUCache) Get(key interface{}) (interface{}, error) {
	v, err := c.get(key, false)
	if err == KeyNotFoundError {
		return c.getWithLoader(key, true)
	}
	return v, err
}

func (c *LRUCache) MGet(keys []interface{}) ([]interface{}, []int, error) {
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
func (c *LRUCache) GetIFPresent(key interface{}) (interface{}, error) {
	v, err := c.get(key, false)
	if err == KeyNotFoundError {
		return c.getWithLoader(key, false)
	}
	return v, err
}

func (c *LRUCache) get(key interface{}, onLoad bool) (interface{}, error) {
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

func (c *LRUCache) mget(keys []interface{}, onLoad bool) ([]interface{}, []int, error) {
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

func (c *LRUCache) getValue(key interface{}, onLoad bool) (interface{}, error) {
	item, ok := c.items[key]
	if ok {
		it := item.Value.(*lruItem)
		if !it.IsExpired(nil) {
			c.evictList.MoveToFront(item)
			v := it.value
			if !onLoad {
				c.stats.IncrHitCount()
			}
			return v, nil
		}
		c.removeElement(item)
	}
	if !onLoad {
		c.stats.IncrMissCount()
	}
	return nil, KeyNotFoundError
}

func (c *LRUCache) getWithLoader(key interface{}, isWait bool) (interface{}, error) {
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
			item.(*lruItem).expiration = &t
		}
		return v, nil
	}, isWait)
	if err != nil {
		return nil, err
	}
	return value, nil
}

// evict removes the oldest item from the cache.
func (c *LRUCache) evict(count int) {
	for i := 0; i < count; i++ {
		ent := c.evictList.Back()
		if ent == nil {
			return
		} else {
			c.removeElement(ent)
		}
	}
}

// Has checks if key exists in cache
func (c *LRUCache) Has(key interface{}) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	now := c.clock.Now()
	return c.has(key, &now)
}

func (c *LRUCache) has(key interface{}, now *time.Time) bool {
	item, ok := c.items[key]
	if !ok {
		return false
	}
	return !item.Value.(*lruItem).IsExpired(now)
}

// Remove removes the provided key from the cache.
func (c *LRUCache) Remove(key interface{}) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.remove(key)
}

func (c *LRUCache) remove(key interface{}) bool {
	if ent, ok := c.items[key]; ok {
		c.removeElement(ent)
		return true
	}
	return false
}

func (c *LRUCache) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	entry := e.Value.(*lruItem)
	delete(c.items, entry.key)
	if c.evictedFunc != nil {
		entry := e.Value.(*lruItem)
		c.evictedFunc(entry.key, entry.value)
	}
}

// GetALL returns all key-value pairs in the cache.
func (c *LRUCache) GetALL(checkExpired bool) map[interface{}]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	items := make(map[interface{}]interface{}, len(c.items))
	now := c.clock.Now()
	for k, item := range c.items {
		if !checkExpired || c.has(k, &now) {
			items[k] = item.Value.(*lruItem).value
		}
	}
	return items
}

// Keys returns a slice of the keys in the cache.
func (c *LRUCache) Keys(checkExpired bool) []interface{} {
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
func (c *LRUCache) Len(checkExpired bool) int {
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
func (c *LRUCache) Purge() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.purgeVisitorFunc != nil {
		for key, item := range c.items {
			it := item.Value.(*lruItem)
			v := it.value
			c.purgeVisitorFunc(key, v)
		}
	}

	c.init()
}

func (c *LRUCache) HotKeys(int) ([]interface{}, int) {
	panic("LRUCache HotKeys func not implementation")
}

type lruItem struct {
	clock      Clock
	key        interface{}
	value      interface{}
	expiration *time.Time
}

// IsExpired returns boolean value whether this item is expired or not.
func (it *lruItem) IsExpired(now *time.Time) bool {
	if it.expiration == nil {
		return false
	}
	if now == nil {
		t := it.clock.Now()
		now = &t
	}
	return it.expiration.Before(*now)
}
