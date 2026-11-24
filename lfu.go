package caches

import "container/list"

type freqEntry struct {
	freq uint
	keys map[any]struct{}
}

type LFUCache struct {
	*store
	freqList list.List
	location map[any]*list.Element
}

func (c *LFUCache) reset() {
	c.freqList.Init()
	c.freqList.PushFront(&freqEntry{keys: make(map[any]struct{})})
	c.location = make(map[any]*list.Element)
}

func (c *LFUCache) put(key any, bucket *list.Element) {
	bucket.Value.(*freqEntry).keys[key] = struct{}{}
	c.location[key] = bucket
}

func (c *LFUCache) forget(key any) {
	bucket := c.location[key]
	b := bucket.Value.(*freqEntry)
	delete(b.keys, key)
	delete(c.location, key)
	if b.freq != 0 && len(b.keys) == 0 {
		c.freqList.Remove(bucket)
	}
}

func (c *LFUCache) touch(key any) {
	current := c.location[key]
	freq := current.Value.(*freqEntry).freq + 1
	next := current.Next()
	if next == nil || next.Value.(*freqEntry).freq != freq {
		bucket := current.Value.(*freqEntry)
		if bucket.freq != 0 && len(bucket.keys) == 1 {
			bucket.freq = freq
			return
		}
		next = c.freqList.InsertAfter(&freqEntry{freq: freq, keys: make(map[any]struct{})}, current)
	}
	c.forget(key)
	c.put(key, next)
}

func (*LFUCache) replace(any) {}

func (c *LFUCache) admit(key any) (victim any, full bool) {
	zero := c.freqList.Front()
	if len(c.location) == c.cfg.size {
		bucket := zero
		if len(bucket.Value.(*freqEntry).keys) == 0 {
			bucket = bucket.Next()
		}
		for victim = range bucket.Value.(*freqEntry).keys {
			break
		}
		c.forget(victim)
		full = true
	}
	c.put(key, zero)
	return
}
