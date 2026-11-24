package caches

import "container/list"

// keyQueue maintains MRU order with constant-time removal by key.
type keyQueue struct {
	order list.List
	index map[any]*list.Element
}

func newQueue() *keyQueue { return &keyQueue{index: make(map[any]*list.Element)} }

func (q *keyQueue) add(key any) {
	if e := q.index[key]; e != nil {
		q.order.MoveToFront(e)
		return
	}
	q.index[key] = q.order.PushFront(key)
}

func (q *keyQueue) remove(key any) bool {
	e := q.index[key]
	if e == nil {
		return false
	}
	q.order.Remove(e)
	delete(q.index, key)
	return true
}

func (q *keyQueue) pop() any {
	key := q.order.Back().Value
	q.remove(key)
	return key
}

type LRUCache struct {
	*store
	recent *keyQueue
}

func (c *LRUCache) reset() { c.recent = newQueue() }

func (c *LRUCache) admit(key any) (any, bool) {
	c.recent.add(key)
	if c.recent.order.Len() > c.cfg.size {
		return c.recent.pop(), true
	}
	return nil, false
}

func (c *LRUCache) touch(key any) { c.recent.add(key) }

func (c *LRUCache) replace(key any) { c.touch(key) }

func (c *LRUCache) forget(key any) { c.recent.remove(key) }
