package caches

// ARC balances recent arrivals against repeatedly used entries. The two
// history queues retain keys only and guide the target size of recent.
type ARC struct {
	*store
	recent, frequent, recentHistory, frequentHistory *keyQueue
	target                                           int
}

func (c *ARC) reset() {
	c.recent, c.frequent = newQueue(), newQueue()
	c.recentHistory, c.frequentHistory = newQueue(), newQueue()
	c.target = 0
}

func (c *ARC) touch(key any) {
	c.recent.remove(key)
	c.frequent.add(key)
}

func (*ARC) replace(any) {}

func (c *ARC) forget(key any) {
	if c.recent.remove(key) {
		c.recentHistory.add(key)
	} else if c.frequent.remove(key) {
		c.frequentHistory.add(key)
	}
}

func (c *ARC) evict(fromFrequentHistory bool) any {
	n := c.recent.order.Len()
	if n > 0 && (n > c.target || (n == c.target && fromFrequentHistory) || c.frequent.order.Len() == 0) {
		victim := c.recent.pop()
		c.recentHistory.add(victim)
		return victim
	}
	victim := c.frequent.pop()
	c.frequentHistory.add(victim)
	return victim
}

func (c *ARC) admit(key any) (victim any, full bool) {
	capacity := c.cfg.size
	full = len(c.entries) == capacity
	b1, b2 := c.recentHistory.order.Len(), c.frequentHistory.order.Len()
	inB1, inB2 := c.recentHistory.index[key] != nil, c.frequentHistory.index[key] != nil
	if inB1 || inB2 {
		if full {
			if inB1 {
				c.target = min(capacity, c.target+max(1, b2/b1))
			} else {
				c.target = max(0, c.target-max(1, b1/b2))
			}
			victim = c.evict(inB2)
		}
		c.recentHistory.remove(key)
		c.frequentHistory.remove(key)
		c.frequent.add(key)
		return
	}
	n := c.recent.order.Len()
	if full && n+b1 == capacity {
		if n == capacity {
			victim = c.recent.pop()
		} else {
			c.recentHistory.pop()
			victim = c.evict(false)
		}
	} else {
		if b1+b2-capacity >= capacity-len(c.entries) {
			if b2 > 0 {
				c.frequentHistory.pop()
			} else {
				c.recentHistory.pop()
			}
		}
		if full {
			victim = c.evict(false)
		}
	}
	c.recent.add(key)
	return
}

func (c *ARC) HotKeys(limit int) ([]any, int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	total := len(c.entries)
	limit = min(max(0, limit), total)
	keys := make([]any, 0, limit)
	for _, queue := range []*keyQueue{c.recent, c.frequent} {
		for e := queue.order.Front(); e != nil && len(keys) < limit; e = e.Next() {
			keys = append(keys, e.Value)
		}
	}
	return keys, total
}
