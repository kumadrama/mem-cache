package caches

// SimpleCache evicts an arbitrary resident entry at capacity.
type SimpleCache struct{ *store }

func (c *SimpleCache) admit(key any) (any, bool) {
	if c.cfg.size > 0 && len(c.entries) >= c.cfg.size {
		var victim any
		now := c.cfg.clock.Now()
		for candidate, e := range c.entries {
			victim = candidate
			if e.deadline.IsZero() || e.expired(now) {
				break
			}
		}
		return victim, true
	}
	return nil, false
}

func (*SimpleCache) touch(any) {}

func (*SimpleCache) replace(any) {}

func (*SimpleCache) forget(any) {}

func (*SimpleCache) reset() {}
