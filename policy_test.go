package caches

import "testing"

// Access A twice, then write B more recently. The policies should distinguish
// recency (LRU) from frequency and repeated access (LFU and ARC).
func TestEvictionByAccessHistory(t *testing.T) {
	for _, tc := range []struct{ policy, victim string }{
		{TYPE_LRU, "a"}, {TYPE_LFU, "b"}, {TYPE_ARC, "b"},
	} {
		t.Run(tc.policy, func(t *testing.T) {
			c := New(2).EvictType(tc.policy).Build()
			for _, k := range []string{"a", "b"} {
				if err := c.Set(k, k); err != nil {
					t.Fatal(err)
				}
			}
			for _, k := range []string{"a", "a"} {
				if _, err := c.Get(k); err != nil {
					t.Fatal(err)
				}
			}
			// Replacing B updates its recency in LRU, and preserves its
			// previous frequency / recent-list membership in LFU and ARC.
			if err := c.Set("b", "new-b"); err != nil {
				t.Fatal(err)
			}
			if err := c.Set("c", "c"); err != nil {
				t.Fatal(err)
			}
			if c.Has(tc.victim) || !c.Has("c") || c.Len(false) != 2 {
				t.Fatalf("unexpected resident keys %v", c.Keys(false))
			}
		})
	}
}

func TestARCGhostHistory(t *testing.T) {
	c := New(2).ARC().Build().(*ARC)
	for _, k := range []string{"a", "b"} {
		if err := c.Set(k, k); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.Get("a"); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"c", "b", "a"} {
		if err := c.Set(k, k); err != nil {
			t.Fatal(err)
		}
	}
	// B returns from recent history and evicts A. A returns from
	// frequent history, giving frequent entries more space.
	if !c.Has("a") || !c.Has("b") || c.Has("c") {
		t.Fatalf("unexpected resident keys %v", c.Keys(false))
	}
}
