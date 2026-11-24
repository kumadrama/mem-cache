package caches_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/kumadrama/mem-cache"
)

func TestCacheClockAndResidentCapacity(t *testing.T) {
	for _, policy := range []string{caches.TYPE_SIMPLE, caches.TYPE_LRU, caches.TYPE_LFU, caches.TYPE_ARC} {
		t.Run(policy, func(t *testing.T) {
			clock := caches.NewFakeClock()
			c := caches.New(1).EvictType(policy).Expiration(time.Minute).Clock(clock).Build()
			for _, key := range []string{"old", "new"} {
				if err := c.Set(key, key); err != nil {
					t.Fatal(err)
				}
			}
			if !c.Has("new") || c.Len(false) != 1 || c.Len(true) != 1 || len(c.Keys(true)) != 1 || len(c.GetALL(true)) != 1 {
				t.Fatal("expected one live entry at the configured clock time")
			}
			clock.Advance(2 * time.Minute)
			if c.Has("new") || c.Len(true) != 0 || len(c.Keys(true)) != 0 || len(c.GetALL(true)) != 0 {
				t.Fatal("expired entry is still visible")
			}
		})
	}
}

func TestBatchLoaderLength(t *testing.T) {
	for _, policy := range []string{caches.TYPE_SIMPLE, caches.TYPE_LRU, caches.TYPE_LFU, caches.TYPE_ARC} {
		t.Run(policy, func(t *testing.T) {
			c := caches.New(4).EvictType(policy).MLoaderFunc(func(keys []interface{}) ([]interface{}, error) {
				return nil, nil
			}).Build()
			if err := c.Set("hit", 7); err != nil {
				t.Fatal(err)
			}
			values, misses, err := c.MGet([]interface{}{"missing", "hit"})
			if err != caches.KeysAndValuesNotMatchError || len(misses) != 1 || misses[0] != 0 || values[1] != 7 || c.Has("missing") {
				t.Fatalf("MGet = %v, %v, %v", values, misses, err)
			}
		})
	}
}

func TestSimpleGetIFPresentDecodeError(t *testing.T) {
	want := errors.New("decode failed")
	c := caches.New(1).DeserializeFunc(func(key, value interface{}) (interface{}, error) {
		return nil, want
	}).Build()
	if err := c.Set("key", "stored"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetIFPresent("key"); err != want {
		t.Fatalf("GetIFPresent returned %v, want %v", err, want)
	}
}

func TestConcurrentARCHotKeys(t *testing.T) {
	c := caches.New(8).ARC().Build()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_ = c.Set(i, i)
			c.Remove(i - 2)
		}
	}()
	for i := 0; i < 200; i++ {
		keys, total := c.HotKeys(8)
		if len(keys) != total {
			t.Errorf("HotKeys returned %d keys for %d resident entries", len(keys), total)
			break
		}
	}
	wg.Wait()
}
