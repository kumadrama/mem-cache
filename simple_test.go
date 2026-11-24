package caches

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func TestSimpleGet(t *testing.T) {
	size := 1000
	gc := buildTestCache(t, TYPE_SIMPLE, size)
	testSetCache(t, gc, size)
	testGetCache(t, gc, size)
}

func TestSimpleMGet(t *testing.T) {
	size := 1000
	gc := buildTestCache(t, TYPE_SIMPLE, size*int(math.Pow10(MPowSize)))
	testMSetCache(t, gc, size)
	testMGetCache(t, gc, size)
}

func TestLoadingSimpleGet(t *testing.T) {
	size := 1000
	numbers := 1000
	testGetCache(t, buildTestLoadingCache(t, TYPE_SIMPLE, size), numbers)
}

func TestLoadingSimpleMGet(t *testing.T) {
	size := 1000
	numbers := 1000

	testMGetCache(t, buildTestLoadingCache(t, TYPE_SIMPLE, size*int(math.Pow10(MPowSize))), numbers)
}

func TestSimpleLength(t *testing.T) {
	gc := buildTestLoadingCache(t, TYPE_SIMPLE, 1000)
	gc.Get("test1")
	gc.Get("test2")
	length := gc.Len(true)
	expectedLength := 2
	if length != expectedLength {
		t.Errorf("Expected length is %v, not %v", length, expectedLength)
	}
}

func TestSimpleEvictItem(t *testing.T) {
	cacheSize := 10
	numbers := 11
	gc := buildTestLoadingCache(t, TYPE_SIMPLE, cacheSize)

	for i := 0; i < numbers; i++ {
		_, err := gc.Get(fmt.Sprintf("Key-%d", i))
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}
}

func TestSimpleUnboundedNoEviction(t *testing.T) {
	numbers := 1000
	size_tracker := 0
	gcu := buildTestLoadingCache(t, TYPE_SIMPLE, 0)

	for i := 0; i < numbers; i++ {
		current_size := gcu.Len(true)
		if current_size != size_tracker {
			t.Errorf("Excepted cache size is %v not %v", current_size, size_tracker)
		}

		_, err := gcu.Get(fmt.Sprintf("Key-%d", i))
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		size_tracker++
	}
}

func TestSimpleGetIFPresent(t *testing.T) {
	testGetIFPresent(t, TYPE_SIMPLE)
}

func TestSimpleHas(t *testing.T) {
	gc := buildTestLoadingCacheWithExpiration(t, TYPE_SIMPLE, 2, 10*time.Millisecond)

	for i := 0; i < 10; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			gc.Get("test1")
			gc.Get("test2")

			if gc.Has("test0") {
				t.Fatal("should not have test0")
			}
			if !gc.Has("test1") {
				t.Fatal("should have test1")
			}
			if !gc.Has("test2") {
				t.Fatal("should have test2")
			}

			time.Sleep(20 * time.Millisecond)

			if gc.Has("test0") {
				t.Fatal("should not have test0")
			}
			if gc.Has("test1") {
				t.Fatal("should not have test1")
			}
			if gc.Has("test2") {
				t.Fatal("should not have test2")
			}
		})
	}
}

func BenchmarkSimpleSet(b *testing.B) {
	cacheSize := 50000
	gc := buildBenchCache(TYPE_SIMPLE, cacheSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		keysSize := 2000
		keys := make([]interface{}, keysSize)
		values := make([]interface{}, keysSize)
		randKeys := GenerateRandomNumber(0, cacheSize, keysSize)
		for j := range keys {
			keys[j] = fmt.Sprintf("Key-%d", randKeys[j])
			value, err := loader(keys[j])
			if err != nil {
				b.Error(err)
				return
			}
			values[j] = value
		}
		b.StartTimer()
		for j := range keys {
			gc.Set(keys[j], values[j])
		}
	}
}

func BenchmarkSimpleMSet(b *testing.B) {
	cacheSize := 50000
	gc := buildBenchCache(TYPE_SIMPLE, cacheSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		keysSize := 2000
		keys := make([]interface{}, keysSize)
		values := make([]interface{}, keysSize)
		randKeys := GenerateRandomNumber(0, cacheSize, keysSize)
		for j := range keys {
			keys[j] = fmt.Sprintf("Key-%d", randKeys[j])
			value, err := loader(keys[j])
			if err != nil {
				b.Error(err)
				return
			}
			values[j] = value
		}
		b.StartTimer()
		gc.MSet(keys, values)
	}
}

func BenchmarkSimple_Get(b *testing.B) {
	cacheSize := 50000
	gc := buildBenchCache(TYPE_SIMPLE, cacheSize)
	keys := make([]interface{}, cacheSize)
	values := make([]interface{}, cacheSize)
	for i := 0; i < cacheSize; i++ {
		keys[i] = fmt.Sprintf("Key-%d", i)
		value, err := loader(keys[i])
		if err != nil {
			b.Error(err)
			return
		}
		values[i] = value
	}
	gc.MSet(keys, values)

	keysSize := 2000
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		keys := make([]interface{}, keysSize)
		randKeys := GenerateRandomNumber(0, cacheSize, keysSize)
		for j := range keys {
			keys[j] = fmt.Sprintf("Key-%d", randKeys[j])
		}
		b.StartTimer()
		for j := range keys {
			gc.Get(keys[j])
		}
	}
}

func BenchmarkSimple_MGet(b *testing.B) {
	cacheSize := 50000
	gc := buildBenchCache(TYPE_SIMPLE, cacheSize)
	keys := make([]interface{}, cacheSize)
	values := make([]interface{}, cacheSize)
	for i := 0; i < cacheSize; i++ {
		keys[i] = fmt.Sprintf("Key-%d", i)
		value, err := loader(keys[i])
		if err != nil {
			b.Error(err)
			return
		}
		values[i] = value
	}
	gc.MSet(keys, values)

	keysSize := 2000
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		keys := make([]interface{}, keysSize)
		randKeys := GenerateRandomNumber(0, cacheSize, keysSize)
		for j := range keys {
			keys[j] = fmt.Sprintf("Key-%d", randKeys[j])
		}
		b.StartTimer()
		gc.MGet(keys)
		b.StopTimer()
	}
}
