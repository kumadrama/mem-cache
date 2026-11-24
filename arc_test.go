package caches

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"
)

func TestARCGet(t *testing.T) {
	size := 1000
	gc := buildTestCache(t, TYPE_ARC, size)
	testSetCache(t, gc, size)
	testGetCache(t, gc, size)
}

func TestARC_MGet(t *testing.T) {
	size := 1000
	gc := buildTestCache(t, TYPE_ARC, size*int(math.Pow10(MPowSize)))
	testMSetCache(t, gc, size)
	testMGetCache(t, gc, size)
}

func TestLoadingARCGet(t *testing.T) {
	size := 1000
	numbers := 1000

	testGetCache(t, buildTestLoadingCache(t, TYPE_ARC, size), numbers)
}

func TestLoadingARCMGet(t *testing.T) {
	size := 1000
	numbers := 1000

	testMGetCache(t, buildTestLoadingCache(t, TYPE_ARC, size*int(math.Pow10(MPowSize))), numbers)
}

func TestARCLength(t *testing.T) {
	gc := buildTestLoadingCacheWithExpiration(t, TYPE_ARC, 2, time.Millisecond)
	gc.Get("test1")
	gc.Get("test2")
	gc.Get("test3")
	length := gc.Len(true)
	expectedLength := 2
	if length != expectedLength {
		t.Errorf("Expected length is %v, not %v", expectedLength, length)
	}
	time.Sleep(time.Millisecond)
	gc.Get("test4")
	length = gc.Len(true)
	expectedLength = 1
	if length != expectedLength {
		t.Errorf("Expected length is %v, not %v", expectedLength, length)
	}
}

func TestARCEvictItem(t *testing.T) {
	cacheSize := 10
	numbers := cacheSize + 1
	gc := buildTestLoadingCache(t, TYPE_ARC, cacheSize)

	for i := 0; i < numbers; i++ {
		_, err := gc.Get(fmt.Sprintf("Key-%d", i))
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}
}

func TestARCPurgeCache(t *testing.T) {
	cacheSize := 10
	purgeCount := 0
	gc := New(cacheSize).
		ARC().
		LoaderFunc(loader).
		PurgeVisitorFunc(func(k, v interface{}) {
			purgeCount++
		}).
		Build()

	for i := 0; i < cacheSize; i++ {
		_, err := gc.Get(fmt.Sprintf("Key-%d", i))
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}

	gc.Purge()

	if purgeCount != cacheSize {
		t.Errorf("failed to purge everything")
	}
}

func TestARCGetIFPresent(t *testing.T) {
	testGetIFPresent(t, TYPE_ARC)
}

func TestARCHas(t *testing.T) {
	gc := buildTestLoadingCacheWithExpiration(t, TYPE_ARC, 2, 10*time.Millisecond)

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

func BenchmarkARC_Set(b *testing.B) {
	cacheSize := 50000
	gc := buildBenchCache(TYPE_ARC, cacheSize)
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

func BenchmarkARC_MSet(b *testing.B) {
	cacheSize := 50000
	gc := buildBenchCache(TYPE_ARC, cacheSize)
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

func BenchmarkARC_Get(b *testing.B) {
	cacheSize := 50000
	gc := buildBenchCache(TYPE_ARC, cacheSize)
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

func BenchmarkARC_MGet(b *testing.B) {
	cacheSize := 50000
	gc := buildBenchCache(TYPE_ARC, cacheSize)
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

func BenchmarkARC_MGet_WithHotKeys(b *testing.B) {
	cacheSize := 50000
	gc := buildBenchCache(TYPE_ARC, cacheSize)
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

	wg := sync.WaitGroup{}
	wg.Add(100)
	for range [100]struct{}{} {
		go func() {
			wg.Done()
			for {
				time.Sleep(5 * time.Millisecond)
				gc.HotKeys(50000)
			}
		}()
	}
	wg.Wait()

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
