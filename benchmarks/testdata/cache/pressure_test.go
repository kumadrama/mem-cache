package caches

import (
	"context"
	"encoding/json"
	"fmt"
	"math/bits"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/metric/noop"
)

const value = "resident-value"

type workloadCache interface {
	Get(any) (any, error)
	Set(any, any) error
	MGet([]any) ([]any, []int, error)
	Len(bool) int
}

type scenario struct {
	Mode, Policy, Work string
	Capacity, Workers  int
}

var originalCache func(scenario) workloadCache

func (s scenario) build() workloadCache {
	if s.Mode == "original" {
		return originalCache(s)
	}
	b := New(s.Capacity).EvictType(s.Policy).Name("pressure").Expiration(time.Hour)
	if s.Work == "partial32" {
		b.MLoaderFunc(func(keys []any) ([]any, error) {
			out := make([]any, len(keys))
			for i := range out {
				out[i] = value
			}
			return out, nil
		})
	}
	c := b.Build()
	if s.Mode == "noop" {
		instr := cacheInstruments(noop.NewMeterProvider().Meter("pressure"))
		switch p := c.(type) {
		case *SimpleCache:
			p.signals.instruments = instr
		case *LRUCache:
			p.signals.instruments = instr
		case *LFUCache:
			p.signals.instruments = instr
		case *ARC:
			p.signals.instruments = instr
		}
	}
	return c
}

// Each bucket spans at most 1/32 of a power of two. Return upper bounds in ns.
type histogram [2048]uint64

func (h *histogram) add(ns uint64) {
	if ns == 0 {
		ns = 1
	}
	exp := bits.Len64(ns) - 1
	base := uint64(1) << exp
	h[exp*32+int((ns-base)*32/base)]++
}
func (h *histogram) percentile(n, percent uint64) uint64 {
	target := (n*percent + 99) / 100
	var seen uint64
	for i, count := range h {
		seen += count
		if seen >= target {
			base := uint64(1) << (i / 32)
			return base + (uint64(i%32+1)*base+31)/32 - 1
		}
	}
	return 0
}

type totals struct {
	lat                            histogram
	ops, samples, hits, reads, bad uint64
}

func emit(v any) {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	fmt.Println("ROW", string(b))
}

type measurement struct {
	scenario
	Round                                            int
	Ops, LatencySamples, ReadKeys, HitKeys, Errors   uint64
	Seconds, OpsPerSec, AllocBytesPerOp, AllocsPerOp float64
	P50ns, P95ns, P99ns                              uint64
	Size                                             int
	GCs                                              uint32
}

func measure(t *testing.T, s scenario, d time.Duration, round int) {
	t.Helper()
	c := s.build()
	keys := make([]any, s.Capacity)
	for i := range keys {
		keys[i] = i
		if err := c.Set(keys[i], value); err != nil {
			t.Fatal(err)
		}
	}
	// Prime the frequently reused keys for the hot/cold workloads. Uniform reads
	// and admission workloads start at full capacity with no extra lookups.
	if s.Work == "churn" || s.Work == "partial32" {
		for i := 0; i < 128; i++ {
			_, _ = c.Get(keys[i])
		}
	}
	var ready, done sync.WaitGroup
	start := make(chan struct{})
	var deadline time.Time
	res := make([]totals, s.Workers)
	for id := range res {
		ready.Add(1)
		done.Add(1)
		go func(id int) {
			defer done.Done()
			r := rand.New(rand.NewSource(int64(9731 + id*607)))
			var batch [32]any
			out := &res[id]
			badRead := func(v any, e error) {
				out.reads++
				if e == nil && v == value {
					out.hits++
				} else {
					out.bad++
				}
			}
			seq := int64(s.Capacity + id)
			newKey := func() any { k := seq; seq += int64(s.Workers); return k }
			ready.Done()
			<-start
			for time.Now().Before(deadline) {
				// Sample every call for slow admissions. Sample one in 16 for the
				// other paths, with the phase offset per worker. Time the API only.
				sample := (out.ops+uint64(id))%16 == 0 || s.Work == "admit"
				var begin time.Time
				switch s.Work {
				case "hit", "mix":
					key := keys[r.Intn(len(keys))]
					write := s.Work == "mix" && r.Intn(10) == 0
					if sample {
						begin = time.Now()
					}
					if write {
						if c.Set(key, value) != nil {
							out.bad++
						}
					} else {
						v, e := c.Get(key)
						badRead(v, e)
					}
				case "churn":
					write := r.Intn(100) < 5
					var k any
					if write {
						k = newKey()
					} else {
						k = keys[r.Intn(128)]
					}
					if sample {
						begin = time.Now()
					}
					if write {
						if c.Set(k, value) != nil {
							out.bad++
						}
					} else {
						v, e := c.Get(k)
						out.reads++
						if e == nil && v == value {
							out.hits++
						} else if e != KeyNotFoundError {
							out.bad++
						}
					}
				case "admit":
					k := newKey()
					if sample {
						begin = time.Now()
					}
					if c.Set(k, value) != nil {
						out.bad++
					}
				case "batch32", "partial32":
					for j := range batch {
						if s.Work == "partial32" {
							if j >= 24 {
								batch[j] = newKey()
							} else {
								batch[j] = keys[r.Intn(128)]
							}
						} else {
							batch[j] = keys[r.Intn(len(keys))]
						}
					}
					if sample {
						begin = time.Now()
					}
					values, miss, e := c.MGet(batch[:])
					if sample {
						out.lat.add(uint64(time.Since(begin)))
						out.samples++
					}
					if e != nil || len(values) != len(batch) {
						out.bad++
					} else {
						for _, v := range values {
							if v != value {
								out.bad++
								break
							}
						}
					}
					if s.Work == "batch32" && len(miss) != 0 {
						out.bad++
					}
					if s.Work == "partial32" && len(miss) < 8 {
						out.bad++
					}
					out.hits += uint64(len(batch) - len(miss))
					out.reads += uint64(len(batch))
					out.ops++
					continue
				default:
					panic(s.Work)
				}
				if sample {
					out.lat.add(uint64(time.Since(begin)))
					out.samples++
				}
				out.ops++
			}
		}(id)
	}
	ready.Wait()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	begin := time.Now()
	deadline = begin.Add(d)
	close(start)
	done.Wait()
	elapsed := time.Since(begin)
	runtime.ReadMemStats(&after)
	var all totals
	for _, x := range res {
		all.ops += x.ops
		all.samples += x.samples
		all.hits += x.hits
		all.reads += x.reads
		all.bad += x.bad
		for i, n := range x.lat {
			all.lat[i] += n
		}
	}
	size := c.Len(false)
	if all.ops == 0 || all.samples == 0 || all.bad != 0 || size != s.Capacity {
		t.Fatalf("%+v: ops=%d samples=%d errors=%d size=%d", s, all.ops, all.samples, all.bad, size)
	}
	emit(measurement{s, round, all.ops, all.samples, all.reads, all.hits, all.bad, elapsed.Seconds(), float64(all.ops) / elapsed.Seconds(), float64(after.TotalAlloc-before.TotalAlloc) / float64(all.ops), float64(after.Mallocs-before.Mallocs) / float64(all.ops), all.lat.percentile(all.samples, 50), all.lat.percentile(all.samples, 95), all.lat.percentile(all.samples, 99), size, after.NumGC - before.NumGC})
}

func TestPressureMatrix(t *testing.T) {
	var jobs []scenario
	add := func(mode, p, w string, n, g int) { jobs = append(jobs, scenario{mode, p, w, n, g}) }
	for _, p := range []string{"simple", "lru", "lfu", "arc"} {
		for _, g := range []int{1, 16, 64} {
			for _, w := range []string{"hit", "mix", "batch32"} {
				add("doku", p, w, 10000, g)
			}
		}
		for _, n := range []int{1000, 10000, 100000} {
			add("doku", p, "admit", n, 1)
		}
	}
	modes := []string{"noop"}
	if originalCache != nil {
		modes = append(modes, "original")
	}
	for _, mode := range modes {
		for _, g := range []int{1, 16, 64} {
			for _, w := range []string{"hit", "mix", "batch32"} {
				add(mode, "arc", w, 10000, g)
			}
		}
		add(mode, "arc", "admit", 10000, 1)
	}
	for _, g := range []int{1, 16, 64} {
		add("doku", "arc", "partial32", 10000, g)
	}
	for round := 1; round <= 3; round++ {
		r := rand.New(rand.NewSource(int64(751 + round)))
		r.Shuffle(len(jobs), func(i, j int) { jobs[i], jobs[j] = jobs[j], jobs[i] })
		for _, s := range jobs {
			measure(t, s, 500*time.Millisecond, round)
		}
	}
}

func TestPressureSoak(t *testing.T) {
	measure(t, scenario{"doku", "arc", "churn", 100000, 64}, 60*time.Second, 1)
}
func TestPressureProfile(t *testing.T) {
	measure(t, scenario{"doku", "arc", "hit", 10000, 64}, 3*time.Second, 1)
}

// Reuse the same instance and key across expiry waves. Wait for every caller
// to miss before releasing the loader, so a fast loader cannot hide duplicate work.
func TestPressureFanout(t *testing.T) {
	const workers = 256
	const waves = 32
	for _, p := range []string{"simple", "lru", "lfu", "arc"} {
		fc := NewFakeClock()
		var calls atomic.Int64
		var gate chan struct{}
		c := New(16).EvictType(p).Clock(fc).Expiration(time.Second).LoaderFunc(func(any) (any, error) {
			calls.Add(1)
			<-gate
			return value, nil
		}).Build()
		for wave := 1; wave <= waves; wave++ {
			fc.Advance(2 * time.Second)
			gate = make(chan struct{})
			var wg sync.WaitGroup
			var bad atomic.Int64
			for i := 0; i < workers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					v, e := c.Get("shared")
					if e != nil || v != value {
						bad.Add(1)
					}
				}()
			}
			limit := time.Now().Add(5 * time.Second)
			for c.MissCount() < uint64(wave*workers) && time.Now().Before(limit) {
				runtime.Gosched()
			}
			close(gate)
			wg.Wait()
			if bad.Load() != 0 || calls.Load() != int64(wave) || c.MissCount() != uint64(wave*workers) {
				t.Fatalf("%s wave %d calls=%d misses=%d bad=%d", p, wave, calls.Load(), c.MissCount(), bad.Load())
			}
		}
		emit(map[string]any{"Test": "fanout", "Policy": p, "Requests": workers * waves, "Loads": calls.Load(), "Waves": waves, "Workers": workers})
	}
	const batches = 64
	const width = 32
	entered := make(chan struct{}, batches)
	release := make(chan struct{})
	keys := make([]any, width)
	for i := range keys {
		keys[i] = i
	}
	c := New(128).ARC().MLoaderFunc(func(ks []any) ([]any, error) { entered <- struct{}{}; <-release; return ks, nil }).Build()
	var wg sync.WaitGroup
	for i := 0; i < batches; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			vs, ms, e := c.MGet(keys)
			if e != nil || len(ms) != width || len(vs) != width {
				t.Errorf("batch: %d/%d/%v", len(vs), len(ms), e)
				return
			}
			for j := range vs {
				if vs[j] != keys[j] || ms[j] != j {
					t.Errorf("batch order")
					return
				}
			}
		}()
	}
	wait := time.NewTimer(5 * time.Second)
	defer wait.Stop()
	for i := 0; i < batches; i++ {
		select {
		case <-entered:
		case <-wait.C:
			close(release)
			wg.Wait()
			t.Fatal("batch fanout timed out")
		}
	}
	close(release)
	wg.Wait()
	emit(map[string]any{"Test": "overlap", "Requests": batches, "Loads": batches, "UniqueKeys": width, "LoadedKeys": batches * width})
}

func TestPressureMemory(t *testing.T) {
	for _, p := range []string{"simple", "lru", "lfu", "arc"} {
		for round := 1; round <= 3; round++ {
			const n = 100000
			c := New(n).EvictType(p).Name("heap").Expiration(time.Hour).Build()
			// Prime metrics before measuring the incremental heap.
			_ = c.Set(-1, value)
			c.Purge()
			snap := func() int64 {
				runtime.GC()
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				return int64(m.HeapAlloc)
			}
			base := snap()
			for i := 0; i < n; i++ {
				buf := new([256]byte)
				buf[0] = byte(i)
				if err := c.Set(i, buf); err != nil {
					t.Fatal(err)
				}
			}
			full := snap()
			if c.Len(false) != n {
				t.Fatal("capacity")
			}
			c.Purge()
			empty := snap()
			runtime.KeepAlive(c)
			emit(map[string]any{"Test": "heap", "Policy": p, "Round": round, "Entries": n, "Payload": 256, "FullBytes": full - base, "PurgedBytes": empty - base})
		}
	}
}

// A slow loader exposes caller cancellation while a shared fill remains in flight.
func TestPressureCancel(t *testing.T) {
	const n = 128
	start := make(chan struct{})
	release := make(chan struct{})
	var loads atomic.Int64
	c := New(4).LRU().LoaderContextFunc(func(context.Context, any) (any, *time.Duration, error) {
		loads.Add(1)
		close(start)
		<-release
		return value, nil, nil
	}).Build()
	owner := make(chan error, 1)
	go func() { _, e := c.Get("k"); owner <- e }()
	<-start
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, e := c.GetContext(ctx, "k"); e != context.Canceled {
				t.Errorf("follower %v", e)
			}
		}()
	}
	limit := time.Now().Add(5 * time.Second)
	for c.MissCount() < n+1 && time.Now().Before(limit) {
		runtime.Gosched()
	}
	cancel()
	wg.Wait()
	close(release)
	if e := <-owner; e != nil || c.MissCount() != n+1 || loads.Load() != 1 {
		t.Fatal(e, c.MissCount(), loads.Load())
	}
	v, e := c.Get("k")
	if e != nil || v != value {
		t.Fatal(v, e)
	}
	emit(map[string]any{"Test": "cancel", "Followers": n, "Loads": loads.Load()})
}

func TestPressureEnv(t *testing.T) {
	fmt.Printf("ENV %s %s %s GOMAXPROCS=%d\n", runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.GOMAXPROCS(0))
}

// Validate resident/policy agreement at quiescent checkpoints after interleaving
// expiry, removal, admissions, snapshots and purges. Every stored value equals its key.
func TestPressureState(t *testing.T) {
	const capacity, workers, epochs, steps = 256, 16, 12, 512
	for _, policy := range []string{"simple", "lru", "lfu", "arc"} {
		clock := NewFakeClock()
		c := New(capacity).EvictType(policy).Clock(clock).Expiration(20 * time.Millisecond).Build()
		for epoch := 0; epoch < epochs; epoch++ {
			var wg sync.WaitGroup
			var bad atomic.Int64
			for id := 0; id < workers; id++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					r := rand.New(rand.NewSource(int64(1 + workers*epoch + id)))
					for i := 0; i < steps; i++ {
						k := r.Intn(2 * capacity)
						switch x := r.Intn(100); {
						case x < 32:
							if e := c.Set(k, k); e != nil {
								bad.Add(1)
							}
						case x < 47:
							v, e := c.Get(k)
							if e != KeyNotFoundError && (e != nil || v != k) {
								bad.Add(1)
							}
						case x < 61:
							c.Remove(k)
						case x < 70:
							keys := []any{k, (k + 1) % (2 * capacity), k}
							if e := c.MSetWithExpire(keys, keys, time.Millisecond); e != nil {
								bad.Add(1)
							}
						case x < 85:
							keys := []any{k, (k + 1) % (2 * capacity), k}
							vals, off, e := c.MGet(keys)
							if e != nil || len(vals) != len(keys) {
								bad.Add(1)
								continue
							}
							j := 0
							for ix, v := range vals {
								if j < len(off) && off[j] == ix {
									if v != nil {
										bad.Add(1)
									}
									j++
								} else if v != keys[ix] {
									bad.Add(1)
								}
							}
							if j != len(off) {
								bad.Add(1)
							}
						case x < 93:
							clock.Advance(time.Microsecond)
						case x < 97:
							for k, v := range c.GetALL(true) {
								if v != k {
									bad.Add(1)
									break
								}
							}
						case x < 99:
							if c.Len(true) > capacity {
								bad.Add(1)
							}
						default:
							if i%31 == 0 {
								c.Purge()
							} else {
								_ = c.Has(k)
							}
						}
					}
				}(id)
			}
			wg.Wait()
			live := c.GetALL(false)
			if bad.Load() != 0 || len(live) != c.Len(false) || len(live) > capacity {
				t.Fatalf("%s epoch %d bad=%d resident=%d", policy, epoch, bad.Load(), len(live))
			}
			switch p := c.(type) {
			case *ARC:
				keys, n := p.HotKeys(capacity)
				seen := map[any]bool{}
				for _, k := range keys {
					if seen[k] || live[k] != k {
						t.Fatalf("ARC index mismatch on %v", k)
					}
					seen[k] = true
				}
				if len(keys) != len(live) || n != len(live) {
					t.Fatal("ARC index size mismatch")
				}
			case *LRUCache:
				if len(p.recent.index) != len(live) || p.recent.order.Len() != len(live) {
					t.Fatal("LRU index size mismatch")
				}
			case *LFUCache:
				if len(p.location) != len(live) {
					t.Fatal("LFU index size mismatch")
				}
				total := 0
				for e := p.freqList.Front(); e != nil; e = e.Next() {
					for k := range e.Value.(*freqEntry).keys {
						if live[k] != k || p.location[k] != e {
							t.Fatal("LFU bucket mismatch")
						}
						total++
					}
				}
				if total != len(live) {
					t.Fatal("LFU bucket size mismatch")
				}
			}
		}
		emit(map[string]any{"Test": "state", "Policy": policy, "Steps": epochs * workers * steps, "Epochs": epochs, "Workers": workers})
	}
}
