// External caller exercises only the public package API.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kumadrama/doku-lib/dlog"
	"github.com/kumadrama/mem-cache"
)

func emit(m map[string]any) {
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	fmt.Println("ROW", string(b))
}

func p99(samples []int64) int64 {
	if len(samples) == 0 {
		panic("no latency samples")
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	return samples[(99*len(samples)+99)/100-1]
}

func paced(width, rate, round int, duration time.Duration) {
	const workers, capacity, patterns = 32, 65536, 1024
	c := caches.New(capacity).ARC().Expiration(time.Hour).Name("paced").Build()
	keys := make([]any, capacity)
	for i := range keys {
		keys[i] = fmt.Sprintf("item:%08d", i)
		v := new([256]byte)
		v[0] = byte(i)
		if err := c.Set(keys[i], v); err != nil {
			panic(err)
		}
	}
	r := rand.New(rand.NewSource(168))
	queries := make([][]any, patterns)
	for seq := range queries {
		q := make([]any, width)
		for j := range q {
			q[j] = keys[r.Intn(len(keys))]
		}
		queries[seq] = q
		if _, m, e := c.MGet(q); len(m) != 0 || e != nil {
			panic("warmup")
		}
	}
	type job struct {
		seq int
		due time.Time
	}
	queue := make(chan job, 256)
	type result struct {
		end, service []int64
		done, bad    int
	}
	out := make([]result, workers)
	var ready, wg sync.WaitGroup
	offered := rate * int(duration/time.Second)
	for id := range out {
		wg.Add(1)
		ready.Add(1)
		go func(id int) {
			defer wg.Done()
			dst := &out[id]
			dst.end = make([]int64, 0, offered/workers/4)
			dst.service = make([]int64, 0, cap(dst.end))
			ready.Done()
			for j := range queue {
				q := queries[j.seq%patterns]
				begin := time.Now()
				vs, ms, e := c.MGetContext(context.Background(), q)
				end := time.Now()
				if e != nil || len(ms) != 0 || len(vs) != width || vs[0] == nil || vs[width-1] == nil {
					dst.bad++
				}
				dst.done++
				if dst.done%4 == 0 {
					dst.end = append(dst.end, end.Sub(j.due).Nanoseconds())
					dst.service = append(dst.service, end.Sub(begin).Nanoseconds())
				}
			}
		}(id)
	}
	ready.Wait()
	runtime.GC()
	start := time.Now().Add(time.Millisecond)
	late := make([]int64, 0, offered/16)
	dropped := 0
	for i := 0; i < offered; i++ {
		due := start.Add(time.Duration(int64(i) * int64(time.Second) / int64(rate)))
		if wait := time.Until(due); wait > 0 {
			time.Sleep(wait)
		}
		if i%16 == 0 {
			late = append(late, time.Since(due).Nanoseconds())
		}
		select {
		case queue <- job{i, due}:
		default:
			dropped++
		}
	}
	close(queue)
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	var end, service []int64
	done, bad := 0, 0
	for _, x := range out {
		end = append(end, x.end...)
		service = append(service, x.service...)
		done += x.done
		bad += x.bad
	}
	if done+dropped != offered || bad != 0 || c.Len(false) != capacity {
		panic("paced accounting")
	}
	emit(map[string]any{"Test": "paced", "Round": round, "Batch": width, "TargetRPS": rate, "Offered": offered, "Dropped": dropped, "Completed": done, "CompletedRPS": float64(done) / elapsed, "Errors": bad, "ServiceP99ns": p99(service), "ScheduledToEndP99ns": p99(end), "ProducerLateP99ns": p99(late), "Samples": len(end), "Seconds": elapsed})
}

func readDuringEviction(policy string, ttl bool, round int) {
	const capacity = 100000
	b := caches.New(capacity).EvictType(policy).Name("contended")
	if ttl {
		b.Expiration(time.Hour)
	}
	c := b.Build()
	for i := 0; i < capacity; i++ {
		if err := c.Set(i, i); err != nil {
			panic(err)
		}
	}
	const workers = 24
	type counter struct {
		samples []int64
		ops     int
		bad     int
	}
	rows := make([]counter, workers+1)
	var ready, done sync.WaitGroup
	start := make(chan struct{})
	var stop time.Time
	for id := range rows {
		ready.Add(1)
		done.Add(1)
		go func(id int) {
			defer done.Done()
			k := capacity
			r := rand.New(rand.NewSource(510 + int64(id)))
			row := &rows[id]
			ready.Done()
			<-start
			for time.Now().Before(stop) {
				sample := id == workers || row.ops%32 == 0
				key := r.Intn(capacity)
				var begin time.Time
				if sample {
					begin = time.Now()
				}
				var e error
				if id == workers {
					e = c.Set(k, k)
					k++
				} else {
					_, e = c.Get(key)
				}
				if sample {
					row.samples = append(row.samples, time.Since(begin).Nanoseconds())
				}
				if e != nil && (id == workers || e != caches.KeyNotFoundError) {
					row.bad++
				}
				row.ops++
			}
		}(id)
	}
	ready.Wait()
	runtime.GC()
	begin := time.Now()
	stop = begin.Add(2 * time.Second)
	close(start)
	done.Wait()
	secs := time.Since(begin).Seconds()
	var read []int64
	n, bad := 0, 0
	for _, x := range rows[:workers] {
		read = append(read, x.samples...)
		n += x.ops
		bad += x.bad
	}
	write := rows[workers]
	bad += write.bad
	if bad != 0 || c.Len(false) != capacity {
		panic("eviction contention")
	}
	emit(map[string]any{"Test": "interference", "Policy": policy, "TTL": ttl, "Round": round, "Reads": n, "Writes": write.ops, "ReadRPS": float64(n) / secs, "WriteRPS": float64(write.ops) / secs, "ReadP99ns": p99(read), "WriteP99ns": p99(write.samples), "HitKeys": c.HitCount(), "Errors": bad})
}

func logBurst() {
	const workers, per = 64, 50
	var calls, failures atomic.Int64
	target := errors.New("synthetic backend unavailable")
	c := caches.New(128).LRU().Name("log-pressure").LoaderContextFunc(func(ctx context.Context, k any) (any, *time.Duration, error) { calls.Add(1); return nil, nil, target }).Build()
	var wg sync.WaitGroup
	start := make(chan struct{})
	for id := 0; id < workers; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx := dlog.ContextWithTrace(context.Background(), fmt.Sprintf("local-trace-%02d", id), "synthetic-span")
			<-start
			for n := 0; n < per; n++ {
				_, e := c.GetContext(ctx, fmt.Sprintf("synthetic-private-key-%d-%d", id, n))
				if errors.Is(e, target) {
					failures.Add(1)
				}
			}
		}(id)
	}
	close(start)
	wg.Wait()
	if failures.Load() != workers*per || calls.Load() != workers*per || c.Len(false) != 0 {
		panic("log burst accounting")
	}
	emit(map[string]any{"Test": "error-burst", "Requests": workers * per, "Loads": calls.Load(), "ReturnedErrors": failures.Load()})
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "steady" {
		paced(32, 100000, 3, 15*time.Second)
		paced(256, 20000, 3, 15*time.Second)
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "logs" {
		logBurst()
		return
	}
	for round := 1; round <= 2; round++ {
		for _, p := range [][2]int{{32, 20000}, {256, 20000}, {32, 100000}, {256, 5000}, {32, 300000}, {256, 60000}} {
			paced(p[0], p[1], round, 3*time.Second)
		}
		for _, p := range []string{"simple", "lru"} {
			for _, ttl := range []bool{false, true} {
				readDuringEviction(p, ttl, round)
			}
		}
	}
}
