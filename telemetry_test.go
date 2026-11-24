package caches

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/kumadrama/doku-lib/dlog"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestBatchTelemetry(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(ctx)
	failure := errors.New("source unavailable")
	c := New(4).ARC().Name("item_info").MLoaderContextFunc(func(ctx context.Context, keys []any) ([]any, *time.Duration, error) {
		if len(keys) == 1 {
			return nil, nil, failure
		}
		return []any{"v", nil}, nil, nil
	}).Build().(*ARC)
	c.signals.instruments = cacheInstruments(provider.Meter("doku.mem_cache"))
	if _, miss, err := c.MGetContext(ctx, []any{"a", "b"}); len(miss) != 2 || err != nil {
		t.Fatal(miss, err)
	}
	values, miss, err := c.MGetContext(ctx, []any{"b", "a", "c"})
	if len(miss) != 1 || miss[0] != 2 || values[1] != "v" || err != failure {
		t.Fatal(values, miss, err)
	}
	c.Remove("a")
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	want := map[string]int64{
		"mem_cache_lookup_keys/hit": 2, "mem_cache_lookup_keys/miss": 3,
		"mem_cache_loads/ok": 1, "mem_cache_loads/error": 1,
		"mem_cache_load_keys/ok": 2, "mem_cache_load_keys/error": 1,
		"mem_cache_load_duration/ok": 1, "mem_cache_load_duration/error": 1,
		"mem_cache_removals/remove": 1, "mem_cache_entries/": 1,
	}
	check := func(name string, attrs attribute.Set, value int64) {
		t.Helper()
		for _, key := range []attribute.Key{"psm", "ip", "cache", "policy"} {
			v, ok := attrs.Value(key)
			if !ok || v.AsString() == "" {
				t.Fatalf("%s missing %s", name, key)
			}
		}
		cache, _ := attrs.Value("cache")
		policy, _ := attrs.Value("policy")
		if cache.AsString() != "item_info" || policy.AsString() != "arc" {
			t.Fatal(attrs)
		}
		result, _ := attrs.Value("result")
		if reason, ok := attrs.Value("reason"); ok {
			result = reason
		}
		id := name + "/" + result.AsString()
		if expected, ok := want[id]; !ok || value != expected {
			t.Fatalf("%s = %d, want %d", id, value, expected)
		}
		delete(want, id)
	}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, p := range data.DataPoints {
					check(m.Name, p.Attributes, p.Value)
				}
			case metricdata.Gauge[int64]:
				for _, p := range data.DataPoints {
					check(m.Name, p.Attributes, p.Value)
				}
			case metricdata.Histogram[float64]:
				if m.Unit != "ms" {
					t.Fatal(m.Unit)
				}
				for _, p := range data.DataPoints {
					check(m.Name, p.Attributes, int64(p.Count))
				}
			}
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing metrics: %v", want)
	}
}

// Exercise the default Doku logger in a separate process so other tests keep
// their own stdout, logger configuration, and trace context.
func TestCacheFailureLog(t *testing.T) {
	if os.Getenv("MEM_CACHE_LOG_TEST") == "1" {
		ctx := dlog.ContextWithTrace(context.Background(), "cache-test-trace", "cache-test-span")
		release := make(chan struct{})
		started := make(chan struct{})
		var callers sync.WaitGroup
		c := New(4).LRU().Name("coalesced").LoaderContextFunc(func(ctx context.Context, k any) (any, *time.Duration, error) {
			close(started)
			<-release
			return nil, nil, errors.New("load failed")
		}).Build()
		callers.Add(1)
		go func() { defer callers.Done(); _, _ = c.GetContext(ctx, "private-key") }()
		// Use a nonblocking follower while the owner holds the flight. Its
		// return guarantees it has joined or started the same flight.
		<-started
		_, _ = c.GetIFPresentContext(ctx, "private-key")
		close(release)
		callers.Wait()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestCacheFailureLog$")
	cmd.Env = append(os.Environ(), "MEM_CACHE_LOG_TEST=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if bytes.Contains(out, []byte("private-key")) {
		t.Fatal("cache key leaked into log")
	}
	count := 0
	for _, line := range bytes.Split(out, []byte("\n")) {
		var fields map[string]any
		if json.Unmarshal(line, &fields) != nil || fields["event"] != "mem_cache.load_failed" {
			continue
		}
		count++
		if fields["trace_id"] != "cache-test-trace" || fields["span_id"] != "cache-test-span" || fields["cache"] != "coalesced" || fields["policy"] != "lru" || fields["key_count"] != float64(1) || fields["error"] != "load failed" {
			t.Fatalf("%v", fields)
		}
	}
	if count != 1 {
		t.Fatalf("got %d failure logs: %s", count, out)
	}
}
