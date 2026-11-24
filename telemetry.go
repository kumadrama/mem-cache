package caches

import (
	"context"
	"strings"
	"time"

	"github.com/kumadrama/doku-lib/denv"

	"github.com/kumadrama/doku-lib/dlog"
	"github.com/kumadrama/doku-lib/dmetrics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type instruments struct {
	lookups, loads, loadKeys, evictions metric.Int64Counter
	duration                            metric.Float64Histogram
	entries                             metric.Int64Gauge
}

func cacheInstruments(m metric.Meter) instruments {
	var i instruments
	i.lookups, _ = m.Int64Counter("mem_cache_lookup_keys")
	i.loads, _ = m.Int64Counter("mem_cache_loads")
	i.loadKeys, _ = m.Int64Counter("mem_cache_load_keys")
	i.evictions, _ = m.Int64Counter("mem_cache_removals")
	i.duration, _ = m.Float64Histogram("mem_cache_load_duration", metric.WithUnit("ms"))
	i.entries, _ = m.Int64Gauge("mem_cache_entries")
	return i
}

var defaultInstruments = cacheInstruments(dmetrics.Meter("doku.mem_cache"))

// Custom instruments on dmetrics.Meter need the same per-instance dimensions
// as Doku's named counters so different tasks produce distinct time series.
func instanceLabel(s string) string {
	if v := strings.TrimSpace(s); v != "" {
		return v
	}
	return "unknown"
}

var instancePSM = attribute.String("psm", instanceLabel(denv.PSM()))
var instanceIP = attribute.String("ip", instanceLabel(denv.PrivateIPv4()))

type signals struct {
	instruments
	name, policy    string
	base, hit, miss metric.MeasurementOption
}

func newSignals(name, policy string) *signals {
	s := &signals{instruments: defaultInstruments, name: name, policy: policy}
	s.base = s.attributes()
	s.hit = s.attributes(attribute.String("result", "hit"))
	s.miss = s.attributes(attribute.String("result", "miss"))
	return s
}

func (s *signals) attributes(extra ...attribute.KeyValue) metric.MeasurementOption {
	attrs := append([]attribute.KeyValue{instancePSM, instanceIP, attribute.String("cache", s.name), attribute.String("policy", s.policy)}, extra...)
	return metric.WithAttributeSet(attribute.NewSet(attrs...))
}

func (s *signals) lookup(ctx context.Context, hits, misses int) {
	if hits > 0 {
		s.lookups.Add(ctx, int64(hits), s.hit)
	}
	if misses > 0 {
		s.lookups.Add(ctx, int64(misses), s.miss)
	}
}

func (s *signals) size(ctx context.Context, n int) { s.entries.Record(ctx, int64(n), s.base) }

func (s *signals) removed(ctx context.Context, reason string, n int) {
	if n > 0 {
		s.evictions.Add(ctx, int64(n), s.attributes(attribute.String("reason", reason)))
	}
}

func (s *signals) loaded(ctx context.Context, op string, n int, d time.Duration, err error) {
	result := "ok"
	ms := float64(d) / float64(time.Millisecond)
	if err != nil {
		result = "error"
		dlog.Warn(ctx).StrKV("event", "mem_cache.load_failed").StrKV("cache", s.name).StrKV("policy", s.policy).
			StrKV("operation", op).KV("key_count", n).KV("duration_ms", ms).StrKV("error", err.Error()).Msg("cache load failed").Emit()
	}
	attrs := s.attributes(attribute.String("operation", op), attribute.String("result", result))
	s.loads.Add(ctx, 1, attrs)
	s.loadKeys.Add(ctx, int64(n), attrs)
	s.duration.Record(ctx, ms, attrs)
}
