package caches

import "sync/atomic"

type statsAccessor interface {
	HitCount() uint64
	MissCount() uint64
	LookupCount() uint64
	HitRate() float64
}

type stats struct{ hits, misses atomic.Uint64 }

func (s *stats) add(hits, misses int) {
	if hits > 0 {
		s.hits.Add(uint64(hits))
	}
	if misses > 0 {
		s.misses.Add(uint64(misses))
	}
}

func (s *stats) IncrHitCount() uint64 { return s.hits.Add(1) }

func (s *stats) IncrMissCount() uint64 { return s.misses.Add(1) }

func (s *stats) HitCount() uint64 { return s.hits.Load() }

func (s *stats) MissCount() uint64 { return s.misses.Load() }

func (s *stats) LookupCount() uint64 { return s.hits.Load() + s.misses.Load() }

func (s *stats) HitRate() float64 {
	hits := s.hits.Load()
	total := hits + s.misses.Load()
	if total == 0 {
		return 0
	}
	return float64(hits) / float64(total)
}
