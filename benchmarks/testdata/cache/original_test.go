package caches

import (
	"time"

	original "git.gametaptap.com/neutron/gotap/caches"
)

// The runner adds this adapter only when replaying the original library baseline.
func init() {
	originalCache = func(s scenario) workloadCache {
		return original.New(s.Capacity).EvictType(s.Policy).Expiration(time.Hour).Build()
	}
}
