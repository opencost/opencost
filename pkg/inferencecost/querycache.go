package inferencecost

import (
	"context"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sync/singleflight"
)

// metricsCache caches the per-window output of CollectMetrics + CalculateCosts.
//
// That step is the expensive part of every /inferenceCost request: two
// ComputeAllocation passes (with and without idle/shared sharing) plus the
// vLLM Prometheus queries. Its output depends only on the window, not on the
// cost basis, aggregation or filter, which are applied afterwards to fresh
// response structs. So one entry serves allocation and usage requests and
// every aggregation of the same window.
//
// Concurrent misses for the same window are collapsed with singleflight so
// the work runs once; previously, two concurrent 7d requests each ran their
// own allocation passes.
//
// The total estimated size of the entries is bounded by maxBytes. Making room
// for a new entry first drops expired entries and then evicts arbitrary ones
// (Go's randomised map iteration order) until the new entry fits; there is no
// LRU ordering.
//
// Every operation on entries and bytes holds mu, and each entry carries its
// own size and expiry, so lookup, expiry, eviction and size accounting are one
// atomic step. There is deliberately no background janitor and no eviction
// callback: go-cache, used previously, runs OnEvicted after releasing its lock,
// which let a sweep of an expired entry clear the accounting of the entry that
// had just replaced it under the same key. Instead, add sweeps expired entries
// under mu before every insert. Expired entries therefore stay in memory until
// the next miss, which is bounded by maxBytes and by the TTL of the entries the
// last insert kept.
//
// Cached slices are shared between requests and must be treated as read-only.
type metricsCache struct {
	group singleflight.Group
	ttl   time.Duration
	// maxBytes bounds the summed estimated entry sizes; 0 means unbounded.
	maxBytes int64
	// now is the clock; tests replace it to make expiry deterministic.
	now func() time.Time

	// mu guards entries and bytes. bytes is always the sum of entries' sizes.
	mu      sync.Mutex
	entries map[string]cacheEntry
	bytes   int64
}

type cacheEntry struct {
	value   []*InferenceCost
	size    int64
	expires time.Time
}

// newMetricsCache returns a cache with the given TTL and size bound in bytes
// (<= 0 for no bound), or nil (caching disabled) when ttl <= 0. A nil
// *metricsCache is valid and always computes.
func newMetricsCache(ttl time.Duration, maxBytes int64) *metricsCache {
	if ttl <= 0 {
		return nil
	}
	return &metricsCache{
		ttl:      ttl,
		maxBytes: max(maxBytes, 0),
		now:      time.Now,
		entries:  make(map[string]cacheEntry),
	}
}

// lookup returns the live entry for key. An expired entry is a miss; add
// removes it.
func (m *metricsCache) lookup(key string) ([]*InferenceCost, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[key]
	if !ok || !m.now().Before(e.expires) {
		return nil, false
	}
	return e.value, true
}

// usedBytes is the summed estimated size of the cached entries.
func (m *metricsCache) usedBytes() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.bytes
}

// removeLocked deletes key and releases its bytes. Caller holds mu.
func (m *metricsCache) removeLocked(key string) {
	if e, ok := m.entries[key]; ok {
		m.bytes -= e.size
		delete(m.entries, key)
	}
}

// add stores v under key, first dropping expired entries and then evicting
// live ones until it fits within maxBytes. An entry larger than maxBytes on
// its own is not cached.
func (m *metricsCache) add(key string, v []*InferenceCost) {
	size := estimateSize(v)

	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	for k, e := range m.entries {
		if !now.Before(e.expires) {
			m.removeLocked(k)
		}
	}
	// A previous entry under this key is replaced, so it never counts
	// against the room the new one needs.
	m.removeLocked(key)

	if m.maxBytes > 0 {
		if size > m.maxBytes {
			return
		}
		for k := range m.entries {
			if m.bytes+size <= m.maxBytes {
				break
			}
			m.removeLocked(k)
		}
	}

	m.entries[key] = cacheEntry{value: v, size: size, expires: now.Add(m.ttl)}
	m.bytes += size
}

// Rough per-value overheads for estimateSize. Exactness does not matter:
// the bound only has to track real memory closely enough to be useful.
const (
	mapOverheadBytes   = 48 + 8*(16+8) + 16 // hmap header + one bucket
	mapEntryBytes      = 16 + 8             // string header + float64
	slicePointerBytes  = 8
	sliceHeaderBytes   = 24
	inferenceCostBytes = int64(unsafe.Sizeof(InferenceCost{}))
)

// estimateSize approximates the heap held by a cached result: the struct
// values, their string contents and their per-basis maps.
func estimateSize(v []*InferenceCost) int64 {
	n := int64(sliceHeaderBytes + slicePointerBytes*cap(v))
	for _, ic := range v {
		if ic == nil {
			continue
		}
		p := ic.Properties
		n += inferenceCostBytes + int64(
			len(p.ModelName)+len(p.ModelVersion)+len(p.Namespace)+len(p.Cluster)+
				len(p.Pod)+len(p.Controller)+len(p.ControllerKind)+len(p.Container)+
				len(p.WorkloadType)+len(ic.AllocationMethod))
		for _, mp := range []map[CostBasis]float64{
			ic.InputCost, ic.OutputCost, ic.CostPerMillionTokens,
			ic.InputCostPerMillionTokens, ic.OutputCostPerMillionTokens,
		} {
			if mp == nil {
				continue
			}
			n += mapOverheadBytes
			for k := range mp {
				n += mapEntryBytes + int64(len(k))
			}
		}
	}
	return n
}

func metricsCacheKey(start, end time.Time) string {
	return start.UTC().Format(time.RFC3339Nano) + "," + end.UTC().Format(time.RFC3339Nano)
}

// get returns the cached result for [start, end], computing it on a miss.
// Errors are not cached, so the next request retries.
//
// The shared computation runs on a context detached from the caller's
// cancellation: when several requests wait on one computation, the first
// caller disconnecting must not fail the others, and a finished result is
// still worth caching. Each caller still stops waiting when its own context
// is done.
func (m *metricsCache) get(
	ctx context.Context,
	start, end time.Time,
	compute func(context.Context) ([]*InferenceCost, error),
) ([]*InferenceCost, error) {
	if m == nil {
		return compute(ctx)
	}

	key := metricsCacheKey(start, end)
	if v, ok := m.lookup(key); ok {
		return v, nil
	}

	ch := m.group.DoChan(key, func() (interface{}, error) {
		// Re-check: another flight for this key may have just finished.
		if v, ok := m.lookup(key); ok {
			return v, nil
		}
		res, err := compute(context.WithoutCancel(ctx))
		if err != nil {
			return nil, err
		}
		m.add(key, res)
		return res, nil
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-ch:
		if r.Err != nil {
			return nil, r.Err
		}
		return r.Val.([]*InferenceCost), nil
	}
}
