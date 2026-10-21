package inferencecost

import (
	"context"
	"sync"
	"time"
	"unsafe"

	"github.com/patrickmn/go-cache"
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
// The total estimated size of the entries is bounded by maxBytes. go-cache has
// no LRU ordering, so making room for a new entry first drops expired entries
// and then evicts arbitrary ones (Go's randomised map iteration order) until
// the new entry fits.
//
// Cached slices are shared between requests and must be treated as read-only.
type metricsCache struct {
	store *cache.Cache
	group singleflight.Group

	// maxBytes bounds the summed estimated entry sizes; 0 means unbounded.
	maxBytes int64
	// addMu serialises add so concurrent inserts cannot both pass the size
	// check. It is never held by onEvicted, so store.Delete under it is safe.
	addMu sync.Mutex
	// mu guards sizes and bytes, which mirror the entries in store.
	mu    sync.Mutex
	sizes map[string]int64
	bytes int64
}

// newMetricsCache returns a cache with the given TTL and size bound in bytes
// (<= 0 for no bound), or nil (caching disabled) when ttl <= 0. A nil
// *metricsCache is valid and always computes.
func newMetricsCache(ttl time.Duration, maxBytes int64) *metricsCache {
	if ttl <= 0 {
		return nil
	}
	// cleanup is only how often go-cache sweeps already-expired entries out
	// of memory; entries still expire at ttl on read regardless.
	cleanup := ttl
	if cleanup > 10*time.Minute {
		cleanup = 10 * time.Minute
	}
	m := &metricsCache{
		store:    cache.New(ttl, cleanup),
		maxBytes: max(maxBytes, 0),
		sizes:    make(map[string]int64),
	}
	// Called for explicit deletes and for expired entries the janitor or
	// DeleteExpired removes, always after go-cache has released its lock.
	m.store.OnEvicted(m.onEvicted)
	return m
}

func (m *metricsCache) onEvicted(key string, _ interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bytes -= m.sizes[key]
	delete(m.sizes, key)
}

// usedBytes is the summed estimated size of the cached entries.
func (m *metricsCache) usedBytes() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.bytes
}

// anyKey returns an arbitrary cached key, or "" when the cache is empty.
func (m *metricsCache) anyKey() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k := range m.sizes {
		return k
	}
	return ""
}

// add stores v under key, first evicting entries until it fits within
// maxBytes. An entry larger than maxBytes on its own is not cached.
func (m *metricsCache) add(key string, v []*InferenceCost) {
	size := estimateSize(v)

	m.addMu.Lock()
	defer m.addMu.Unlock()

	if m.maxBytes > 0 {
		if size > m.maxBytes {
			return
		}
		if m.usedBytes()+size > m.maxBytes {
			m.store.DeleteExpired()
		}
		for m.usedBytes()+size > m.maxBytes {
			victim := m.anyKey()
			if victim == "" {
				break
			}
			m.store.Delete(victim)
		}
	}

	m.store.SetDefault(key, v)
	m.mu.Lock()
	// The key may still be held by an expired entry the janitor has not
	// swept; SetDefault replaces it without calling onEvicted.
	m.bytes += size - m.sizes[key]
	m.sizes[key] = size
	m.mu.Unlock()
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
	if v, ok := m.store.Get(key); ok {
		return v.([]*InferenceCost), nil
	}

	ch := m.group.DoChan(key, func() (interface{}, error) {
		// Re-check: another flight for this key may have just finished.
		if v, ok := m.store.Get(key); ok {
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
