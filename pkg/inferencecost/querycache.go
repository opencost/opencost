package inferencecost

import (
	"context"
	"time"

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
// Cached slices are shared between requests and must be treated as read-only.
type metricsCache struct {
	store *cache.Cache
	group singleflight.Group
}

// newMetricsCache returns a cache with the given TTL, or nil (caching
// disabled) when ttl <= 0. A nil *metricsCache is valid and always computes.
func newMetricsCache(ttl time.Duration) *metricsCache {
	if ttl <= 0 {
		return nil
	}
	cleanup := ttl
	if cleanup > 10*time.Minute {
		cleanup = 10 * time.Minute
	}
	return &metricsCache{store: cache.New(ttl, cleanup)}
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
		m.store.SetDefault(key, res)
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
