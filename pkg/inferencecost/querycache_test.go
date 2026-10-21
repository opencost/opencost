package inferencecost

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

var (
	cacheStart = time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)
	cacheEnd   = cacheStart.Add(24 * time.Hour)
)

func oneResult(cost float64) []*InferenceCost {
	return []*InferenceCost{{AllocationTotalCost: cost}}
}

// countingCompute returns a compute func that counts its calls and returns
// a fresh result with the given cost.
func countingCompute(calls *atomic.Int32, cost float64) func(context.Context) ([]*InferenceCost, error) {
	return func(context.Context) ([]*InferenceCost, error) {
		calls.Add(1)
		return oneResult(cost), nil
	}
}

func TestNewMetricsCache_DisabledForNonPositiveTTL(t *testing.T) {
	if newMetricsCache(0, 0) != nil {
		t.Error("ttl 0 should disable the cache")
	}
	if newMetricsCache(-time.Minute, 0) != nil {
		t.Error("negative ttl should disable the cache")
	}
	if newMetricsCache(time.Hour, 0) == nil {
		t.Error("positive ttl should enable the cache")
	}
	if newMetricsCache(time.Second, 0) == nil {
		t.Error("short ttl should enable the cache")
	}
}

func TestMetricsCacheKey_NormalisesTimezone(t *testing.T) {
	est := time.FixedZone("EST", -5*3600)
	if metricsCacheKey(cacheStart, cacheEnd) != metricsCacheKey(cacheStart.In(est), cacheEnd.In(est)) {
		t.Error("the same instants in different zones must share a key")
	}
	if metricsCacheKey(cacheStart, cacheEnd) == metricsCacheKey(cacheStart, cacheEnd.Add(time.Nanosecond)) {
		t.Error("different windows must not share a key")
	}
}

func TestMetricsCache_NilAlwaysComputes(t *testing.T) {
	var m *metricsCache
	var calls atomic.Int32
	for i := 0; i < 3; i++ {
		if _, err := m.get(context.Background(), cacheStart, cacheEnd, countingCompute(&calls, 1)); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 3 {
		t.Errorf("nil cache: expected 3 computes, got %d", calls.Load())
	}
}

func TestMetricsCache_HitWithinTTL(t *testing.T) {
	m := newMetricsCache(time.Hour, 0)
	var calls atomic.Int32
	first, err := m.get(context.Background(), cacheStart, cacheEnd, countingCompute(&calls, 7))
	if err != nil {
		t.Fatal(err)
	}
	second, err := m.get(context.Background(), cacheStart, cacheEnd, countingCompute(&calls, 99))
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Errorf("expected 1 compute, got %d", calls.Load())
	}
	if second[0] != first[0] || second[0].AllocationTotalCost != 7 {
		t.Error("second call should return the cached result")
	}
}

func TestMetricsCache_SeparateEntriesPerWindow(t *testing.T) {
	m := newMetricsCache(time.Hour, 0)
	var calls atomic.Int32
	_, _ = m.get(context.Background(), cacheStart, cacheEnd, countingCompute(&calls, 1))
	_, _ = m.get(context.Background(), cacheStart, cacheEnd.Add(time.Hour), countingCompute(&calls, 2))
	_, _ = m.get(context.Background(), cacheStart.Add(time.Hour), cacheEnd, countingCompute(&calls, 3))
	if calls.Load() != 3 {
		t.Errorf("expected one compute per distinct window, got %d", calls.Load())
	}
}

func TestMetricsCache_ExpiresAfterTTL(t *testing.T) {
	m := newMetricsCache(30*time.Millisecond, 0)
	var calls atomic.Int32
	_, _ = m.get(context.Background(), cacheStart, cacheEnd, countingCompute(&calls, 1))
	time.Sleep(60 * time.Millisecond)
	got, err := m.get(context.Background(), cacheStart, cacheEnd, countingCompute(&calls, 2))
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || got[0].AllocationTotalCost != 2 {
		t.Errorf("expected a recompute after expiry, calls=%d cost=%v", calls.Load(), got[0].AllocationTotalCost)
	}
}

func TestMetricsCache_ErrorsAreNotCached(t *testing.T) {
	m := newMetricsCache(time.Hour, 0)
	boom := errors.New("prometheus down")
	_, err := m.get(context.Background(), cacheStart, cacheEnd, func(context.Context) ([]*InferenceCost, error) {
		return nil, boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("expected the compute error, got %v", err)
	}
	var calls atomic.Int32
	if _, err := m.get(context.Background(), cacheStart, cacheEnd, countingCompute(&calls, 1)); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Error("a failed compute must not be cached")
	}
}

func TestMetricsCache_ConcurrentMissesComputeOnce(t *testing.T) {
	m := newMetricsCache(time.Hour, 0)
	var calls atomic.Int32
	release := make(chan struct{})
	compute := func(context.Context) ([]*InferenceCost, error) {
		calls.Add(1)
		<-release
		return oneResult(5), nil
	}

	const n = 8
	var wg sync.WaitGroup
	results := make([][]*InferenceCost, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = m.get(context.Background(), cacheStart, cacheEnd, compute)
		}(i)
	}
	// Let every goroutine reach the flight before it completes.
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	if calls.Load() != 1 {
		t.Errorf("expected 1 compute for %d concurrent callers, got %d", n, calls.Load())
	}
	for i := 0; i < n; i++ {
		if errs[i] != nil || results[i][0] != results[0][0] {
			t.Fatalf("caller %d: err=%v, want the shared result", i, errs[i])
		}
	}
}

func TestMetricsCache_CallerCancelDoesNotAbortSharedCompute(t *testing.T) {
	m := newMetricsCache(time.Hour, 0)
	release := make(chan struct{})
	var computeCtxErr atomic.Pointer[error]
	compute := func(ctx context.Context) ([]*InferenceCost, error) {
		<-release
		err := ctx.Err()
		computeCtxErr.Store(&err)
		return oneResult(3), nil
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancelledErr := make(chan error, 1)
	go func() {
		_, err := m.get(cancelled, cacheStart, cacheEnd, compute)
		cancelledErr <- err
	}()
	patient := make(chan []*InferenceCost, 1)
	go func() {
		res, _ := m.get(context.Background(), cacheStart, cacheEnd, compute)
		patient <- res
	}()
	time.Sleep(50 * time.Millisecond)

	cancel()
	if err := <-cancelledErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled caller should get context.Canceled, got %v", err)
	}
	close(release)
	res := <-patient
	if res == nil || res[0].AllocationTotalCost != 3 {
		t.Fatal("the other caller should still get the result")
	}
	if p := computeCtxErr.Load(); p == nil || *p != nil {
		t.Errorf("shared compute must run and must not see the caller's cancellation, got %v", p)
	}

	var calls atomic.Int32
	_, _ = m.get(context.Background(), cacheStart, cacheEnd, countingCompute(&calls, 9))
	if calls.Load() != 0 {
		t.Error("the result should have been cached despite the cancellation")
	}
}

// --- QueryService integration: the expensive path runs once per window ---

// countingQuerier wraps an AllocationQuerier and counts ComputeAllocation calls.
type countingQuerier struct {
	inner AllocationQuerier
	calls atomic.Int32
}

func (c *countingQuerier) ComputeAllocation(start, end time.Time) (*opencost.AllocationSet, error) {
	c.calls.Add(1)
	return c.inner.ComputeAllocation(start, end)
}

func newCountingQueryService(t *testing.T) (*QueryService, *countingQuerier, func()) {
	t.Helper()
	qs, srv := newQueryServiceWithDirectMetrics(t, "llama-3", "llm-prod", 10.0, 1_000_000)
	cq := &countingQuerier{inner: qs.collector.allocationQuerier}
	qs.collector.allocationQuerier = cq
	return qs, cq, srv.Close
}

func totalCost(t *testing.T, qs *QueryService, req *QueryRequest) float64 {
	t.Helper()
	setRange, err := qs.query(context.Background(), req)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	var tc float64
	for _, ic := range accumulate(setRange.InferenceCostSets).InferenceCosts {
		tc += ic.TotalCost
	}
	return tc
}

func TestQueryService_CachesAcrossCostBasisAndAggregation(t *testing.T) {
	qs, cq, closeSrv := newCountingQueryService(t)
	defer closeSrv()
	if qs.cache == nil {
		t.Fatal("cache should be enabled by default")
	}

	now := time.Now().UTC().Truncate(time.Hour)
	base := QueryRequest{Start: now.Add(-time.Hour), End: now, Step: time.Hour}

	alloc := base
	alloc.CostBasis = CostBasisAllocation
	allocCost := totalCost(t, qs, &alloc)
	perWindow := cq.calls.Load()
	if perWindow == 0 {
		t.Fatal("expected ComputeAllocation to run on the first request")
	}

	usage := base
	usage.CostBasis = CostBasisUsage
	usageCost := totalCost(t, qs, &usage)

	byModel := alloc
	byModel.AggregateBy = []string{"model_name"}
	byModelCost := totalCost(t, qs, &byModel)

	if got := cq.calls.Load(); got != perWindow {
		t.Errorf("later requests for the same window should hit the cache: ComputeAllocation calls %d -> %d", perWindow, got)
	}
	if usageCost > allocCost {
		t.Errorf("usage (%.4f) > allocation (%.4f) when served from cache", usageCost, allocCost)
	}
	// Projection and aggregation must not mutate the shared cached metrics.
	if again := totalCost(t, qs, &alloc); again != allocCost || byModelCost != allocCost {
		t.Errorf("cached results changed across requests: first=%.4f again=%.4f byModel=%.4f", allocCost, again, byModelCost)
	}
}

func TestQueryService_CachesPerStep(t *testing.T) {
	qs, cq, closeSrv := newCountingQueryService(t)
	defer closeSrv()
	now := time.Now().UTC().Truncate(time.Hour)

	one := &QueryRequest{Start: now.Add(-time.Hour), End: now, Step: time.Hour, CostBasis: CostBasisAllocation}
	totalCost(t, qs, one)
	perWindow := cq.calls.Load()

	// A two-hour hourly timeseries reuses the cached last hour and computes one new step.
	two := &QueryRequest{Start: now.Add(-2 * time.Hour), End: now, Step: time.Hour, CostBasis: CostBasisAllocation}
	totalCost(t, qs, two)
	if got := cq.calls.Load(); got != 2*perWindow {
		t.Errorf("expected only the new step to be computed: calls %d, want %d", got, 2*perWindow)
	}
}

func TestQueryService_CacheDisabledByEnv(t *testing.T) {
	t.Setenv("INFERENCE_QUERY_CACHE_TTL", "0")
	qs, cq, closeSrv := newCountingQueryService(t)
	defer closeSrv()
	if qs.cache != nil {
		t.Fatal("INFERENCE_QUERY_CACHE_TTL=0 should disable the cache")
	}
	now := time.Now().UTC().Truncate(time.Hour)
	req := &QueryRequest{Start: now.Add(-time.Hour), End: now, Step: time.Hour, CostBasis: CostBasisAllocation}
	totalCost(t, qs, req)
	perWindow := cq.calls.Load()
	totalCost(t, qs, req)
	if got := cq.calls.Load(); got != 2*perWindow {
		t.Errorf("without a cache every request recomputes: calls %d, want %d", got, 2*perWindow)
	}
}

func TestQueryService_CollectErrorIsReturnedAndNotCached(t *testing.T) {
	qs, srv := newQueryServiceWithDirectMetrics(t, "llama-3", "llm-prod", 10.0, 1_000_000)
	defer srv.Close()
	good := qs.collector.allocationQuerier
	qs.collector.allocationQuerier = &mockQuerier{err: errors.New("allocation backend down")}

	now := time.Now().UTC().Truncate(time.Hour)
	req := &QueryRequest{Start: now.Add(-time.Hour), End: now, Step: time.Hour, CostBasis: CostBasisAllocation}
	if _, err := qs.query(context.Background(), req); err == nil {
		t.Fatal("expected the collector error")
	}

	qs.collector.allocationQuerier = good
	if _, err := qs.query(context.Background(), req); err != nil {
		t.Fatalf("after the backend recovers the query should succeed, got %v", err)
	}
}

// --- size bound ---

// window returns the i-th distinct one-hour window, for distinct cache keys.
func window(i int) (time.Time, time.Time) {
	s := cacheStart.Add(time.Duration(i) * time.Hour)
	return s, s.Add(time.Hour)
}

func fill(t *testing.T, m *metricsCache, i int, calls *atomic.Int32) {
	t.Helper()
	s, e := window(i)
	if _, err := m.get(context.Background(), s, e, countingCompute(calls, float64(i))); err != nil {
		t.Fatal(err)
	}
}

// cached reports whether window i is held, without computing on a miss.
func cached(m *metricsCache, i int) bool {
	s, e := window(i)
	_, ok := m.store.Get(metricsCacheKey(s, e))
	return ok
}

// checkAccounting asserts the size bookkeeping mirrors the store exactly.
func checkAccounting(t *testing.T, m *metricsCache) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	items := m.store.Items()
	if len(items) != len(m.sizes) {
		t.Fatalf("store has %d entries, size map has %d", len(items), len(m.sizes))
	}
	var sum int64
	for k, it := range items {
		want := estimateSize(it.Object.([]*InferenceCost))
		if m.sizes[k] != want {
			t.Fatalf("size for %s: tracked %d, actual %d", k, m.sizes[k], want)
		}
		sum += want
	}
	if sum != m.bytes {
		t.Fatalf("tracked total %d, actual %d", m.bytes, sum)
	}
}

var unit = estimateSize(oneResult(0))

func TestMetricsCache_EvictsToFitNewEntry(t *testing.T) {
	m := newMetricsCache(time.Hour, 2*unit+unit/2) // room for two entries
	var calls atomic.Int32
	for i := 0; i < 5; i++ {
		fill(t, m, i, &calls)
		if used := m.usedBytes(); used > m.maxBytes {
			t.Fatalf("after add %d: %d bytes used, bound %d", i, used, m.maxBytes)
		}
		checkAccounting(t, m)
	}
	if m.store.ItemCount() != 2 {
		t.Errorf("expected 2 entries to fit, got %d", m.store.ItemCount())
	}
	if !cached(m, 4) {
		t.Error("the newest entry must be kept")
	}
}

func TestMetricsCache_EntryLargerThanBoundIsNotCached(t *testing.T) {
	m := newMetricsCache(time.Hour, unit-1)
	var calls atomic.Int32
	fill(t, m, 0, &calls)
	fill(t, m, 0, &calls)
	if calls.Load() != 2 {
		t.Errorf("an oversized result must be returned but not cached: computes %d", calls.Load())
	}
	if m.usedBytes() != 0 || m.store.ItemCount() != 0 {
		t.Errorf("nothing should be stored, got %d bytes / %d entries", m.usedBytes(), m.store.ItemCount())
	}
}

func TestMetricsCache_UnboundedWhenMaxIsZeroOrNegative(t *testing.T) {
	for _, max := range []int64{0, -1} {
		m := newMetricsCache(time.Hour, max)
		var calls atomic.Int32
		for i := 0; i < 20; i++ {
			fill(t, m, i, &calls)
		}
		if m.store.ItemCount() != 20 {
			t.Errorf("max %d: expected all 20 entries, got %d", max, m.store.ItemCount())
		}
		checkAccounting(t, m)
	}
}

func TestMetricsCache_DropsExpiredEntriesBeforeLiveOnes(t *testing.T) {
	const ttl = 300 * time.Millisecond
	m := newMetricsCache(ttl, 2*unit+unit/2)
	var calls atomic.Int32
	fill(t, m, 0, &calls)
	time.Sleep(ttl + 50*time.Millisecond) // 0 expired, not yet swept
	fill(t, m, 1, &calls)                 // fits alongside the expired 0
	fill(t, m, 2, &calls)                 // needs room: 0 must go, not 1
	if !cached(m, 1) || !cached(m, 2) {
		t.Errorf("live entries were evicted while an expired one was available: 1=%v 2=%v", cached(m, 1), cached(m, 2))
	}
	checkAccounting(t, m)
}

func TestMetricsCache_ExpirySweepReleasesBytes(t *testing.T) {
	m := newMetricsCache(30*time.Millisecond, 10*unit)
	var calls atomic.Int32
	fill(t, m, 0, &calls)
	fill(t, m, 1, &calls)
	time.Sleep(60 * time.Millisecond)
	m.store.DeleteExpired()
	if m.usedBytes() != 0 {
		t.Errorf("expired entries must release their bytes, %d still counted", m.usedBytes())
	}
	checkAccounting(t, m)
}

func TestMetricsCache_ReplacingAnUnsweptExpiredKeyCountsOnce(t *testing.T) {
	m := newMetricsCache(30*time.Millisecond, 10*unit)
	var calls atomic.Int32
	fill(t, m, 0, &calls)
	time.Sleep(60 * time.Millisecond) // expired; the janitor runs far later
	fill(t, m, 0, &calls)             // miss, recompute, same key
	if calls.Load() != 2 {
		t.Fatalf("expected a recompute, got %d computes", calls.Load())
	}
	if m.usedBytes() != unit {
		t.Errorf("one live entry should count once: %d bytes, want %d", m.usedBytes(), unit)
	}
	checkAccounting(t, m)
}

func TestMetricsCache_ConcurrentAddsStayWithinBound(t *testing.T) {
	m := newMetricsCache(time.Hour, 5*unit+unit/2)
	var calls atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s, e := window(i)
			_, _ = m.get(context.Background(), s, e, countingCompute(&calls, float64(i)))
		}(i)
	}
	wg.Wait()
	if used := m.usedBytes(); used > m.maxBytes {
		t.Errorf("%d bytes used, bound %d", used, m.maxBytes)
	}
	if m.store.ItemCount() != 5 {
		t.Errorf("expected the cache full at 5 entries, got %d", m.store.ItemCount())
	}
	checkAccounting(t, m)
}

func TestEstimateSize(t *testing.T) {
	if got := estimateSize(nil); got != sliceHeaderBytes {
		t.Errorf("empty result: %d, want %d", got, sliceHeaderBytes)
	}
	if got, want := estimateSize([]*InferenceCost{nil}), int64(sliceHeaderBytes+slicePointerBytes); got != want {
		t.Errorf("nil entries count only their pointer: %d, want %d", got, want)
	}

	base := &InferenceCost{}
	named := &InferenceCost{Properties: InferenceCostProperties{ModelName: "llama-3", Namespace: "llm-prod"}, AllocationMethod: "x"}
	if d := estimateSize([]*InferenceCost{named}) - estimateSize([]*InferenceCost{base}); d != int64(len("llama-3")+len("llm-prod")+1) {
		t.Errorf("string contents should add their length, got +%d", d)
	}

	withMap := &InferenceCost{CostPerMillionTokens: map[CostBasis]float64{CostBasisAllocation: 1, CostBasisUsage: 2}}
	wantMap := int64(mapOverheadBytes + 2*mapEntryBytes + len(CostBasisAllocation) + len(CostBasisUsage))
	if d := estimateSize([]*InferenceCost{withMap}) - estimateSize([]*InferenceCost{base}); d != wantMap {
		t.Errorf("a two-entry map should add %d, got +%d", wantMap, d)
	}
}

func TestGetQueryCacheMaxBytes(t *testing.T) {
	t.Setenv("INFERENCE_QUERY_CACHE_MAX_SIZE_MB", "3")
	if got := getQueryCacheMaxBytes(); got != 3<<20 {
		t.Errorf("3 MiB: got %d bytes", got)
	}
	t.Setenv("INFERENCE_QUERY_CACHE_MAX_SIZE_MB", "")
	if got := getQueryCacheMaxBytes(); got != 100<<20 {
		t.Errorf("default should be 100 MiB, got %d bytes", got)
	}
}
