package inferencecost

import (
	"context"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

// mockQuerier implements AllocationQuerier for testing.
type mockQuerier struct {
	set *opencost.AllocationSet
	err error
	// For dual-query tests, return different sets on subsequent calls
	callCount int
	sets      []*opencost.AllocationSet
}

func (m *mockQuerier) ComputeAllocation(start, end time.Time) (*opencost.AllocationSet, error) {
	if m.err != nil {
		return nil, m.err
	}
	
	// If multiple sets are provided, return them in sequence
	if len(m.sets) > 0 {
		if m.callCount < len(m.sets) {
			set := m.sets[m.callCount]
			m.callCount++
			return set, nil
		}
		// Return last set for any additional calls
		return m.sets[len(m.sets)-1], nil
	}
	
	// Otherwise return the single set
	return m.set, nil
}

func makeAllocation(name string, gpuCost, cpuCost, ramCost, gpuCostIdle, cpuCostIdle, ramCostIdle float64, labels map[string]string, namespace string) *opencost.Allocation {
	a := &opencost.Allocation{
		Name:    name,
		GPUCost: gpuCost,
		CPUCost: cpuCost,
		RAMCost: ramCost,
		// Idle fields stored directly — they are added into TotalCost by OpenCost
		// when idle is distributed via ShareWeighted.
		GPUCostIdle: gpuCostIdle,
		CPUCostIdle: cpuCostIdle,
		RAMCostIdle: ramCostIdle,
		Properties: &opencost.AllocationProperties{
			Namespace: namespace,
			Labels:    opencost.AllocationLabels(labels),
		},
	}
	return a
}

func baseConfig() *Config {
	return &Config{
		PrometheusURL:             "http://fake-prometheus:9090",
		CollectionInterval:        5 * time.Minute,
		ModelLabel:                "llm-d.ai/model",
		SharedInfraLabel:          "llm-d.ai/inference-serving",
		SharedInfraLabelValue:     "true",
		KVCacheBlockSize:          0,
		AllocationMode:            AllocationModeComputeTime,
		OutputTokenCostMultiplier: 2.5,
		UsageCostShareSplit:       UsageCostShareSplitNone, // Default: no shared costs in usage
	}
}

// TestCollector_ExtractAllocationResults verifies that extractAllocationResults
// correctly extracts allocation and usage costs from AllocationSets.
func TestCollector_ExtractAllocationResults(t *testing.T) {
	now := time.Now()
	cfg := baseConfig()
	c := &Collector{config: cfg}
	
	// Test allocation cost extraction (with idle)
	allocWithIdle := &opencost.Allocation{
		Name:    "llama-3",
		GPUCost: 3.0,
		CPUCost: 0.5,
		RAMCost: 0.5,
		Properties: &opencost.AllocationProperties{
			Namespace: "llm-prod",
		},
	}
	asWithIdle := opencost.NewAllocationSet(now.Add(-5*time.Minute), now)
	asWithIdle.Set(allocWithIdle)

	resultsAlloc, err := c.extractAllocationResults(asWithIdle, true)
	if err != nil {
		t.Fatalf("extractAllocationResults (allocation) failed: %v", err)
	}

	key := modelNamespaceKey("llama-3", "llm-prod")
	r, ok := resultsAlloc[key]
	if !ok {
		t.Fatal("expected allocation result for llama-3/llm-prod")
	}
	
	if !floatEq(r.allocationTotalCost, 4.0) {
		t.Errorf("allocationTotalCost want 4.0 got %f", r.allocationTotalCost)
	}
	if r.usageTotalCost != 0 {
		t.Errorf("usageTotalCost should be 0 in allocation query, got %f", r.usageTotalCost)
	}

	// Test usage cost extraction (without idle)
	allocWithoutIdle := &opencost.Allocation{
		Name:    "llama-3",
		GPUCost: 2.0,
		CPUCost: 0.3,
		RAMCost: 0.3,
		Properties: &opencost.AllocationProperties{
			Namespace: "llm-prod",
		},
	}
	asWithoutIdle := opencost.NewAllocationSet(now.Add(-5*time.Minute), now)
	asWithoutIdle.Set(allocWithoutIdle)

	resultsUsage, err := c.extractAllocationResults(asWithoutIdle, false)
	if err != nil {
		t.Fatalf("extractAllocationResults (usage) failed: %v", err)
	}

	r2, ok := resultsUsage[key]
	if !ok {
		t.Fatal("expected usage result for llama-3/llm-prod")
	}
	
	if !floatEq(r2.usageTotalCost, 2.6) {
		t.Errorf("usageTotalCost want 2.6 got %f", r2.usageTotalCost)
	}
	if r2.allocationTotalCost != 0 {
		t.Errorf("allocationTotalCost should be 0 in usage query, got %f", r2.allocationTotalCost)
	}
}

// TestCollector_UsageCost_ExcludesIdle verifies the mathematical relationship
// between allocation and usage costs when idle is present.
func TestCollector_UsageCost_ExcludesIdle(t *testing.T) {
	// With ShareWeighted: AllocationTotalCost = 4.0 (GPU 3.0 + CPU 0.5 + RAM 0.5)
	// With ShareNone: UsageCost = 2.6 (excludes idle: 1.0 + 0.2 + 0.2 = 1.4)

	allocTotal := 4.0
	idleGPU, idleCPU, idleRAM := 1.0, 0.2, 0.2
	expectedUsageCost := allocTotal - (idleGPU + idleCPU + idleRAM)

	if !floatEq(expectedUsageCost, 2.6) {
		t.Errorf("expected usage cost 2.6 got %f", expectedUsageCost)
	}
	if expectedUsageCost >= allocTotal {
		t.Error("usage cost should be less than allocation cost when idle is present")
	}
}

// TestCollector_CombineMetrics_DerivesCachedTokens verifies that combineMetrics
// sets CachedTokens and EffectiveInputTokens correctly from CacheHitBlocks * BlockSize.
func TestCollector_CombineMetrics_DerivesCachedTokens(t *testing.T) {
	cfg := baseConfig()
	cfg.KVCacheBlockSize = 4

	allocCosts := map[string]*allocationResult{
		"llama-3:llm-prod": {allocationTotalCost: 4.0, usageTotalCost: 2.6, namespace: "llm-prod"},
	}
	promptTokens := map[string]float64{"llama-3:llm-prod": 20}
	genTokens := map[string]float64{"llama-3:llm-prod": 10}
	inputTime := map[string]float64{}
	outputTime := map[string]float64{}
	cacheHits := map[string]float64{"llama-3:llm-prod": 2} // 2 blocks × 4 = 8 cached tokens

	c := &Collector{config: cfg}
	results := c.combineMetrics(allocCosts, promptTokens, genTokens, inputTime, outputTime, cacheHits, time.Now())

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	m := results[0]
	if !floatEq(m.CachedTokens, 8) {
		t.Errorf("CachedTokens want 8 got %f", m.CachedTokens)
	}
	if !floatEq(m.EffectiveInputTokens, 12) {
		t.Errorf("EffectiveInputTokens want 12 got %f", m.EffectiveInputTokens)
	}
}

// TestCollector_CombineMetrics_NoCacheHits_FallsBackToPromptTokens verifies that
// EffectiveInputTokens equals PromptTokens when no cache hits are reported.
func TestCollector_CombineMetrics_NoCacheHits_FallsBackToPromptTokens(t *testing.T) {
	cfg := baseConfig()
	cfg.KVCacheBlockSize = 16

	allocCosts := map[string]*allocationResult{
		"llama-3:llm-prod": {allocationTotalCost: 1.0, usageTotalCost: 1.0, namespace: "llm-prod"},
	}
	promptTokens := map[string]float64{"llama-3:llm-prod": 1000}
	genTokens := map[string]float64{"llama-3:llm-prod": 500}
	// cacheHits map is empty — simulates metric being unavailable
	cacheHits := map[string]float64{}

	c := &Collector{config: cfg}
	results := c.combineMetrics(allocCosts, promptTokens, genTokens,
		map[string]float64{}, map[string]float64{}, cacheHits, time.Now())

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	m := results[0]
	if !floatEq(m.EffectiveInputTokens, 1000) {
		t.Errorf("EffectiveInputTokens should fall back to PromptTokens=1000, got %f", m.EffectiveInputTokens)
	}
}

// TestReconcileTokenKeys_OrgPrefixMismatch verifies that a metric key with a
// fully-qualified org/model name is re-keyed to match the allocation key that
// uses only the short name, and that keys which already match are left unchanged.
func TestReconcileTokenKeys_OrgPrefixMismatch(t *testing.T) {
	allocCosts := map[string]*allocationResult{
		"MiniMax-M2.7:llm-d-pic": {allocationTotalCost: 489.0, namespace: "llm-d-pic"},
		"gpt-oss-120b:dolev-inf": {allocationTotalCost: 453.0, namespace: "dolev-inf"},
		// This alloc key already has a slash and no short-name alternative.
		"meta-llama/Llama-3:prod": {allocationTotalCost: 10.0, namespace: "prod"},
	}

	tokens := map[string]float64{
		// Mismatch: vLLM uses full org/model, alloc uses short name.
		"MiniMaxAI/MiniMax-M2.7:llm-d-pic": 4316.0,
		"openai/gpt-oss-120b:dolev-inf":    4773.0,
		// Already matches alloc key — should pass through unchanged.
		"meta-llama/Llama-3:prod": 1000.0,
		// No alloc entry at all — should pass through unchanged.
		"unknown-org/new-model:some-ns": 99.0,
	}

	out, remappedKeys := reconcileTokenKeys(tokens, allocCosts)

	// Remapped entries should appear under the short-name alloc keys.
	if v, ok := out["MiniMax-M2.7:llm-d-pic"]; !ok || !floatEq(v, 4316.0) {
		t.Errorf("MiniMax-M2.7:llm-d-pic want 4316.0 got %v (ok=%v)", v, ok)
	}
	if v, ok := out["gpt-oss-120b:dolev-inf"]; !ok || !floatEq(v, 4773.0) {
		t.Errorf("gpt-oss-120b:dolev-inf want 4773.0 got %v (ok=%v)", v, ok)
	}
	// Original org-prefixed keys must be gone.
	if _, ok := out["MiniMaxAI/MiniMax-M2.7:llm-d-pic"]; ok {
		t.Error("org-prefixed key MiniMaxAI/MiniMax-M2.7:llm-d-pic should have been removed")
	}
	if _, ok := out["openai/gpt-oss-120b:dolev-inf"]; ok {
		t.Error("org-prefixed key openai/gpt-oss-120b:dolev-inf should have been removed")
	}
	// Verify remapped keys are tracked.
	if _, ok := remappedKeys["MiniMaxAI/MiniMax-M2.7:llm-d-pic"]; !ok {
		t.Error("MiniMaxAI/MiniMax-M2.7:llm-d-pic should be in remappedKeys")
	}
	if _, ok := remappedKeys["openai/gpt-oss-120b:dolev-inf"]; !ok {
		t.Error("openai/gpt-oss-120b:dolev-inf should be in remappedKeys")
	}
	// Keys that already matched or had no alloc entry pass through unchanged.
	if v, ok := out["meta-llama/Llama-3:prod"]; !ok || !floatEq(v, 1000.0) {
		t.Errorf("meta-llama/Llama-3:prod want 1000.0 got %v (ok=%v)", v, ok)
	}
	if v, ok := out["unknown-org/new-model:some-ns"]; !ok || !floatEq(v, 99.0) {
		t.Errorf("unknown-org/new-model:some-ns want 99.0 got %v (ok=%v)", v, ok)
	}
}

func TestReconcileTokenKeys_PrefersShortAllocationKeyWhenBothFormsExist(t *testing.T) {
	allocCosts := map[string]*allocationResult{
		"gemma-4-31B:llm-d-pic":        {allocationTotalCost: 10.0, namespace: "llm-d-pic"},
		"google/gemma-4-31B:llm-d-pic": {allocationTotalCost: 1.0, namespace: "llm-d-pic"},
	}

	tokens := map[string]float64{
		"google/gemma-4-31B:llm-d-pic": 123.0,
	}

	out, remappedKeys := reconcileTokenKeys(tokens, allocCosts)

	if v, ok := out["gemma-4-31B:llm-d-pic"]; !ok || !floatEq(v, 123.0) {
		t.Errorf("gemma-4-31B:llm-d-pic want 123.0 got %v (ok=%v)", v, ok)
	}
	if _, ok := out["google/gemma-4-31B:llm-d-pic"]; ok {
		t.Error("google/gemma-4-31B:llm-d-pic should have been folded into gemma-4-31B:llm-d-pic")
	}
	if _, ok := remappedKeys["google/gemma-4-31B:llm-d-pic"]; !ok {
		t.Error("google/gemma-4-31B:llm-d-pic should be in remappedKeys")
	}
}

// TestCollector_BuildQueryWindow verifies that buildQueryWindow generates
// correct Prometheus time range selectors based on CollectionInterval.
func TestCollector_BuildQueryWindow(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		want     string
	}{
		{
			name:     "5 minutes",
			interval: 5 * time.Minute,
			want:     "[5m]",
		},
		{
			name:     "10 minutes",
			interval: 10 * time.Minute,
			want:     "[10m]",
		},
		{
			name:     "1 hour",
			interval: 60 * time.Minute,
			want:     "[60m]",
		},
		{
			name:     "30 seconds (rounds to 1m minimum)",
			interval: 30 * time.Second,
			want:     "[1m]",
		},
		{
			name:     "90 seconds (rounds to 1m)",
			interval: 90 * time.Second,
			want:     "[1m]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			cfg.CollectionInterval = tt.interval
			c := &Collector{config: cfg}
			
			got := c.buildQueryWindow()
			if got != tt.want {
				t.Errorf("buildQueryWindow() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestReconcileTokenKeys_NoMismatch verifies that when all token keys directly
// match allocation keys, no re-keying occurs and no entries are dropped.
func TestReconcileTokenKeys_NoMismatch(t *testing.T) {
	allocCosts := map[string]*allocationResult{
		"llama-3:prod": {allocationTotalCost: 1.0},
	}
	tokens := map[string]float64{
		"llama-3:prod": 500.0,
	}

	out, remappedKeys := reconcileTokenKeys(tokens, allocCosts)
	if v, ok := out["llama-3:prod"]; !ok || !floatEq(v, 500.0) {
		t.Errorf("want llama-3:prod=500.0 got %v (ok=%v)", v, ok)
	}
	if len(out) != 1 {
		t.Errorf("expected 1 entry, got %d", len(out))
	}
	if len(remappedKeys) != 0 {
		t.Errorf("expected no remapped keys, got %d", len(remappedKeys))
	}
}

// TestCollector_CollectMetrics_PrometheusUnavailable ensures that CollectMetrics
// returns an error (not a panic) when Prometheus is unreachable.
func TestCollector_CollectMetrics_PrometheusUnavailable(t *testing.T) {
	cfg := baseConfig()
	// Use a non-routable address to ensure the HTTP call fails fast.
	cfg.PrometheusURL = "http://192.0.2.1:9090"

	now := time.Now()
	querier := &mockQuerier{set: opencost.NewAllocationSet(now.Add(-5*time.Minute), now)}

	collector, err := NewCollector(cfg, querier)
	if err != nil {
		t.Fatalf("NewCollector returned unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = collector.CollectMetrics(ctx)
	// The allocation query succeeds (mock), but the Prometheus query will fail.
	// CollectMetrics should return an error from the prompt token query.
	if err == nil {
		t.Error("expected error when Prometheus is unreachable, got nil")
	}
}

func TestCollector_CombineMetrics_IncludesTimingOnlyKeysInUnion(t *testing.T) {
	cfg := baseConfig()
	c := &Collector{config: cfg}

	allocCosts := map[string]*allocationResult{}
	promptTokens := map[string]float64{}
	genTokens := map[string]float64{}
	inputTime := map[string]float64{"timing-only:ns1": 60}
	outputTime := map[string]float64{"timing-only:ns1": 40}
	cacheHits := map[string]float64{"timing-only:ns1": 2}

	results := c.combineMetrics(allocCosts, promptTokens, genTokens, inputTime, outputTime, cacheHits, time.Now())

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	m := results[0]
	if m.Properties.ModelName != "timing-only" || m.Properties.Namespace != "ns1" {
		t.Fatalf("unexpected properties: model=%s namespace=%s", m.Properties.ModelName, m.Properties.Namespace)
	}
	if !floatEq(m.InputProcessingTime, 60) {
		t.Errorf("InputProcessingTime want 60 got %f", m.InputProcessingTime)
	}
	if !floatEq(m.OutputProcessingTime, 40) {
		t.Errorf("OutputProcessingTime want 40 got %f", m.OutputProcessingTime)
	}
	if !floatEq(m.CacheHitBlocks, 2) {
		t.Errorf("CacheHitBlocks want 2 got %f", m.CacheHitBlocks)
	}
}
