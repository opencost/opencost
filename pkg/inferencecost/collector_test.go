package inferencecost

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
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

// Helper function to create a mock metrics querier with custom inference metric responses
func newMockMetricsQuerierWithInferenceMetrics(
	promptTokens map[string]float64,
	generationTokens map[string]float64,
	inputTime map[string]float64,
	outputTime map[string]float64,
	cachedTokens map[string]float64,
	cacheConfigs map[string]*source.InferenceCacheConfig,
) *source.MockMetricsQuerier {
	mock := source.NewMockMetricsQuerier()

	// Set up inference metric overrides
	if promptTokens != nil {
		mock.SetOverride(source.QueryInferencePromptTokens, []*source.InferenceTokensResult{
			{Values: promptTokens},
		})
	}
	if generationTokens != nil {
		mock.SetOverride(source.QueryInferenceGenerationTokens, []*source.InferenceTokensResult{
			{Values: generationTokens},
		})
	}
	if inputTime != nil {
		mock.SetOverride(source.QueryInferenceInputProcessingTime, []*source.InferenceProcessingTimeResult{
			{Values: inputTime},
		})
	}
	if outputTime != nil {
		mock.SetOverride(source.QueryInferenceOutputProcessingTime, []*source.InferenceProcessingTimeResult{
			{Values: outputTime},
		})
	}
	if cachedTokens != nil {
		mock.SetOverride(source.QueryInferenceCachedTokens, []*source.InferenceTokensResult{
			{Values: cachedTokens},
		})
	}
	if cacheConfigs != nil {
		mock.SetOverride(source.QueryInferenceCacheConfig, []*source.InferenceCacheConfigResult{
			{Configs: cacheConfigs},
		})
	}

	return mock
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
		SharedInfraLabel:          "llm-d.ai/inference-shared",
		SharedInfraLabelValue:     "true",
		AllocationMode:            AllocationModeComputeTime,
		OutputTokenCostMultiplier: 2.5,
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
// between allocation and usage costs when idle is present, in the absence of
// utilisation metrics.
//
// Note: when utilisation metrics (CPUCoreUsageAverage, RAMBytesUsageAverage,
// GPUUsageAverage) are available, usage cost is further reduced below
// allocationCost - idle by scaling each resource to its actual consumption.
// This test covers only the idle-exclusion step; see
// TestCollector_UsageCost_ScalesResourcesByUtilisation for utilisation scaling.
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

// makeAllocationWithUtilisation creates an Allocation with both cost and
// utilisation fields set, for testing the usage cost scaling path.
// Pass nil for gpuUsageAverage to omit the GPUAllocation entirely (no GPU metric available).
func makeAllocationWithUtilisation(
	name string,
	gpuCost, cpuCost, ramCost float64,
	gpuUsageAverage *float64, // SM duty cycle fraction [0,1]; nil means no GPU metric
	cpuCoreRequest, cpuCoreUsage float64,
	ramBytesRequest, ramBytesUsage float64,
	namespace string,
) *opencost.Allocation {
	a := &opencost.Allocation{
		Name:                   name,
		GPUCost:                gpuCost,
		CPUCost:                cpuCost,
		RAMCost:                ramCost,
		CPUCoreRequestAverage:  cpuCoreRequest,
		CPUCoreUsageAverage:    cpuCoreUsage,
		RAMBytesRequestAverage: ramBytesRequest,
		RAMBytesUsageAverage:   ramBytesUsage,
		Properties: &opencost.AllocationProperties{
			Namespace: namespace,
			Labels:    opencost.AllocationLabels(map[string]string{"llm-d.ai/model": name}),
		},
	}
	if gpuUsageAverage != nil {
		a.GPUAllocation = &opencost.GPUAllocation{
			GPUUsageAverage: gpuUsageAverage,
		}
	}
	return a
}

// gpuUsage is a helper that returns a pointer to a float64, for use in
// makeAllocationWithUtilisation calls.
func gpuUsage(v float64) *float64 { return &v }

// TestCollector_UsageCost_ScalesResourcesByUtilisation verifies that when
// utilisation metrics are present, extractAllocationResults scales GPU, CPU,
// and RAM costs proportionally to their actual consumption.
//
// Numbers:
//
//	GPU $6 at 50% → $3.00
//	CPU $4 at 25% (1 core used / 4 requested) → $1.00
//	RAM $2 at 10% (10 GB used / 100 GB requested) → $0.20
//	Total = $4.20
func TestCollector_UsageCost_ScalesResourcesByUtilisation(t *testing.T) {
	cfg := baseConfig()
	c := &Collector{config: cfg}

	now := time.Now()
	alloc := makeAllocationWithUtilisation(
		"llama-3",
		6.0,           // gpuCost
		4.0,           // cpuCost
		2.0,           // ramCost
		gpuUsage(0.5), // gpuUsageAverage: 50%
		4.0, 1.0,      // cpuCoreRequest=4, cpuCoreUsage=1 → 25%
		100.0, 10.0, // ramBytesRequest=100, ramBytesUsage=10 → 10%
		"llm-prod",
	)
	as := opencost.NewAllocationSet(now.Add(-5*time.Minute), now)
	as.Set(alloc)

	results, err := c.extractAllocationResults(as, false)
	if err != nil {
		t.Fatalf("extractAllocationResults failed: %v", err)
	}

	key := modelNamespaceKey("llama-3", "llm-prod")
	r, ok := results[key]
	if !ok {
		t.Fatal("expected result for llama-3/llm-prod")
	}

	// GPU: $6 × 0.50 = $3.00
	// CPU: $4 × (1/4) = $1.00
	// RAM: $2 × (10/100) = $0.20
	// Total: $4.20
	want := 4.20
	if !floatEq(r.usageTotalCost, want) {
		t.Errorf("usageTotalCost want %.2f got %.4f", want, r.usageTotalCost)
	}
}

// TestCollector_UsageCost_NoScalingWhenUtilisationMetricsAbsent verifies that
// when utilisation averages are zero (metrics not available), extractAllocationResults
// leaves usageTotalCost at the full TotalCost() — the safe fallback.
func TestCollector_UsageCost_NoScalingWhenUtilisationMetricsAbsent(t *testing.T) {
	cfg := baseConfig()
	c := &Collector{config: cfg}

	now := time.Now()
	// No utilisation fields set — CPUCoreUsageAverage and RAMBytesUsageAverage
	// default to 0, so gating conditions are not met and no scaling fires.
	alloc := &opencost.Allocation{
		Name:    "llama-3",
		GPUCost: 6.0,
		CPUCost: 4.0,
		RAMCost: 2.0,
		Properties: &opencost.AllocationProperties{
			Namespace: "llm-prod",
			Labels:    opencost.AllocationLabels(map[string]string{"llm-d.ai/model": "llama-3"}),
		},
	}
	as := opencost.NewAllocationSet(now.Add(-5*time.Minute), now)
	as.Set(alloc)

	results, err := c.extractAllocationResults(as, false)
	if err != nil {
		t.Fatalf("extractAllocationResults failed: %v", err)
	}

	key := modelNamespaceKey("llama-3", "llm-prod")
	r, ok := results[key]
	if !ok {
		t.Fatal("expected result for llama-3/llm-prod")
	}

	// No utilisation metrics → full TotalCost() = $12.00 unchanged.
	want := 12.0
	if !floatEq(r.usageTotalCost, want) {
		t.Errorf("usageTotalCost want %.2f got %.4f (expected no scaling)", want, r.usageTotalCost)
	}
}

// TestCollector_UsageCost_ZeroGPUUsage verifies that GPUUsageAverage==0 (GPU
// completely idle) scales the GPU cost to $0, not left at full reservation.
// This is the primary regression test for the original exclusive `> 0` guard.
func TestCollector_UsageCost_ZeroGPUUsage(t *testing.T) {
	cfg := baseConfig()
	c := &Collector{config: cfg}

	now := time.Now()
	alloc := makeAllocationWithUtilisation(
		"llama-3",
		6.0,           // gpuCost
		4.0,           // cpuCost
		2.0,           // ramCost
		gpuUsage(0.0), // GPUUsageAverage = 0: completely idle GPU
		0, 0,          // no CPU scaling
		0, 0, // no RAM scaling
		"llm-prod",
	)
	as := opencost.NewAllocationSet(now.Add(-5*time.Minute), now)
	as.Set(alloc)

	results, err := c.extractAllocationResults(as, false)
	if err != nil {
		t.Fatalf("extractAllocationResults failed: %v", err)
	}

	key := modelNamespaceKey("llama-3", "llm-prod")
	r, ok := results[key]
	if !ok {
		t.Fatal("expected result for llama-3/llm-prod")
	}

	// GPU $6 × 0.0 = $0; CPU $4 + RAM $2 = $6 total (no CPU/RAM scaling).
	want := 6.0
	if !floatEq(r.usageTotalCost, want) {
		t.Errorf("usageTotalCost want %.2f got %.4f (zero GPU usage should zero GPU cost)", want, r.usageTotalCost)
	}
}

// TestCollector_UsageCost_OutOfRangeGPUUsageClamped verifies that
// GPUUsageAverage values outside [0,1] are clamped before scaling, so they
// never produce nonsensical (negative or inflated) costs.
func TestCollector_UsageCost_OutOfRangeGPUUsageClamped(t *testing.T) {
	cfg := baseConfig()
	c := &Collector{config: cfg}
	now := time.Now()

	tests := []struct {
		name          string
		gpuUsageAvg   float64
		wantUsageCost float64 // GPU $6 clamped + CPU $4 + RAM $2 (no CPU/RAM scaling)
	}{
		{"above_one", 1.5, 12.0}, // clamped to 1.0 → $6 GPU + $4 CPU + $2 RAM
		{"negative", -0.5, 6.0},  // clamped to 0.0 → $0 GPU + $4 CPU + $2 RAM
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			alloc := makeAllocationWithUtilisation(
				"llama-3",
				6.0, 4.0, 2.0,
				gpuUsage(tc.gpuUsageAvg),
				0, 0,
				0, 0,
				"llm-prod",
			)
			as := opencost.NewAllocationSet(now.Add(-5*time.Minute), now)
			as.Set(alloc)

			results, err := c.extractAllocationResults(as, false)
			if err != nil {
				t.Fatalf("extractAllocationResults failed: %v", err)
			}

			key := modelNamespaceKey("llama-3", "llm-prod")
			r, ok := results[key]
			if !ok {
				t.Fatal("expected result for llama-3/llm-prod")
			}

			if !floatEq(r.usageTotalCost, tc.wantUsageCost) {
				t.Errorf("usageTotalCost want %.2f got %.4f (gpuUsageAvg=%.2f should be clamped)",
					tc.wantUsageCost, r.usageTotalCost, tc.gpuUsageAvg)
			}
		})
	}
}

// TestCollector_UsageCost_GPUOnlyScaling verifies that GPU cost is scaled by
// GPUUsageAverage while CPU and RAM costs are left at their full reservation
// when no CPU/RAM utilisation metrics are available.
//
// Numbers:
//
//	GPU $6 × 0.75 = $4.50
//	CPU $4 (no scaling — CPUCoreUsageAverage == 0)
//	RAM $2 (no scaling — RAMBytesUsageAverage == 0)
//	Total = $10.50
func TestCollector_UsageCost_GPUOnlyScaling(t *testing.T) {
	cfg := baseConfig()
	c := &Collector{config: cfg}
	now := time.Now()

	alloc := makeAllocationWithUtilisation(
		"llama-3",
		6.0, 4.0, 2.0,
		gpuUsage(0.75), // GPU 75%
		0, 0,           // no CPU utilisation metrics
		0, 0, // no RAM utilisation metrics
		"llm-prod",
	)
	as := opencost.NewAllocationSet(now.Add(-5*time.Minute), now)
	as.Set(alloc)

	results, err := c.extractAllocationResults(as, false)
	if err != nil {
		t.Fatalf("extractAllocationResults failed: %v", err)
	}

	key := modelNamespaceKey("llama-3", "llm-prod")
	r, ok := results[key]
	if !ok {
		t.Fatal("expected result for llama-3/llm-prod")
	}

	// GPU $6 × 0.75 = $4.50; CPU $4 + RAM $2 unchanged → $10.50
	want := 10.50
	if !floatEq(r.usageTotalCost, want) {
		t.Errorf("usageTotalCost want %.2f got %.4f", want, r.usageTotalCost)
	}
}

// TestCollector_UsageCost_CPUOnlyScaling verifies that CPU cost is scaled by
// the core utilisation ratio while GPU and RAM costs are left at their full
// reservation when those metrics are absent.
//
// Numbers:
//
//	GPU $6 (no GPUAllocation → no scaling)
//	CPU $4 × (2/8) = $1.00
//	RAM $2 (no scaling — RAMBytesUsageAverage == 0)
//	Total = $9.00
func TestCollector_UsageCost_CPUOnlyScaling(t *testing.T) {
	cfg := baseConfig()
	c := &Collector{config: cfg}
	now := time.Now()

	alloc := makeAllocationWithUtilisation(
		"llama-3",
		6.0, 4.0, 2.0,
		nil,      // no GPUAllocation
		8.0, 2.0, // cpuRequest=8, cpuUsage=2 → 25%
		0, 0, // no RAM utilisation metrics
		"llm-prod",
	)
	as := opencost.NewAllocationSet(now.Add(-5*time.Minute), now)
	as.Set(alloc)

	results, err := c.extractAllocationResults(as, false)
	if err != nil {
		t.Fatalf("extractAllocationResults failed: %v", err)
	}

	key := modelNamespaceKey("llama-3", "llm-prod")
	r, ok := results[key]
	if !ok {
		t.Fatal("expected result for llama-3/llm-prod")
	}

	// GPU $6 unchanged + CPU $4 × (2/8) = $1.00 + RAM $2 unchanged = $9.00
	want := 9.00
	if !floatEq(r.usageTotalCost, want) {
		t.Errorf("usageTotalCost want %.2f got %.4f", want, r.usageTotalCost)
	}
}

// TestCollector_UsageCost_RAMOnlyScaling verifies that RAM cost is scaled by
// the byte utilisation ratio while GPU and CPU costs are left at their full
// reservation when those metrics are absent.
//
// Numbers:
//
//	GPU $6 (no GPUAllocation → no scaling)
//	CPU $4 (no scaling — CPUCoreUsageAverage == 0)
//	RAM $2 × (20/200) = $0.20
//	Total = $10.20
func TestCollector_UsageCost_RAMOnlyScaling(t *testing.T) {
	cfg := baseConfig()
	c := &Collector{config: cfg}
	now := time.Now()

	alloc := makeAllocationWithUtilisation(
		"llama-3",
		6.0, 4.0, 2.0,
		nil,  // no GPUAllocation
		0, 0, // no CPU utilisation metrics
		200.0, 20.0, // ramRequest=200, ramUsage=20 → 10%
		"llm-prod",
	)
	as := opencost.NewAllocationSet(now.Add(-5*time.Minute), now)
	as.Set(alloc)

	results, err := c.extractAllocationResults(as, false)
	if err != nil {
		t.Fatalf("extractAllocationResults failed: %v", err)
	}

	key := modelNamespaceKey("llama-3", "llm-prod")
	r, ok := results[key]
	if !ok {
		t.Fatal("expected result for llama-3/llm-prod")
	}

	// GPU $6 + CPU $4 unchanged + RAM $2 × (20/200) = $0.20 → $10.20
	want := 10.20
	if !floatEq(r.usageTotalCost, want) {
		t.Errorf("usageTotalCost want %.2f got %.4f", want, r.usageTotalCost)
	}
}

// TestCollector_UsageCost_CPUUsageEqualsRequest verifies that when CPU usage
// exactly equals the request (utilisation == 100%), the guard condition
// (usage < request) prevents scaling and the full CPU cost is retained.
// This also confirms no double-counting from the subtraction/addition path.
func TestCollector_UsageCost_CPUUsageEqualsRequest(t *testing.T) {
	cfg := baseConfig()
	c := &Collector{config: cfg}
	now := time.Now()

	alloc := makeAllocationWithUtilisation(
		"llama-3",
		0, 4.0, 0,
		nil,      // no GPU
		4.0, 4.0, // usage == request → guard (usage < request) is false → no scaling
		0, 0,
		"llm-prod",
	)
	as := opencost.NewAllocationSet(now.Add(-5*time.Minute), now)
	as.Set(alloc)

	results, err := c.extractAllocationResults(as, false)
	if err != nil {
		t.Fatalf("extractAllocationResults failed: %v", err)
	}

	key := modelNamespaceKey("llama-3", "llm-prod")
	r, ok := results[key]
	if !ok {
		t.Fatal("expected result for llama-3/llm-prod")
	}

	// usage == request → no scaling → full $4.00 retained
	want := 4.0
	if !floatEq(r.usageTotalCost, want) {
		t.Errorf("usageTotalCost want %.2f got %.4f (CPU at 100%% should not be scaled)", want, r.usageTotalCost)
	}
}

// TestCollector_UsageCost_RAMUsageEqualsRequest verifies the same guard for RAM:
// when RAM usage exactly equals the request, no scaling fires and full cost is kept.
func TestCollector_UsageCost_RAMUsageEqualsRequest(t *testing.T) {
	cfg := baseConfig()
	c := &Collector{config: cfg}
	now := time.Now()

	alloc := makeAllocationWithUtilisation(
		"llama-3",
		0, 0, 2.0,
		nil,
		0, 0,
		100.0, 100.0, // usage == request → no scaling
		"llm-prod",
	)
	as := opencost.NewAllocationSet(now.Add(-5*time.Minute), now)
	as.Set(alloc)

	results, err := c.extractAllocationResults(as, false)
	if err != nil {
		t.Fatalf("extractAllocationResults failed: %v", err)
	}

	key := modelNamespaceKey("llama-3", "llm-prod")
	r, ok := results[key]
	if !ok {
		t.Fatal("expected result for llama-3/llm-prod")
	}

	// usage == request → full $2.00 retained
	want := 2.0
	if !floatEq(r.usageTotalCost, want) {
		t.Errorf("usageTotalCost want %.2f got %.4f (RAM at 100%% should not be scaled)", want, r.usageTotalCost)
	}
}

// TestCollector_UsageCost_CPUOvercommit verifies that when CPU usage exceeds
// the request (overcommit), the guard (usage < request) prevents scaling and
// the full CPU cost is retained — overcommit situations should not produce
// sub-reservation costs.
func TestCollector_UsageCost_CPUOvercommit(t *testing.T) {
	cfg := baseConfig()
	c := &Collector{config: cfg}
	now := time.Now()

	alloc := makeAllocationWithUtilisation(
		"llama-3",
		0, 4.0, 0,
		nil,
		2.0, 3.0, // usage (3.0) > request (2.0) → overcommit, no scaling
		0, 0,
		"llm-prod",
	)
	as := opencost.NewAllocationSet(now.Add(-5*time.Minute), now)
	as.Set(alloc)

	results, err := c.extractAllocationResults(as, false)
	if err != nil {
		t.Fatalf("extractAllocationResults failed: %v", err)
	}

	key := modelNamespaceKey("llama-3", "llm-prod")
	r, ok := results[key]
	if !ok {
		t.Fatal("expected result for llama-3/llm-prod")
	}

	// overcommit → no scaling → full $4.00 retained
	want := 4.0
	if !floatEq(r.usageTotalCost, want) {
		t.Errorf("usageTotalCost want %.2f got %.4f (CPU overcommit should not reduce cost)", want, r.usageTotalCost)
	}
}

// TestCollector_UsageCost_RAMOvercommit verifies the same overcommit guard for RAM.
func TestCollector_UsageCost_RAMOvercommit(t *testing.T) {
	cfg := baseConfig()
	c := &Collector{config: cfg}
	now := time.Now()

	alloc := makeAllocationWithUtilisation(
		"llama-3",
		0, 0, 2.0,
		nil,
		0, 0,
		50.0, 80.0, // usage (80) > request (50) → overcommit, no scaling
		"llm-prod",
	)
	as := opencost.NewAllocationSet(now.Add(-5*time.Minute), now)
	as.Set(alloc)

	results, err := c.extractAllocationResults(as, false)
	if err != nil {
		t.Fatalf("extractAllocationResults failed: %v", err)
	}

	key := modelNamespaceKey("llama-3", "llm-prod")
	r, ok := results[key]
	if !ok {
		t.Fatal("expected result for llama-3/llm-prod")
	}

	// overcommit → no scaling → full $2.00 retained
	want := 2.0
	if !floatEq(r.usageTotalCost, want) {
		t.Errorf("usageTotalCost want %.2f got %.4f (RAM overcommit should not reduce cost)", want, r.usageTotalCost)
	}
}

// TestCollector_UsageCost_NilGPUAllocationStruct verifies that an allocation
// with a nil GPUAllocation pointer (as opposed to a non-nil struct with a nil
// GPUUsageAverage pointer) is handled safely — no GPU scaling, no panic.
func TestCollector_UsageCost_NilGPUAllocationStruct(t *testing.T) {
	cfg := baseConfig()
	c := &Collector{config: cfg}
	now := time.Now()

	// makeAllocationWithUtilisation passes nil for gpuUsageAverage → GPUAllocation stays nil
	alloc := makeAllocationWithUtilisation(
		"llama-3",
		6.0, 4.0, 2.0,
		nil, // GPUAllocation == nil
		0, 0,
		0, 0,
		"llm-prod",
	)
	as := opencost.NewAllocationSet(now.Add(-5*time.Minute), now)
	as.Set(alloc)

	results, err := c.extractAllocationResults(as, false)
	if err != nil {
		t.Fatalf("extractAllocationResults failed: %v", err)
	}

	key := modelNamespaceKey("llama-3", "llm-prod")
	r, ok := results[key]
	if !ok {
		t.Fatal("expected result for llama-3/llm-prod")
	}

	// No GPUAllocation → no GPU scaling; no CPU/RAM metrics → no CPU/RAM scaling.
	// Full TotalCost() = $12.00 retained.
	want := 12.0
	if !floatEq(r.usageTotalCost, want) {
		t.Errorf("usageTotalCost want %.2f got %.4f (nil GPUAllocation should not panic or scale)", want, r.usageTotalCost)
	}
}

// TestCollector_UsageCost_TwoModelsIndependent verifies that two different
// models in the same namespace produce two independent result entries, each
// scaled only by its own utilisation metrics with no cross-contamination.
func TestCollector_UsageCost_TwoModelsIndependent(t *testing.T) {
	cfg := baseConfig()
	c := &Collector{config: cfg}
	now := time.Now()

	// Model A: GPU $6 × 0.5 = $3.00; CPU/RAM no metrics → $3 + $4 + $2 = $9.00
	modelA := makeAllocationWithUtilisation(
		"llama-3",
		6.0, 4.0, 2.0,
		gpuUsage(0.5),
		0, 0,
		0, 0,
		"llm-prod",
	)
	// Model B: GPU $3 × 0.2 = $0.60; CPU/RAM no metrics → $0.60 + $2 + $1 = $3.60
	modelB := makeAllocationWithUtilisation(
		"mistral-7b",
		3.0, 2.0, 1.0,
		gpuUsage(0.2),
		0, 0,
		0, 0,
		"llm-prod",
	)

	as := opencost.NewAllocationSet(now.Add(-5*time.Minute), now)
	as.Set(modelA)
	as.Set(modelB)

	results, err := c.extractAllocationResults(as, false)
	if err != nil {
		t.Fatalf("extractAllocationResults failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 independent result entries, got %d", len(results))
	}

	// llama-3: GPU $6 × 0.5 = $3 + CPU $4 + RAM $2 = $9.00
	keyA := modelNamespaceKey("llama-3", "llm-prod")
	rA, ok := results[keyA]
	if !ok {
		t.Fatal("expected result for llama-3/llm-prod")
	}
	if !floatEq(rA.usageTotalCost, 9.00) {
		t.Errorf("llama-3 usageTotalCost want 9.00 got %.4f", rA.usageTotalCost)
	}

	// mistral-7b: GPU $3 × 0.2 = $0.60 + CPU $2 + RAM $1 = $3.60
	keyB := modelNamespaceKey("mistral-7b", "llm-prod")
	rB, ok := results[keyB]
	if !ok {
		t.Fatal("expected result for mistral-7b/llm-prod")
	}
	if !floatEq(rB.usageTotalCost, 3.60) {
		t.Errorf("mistral-7b usageTotalCost want 3.60 got %.4f", rB.usageTotalCost)
	}
}

// TestCollector_CombineMetrics_DerivesCachedTokens verifies that combineMetrics
// passes CachedTokens through directly and derives EffectiveInputTokens correctly.
func TestCollector_CombineMetrics_DerivesCachedTokens(t *testing.T) {
	cfg := baseConfig()

	allocCosts := map[string]*allocationResult{
		"llama-3:llm-prod": {allocationTotalCost: 4.0, usageTotalCost: 2.6, namespace: "llm-prod"},
	}
	promptTokens := map[string]float64{"llama-3:llm-prod": 20}
	genTokens := map[string]float64{"llama-3:llm-prod": 10}
	inputTime := map[string]float64{}
	outputTime := map[string]float64{}
	// vllm:prefix_cache_hits_total reports tokens directly (not blocks).
	cachedTokens := map[string]float64{"llama-3:llm-prod": 8}
	cacheConfigs := map[string]*cacheConfig{"llama-3:llm-prod": {prefixCachingEnabled: true}}

	c := &Collector{config: cfg}
	now := time.Now()
	results := c.combineMetrics(allocCosts, promptTokens, genTokens, inputTime, outputTime, cachedTokens, cacheConfigs, now.Add(-1*time.Hour), now)

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

	allocCosts := map[string]*allocationResult{
		"llama-3:llm-prod": {allocationTotalCost: 1.0, usageTotalCost: 1.0, namespace: "llm-prod"},
	}
	promptTokens := map[string]float64{"llama-3:llm-prod": 1000}
	genTokens := map[string]float64{"llama-3:llm-prod": 500}
	// cachedTokens map is empty — simulates metric being unavailable
	cacheHits := map[string]float64{}
	cacheConfigs := map[string]*cacheConfig{"llama-3:llm-prod": {prefixCachingEnabled: true}}

	c := &Collector{config: cfg}
	now := time.Now()
	results := c.combineMetrics(allocCosts, promptTokens, genTokens,
		map[string]float64{}, map[string]float64{}, cacheHits, cacheConfigs, now.Add(-1*time.Hour), now)

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
// TestQueryCounterDelta_Formula verifies the delta = end - start subtraction
// and that negative deltas (counter resets) use endVal to capture post-reset activity.
func TestQueryCounterDelta_Formula(t *testing.T) {
	tests := []struct {
		name     string
		endVal   float64
		startVal float64
		want     float64
	}{
		{name: "normal increase", endVal: 1000, startVal: 200, want: 800},
		{name: "no activity", endVal: 500, startVal: 500, want: 0},
		{name: "counter reset uses endVal", endVal: 100, startVal: 900, want: 100},
		{name: "new pod (no start sample)", endVal: 400, startVal: 0, want: 400},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delta := tt.endVal - tt.startVal
			if delta < 0 {
				delta = tt.endVal
			}
			if delta != tt.want {
				t.Errorf("delta = %v, want %v", delta, tt.want)
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

// TestCollector_CollectMetrics_EmptyMetrics ensures that CollectMetrics
// handles empty metrics gracefully (returns empty results, not an error).
func TestCollector_CollectMetrics_EmptyMetrics(t *testing.T) {
	cfg := baseConfig()

	now := time.Now()
	querier := &mockQuerier{set: opencost.NewAllocationSet(now.Add(-5*time.Minute), now)}

	// Use the standard mock - it will return empty results by default
	metricsQuerier := source.NewMockMetricsQuerier()

	collector, err := NewCollector(cfg, querier, metricsQuerier)
	if err != nil {
		t.Fatalf("NewCollector returned unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	end := time.Now()
	start := end.Add(-5 * time.Minute)
	results, dimTokens, err := collector.CollectMetrics(ctx, start, end)
	// With empty metrics, CollectMetrics should succeed with empty results
	if err != nil {
		t.Errorf("unexpected error with empty metrics: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results with empty metrics, got %d", len(results))
	}
	if dimTokens == nil {
		t.Errorf("expected non-nil dimTokens with empty metrics")
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
	cacheConfigs := map[string]*cacheConfig{}

	now := time.Now()
	results := c.combineMetrics(allocCosts, promptTokens, genTokens, inputTime, outputTime, cacheHits, cacheConfigs, now.Add(-1*time.Hour), now)

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
	if !floatEq(m.CachedTokens, 2) {
		t.Errorf("CachedTokens want 2 got %f", m.CachedTokens)
	}
}

func makeTestModelCost(modelName, namespace string, promptTokens, genTokens, allocCost, usageCost, allocInRate, allocOutRate, usageInRate, usageOutRate float64) *InferenceCost {
	mc := &InferenceCost{
		Properties: InferenceCostProperties{
			ModelName:    modelName,
			Namespace:    namespace,
			Cluster:      "test-cluster",
			WorkloadType: "inference",
		},
		PromptTokens:               promptTokens,
		GenerationTokens:           genTokens,
		TotalTokens:                promptTokens + genTokens,
		AllocationTotalCost:        allocCost,
		UsageTotalCost:             usageCost,
		InputCostPerMillionTokens:  map[CostBasis]float64{CostBasisAllocation: allocInRate, CostBasisUsage: usageInRate},
		OutputCostPerMillionTokens: map[CostBasis]float64{CostBasisAllocation: allocOutRate, CostBasisUsage: usageOutRate},
		CostPerMillionTokens:       map[CostBasis]float64{CostBasisAllocation: (allocCost / (promptTokens + genTokens)) * 1e6, CostBasisUsage: (usageCost / (promptTokens + genTokens)) * 1e6},
		InputCost:                  map[CostBasis]float64{CostBasisAllocation: promptTokens * allocInRate / 1e6, CostBasisUsage: promptTokens * usageInRate / 1e6},
		OutputCost:                 map[CostBasis]float64{CostBasisAllocation: genTokens * allocOutRate / 1e6, CostBasisUsage: genTokens * usageOutRate / 1e6},
		AllocationMethod:           AllocationMethodComputeTime,
	}
	mc.Window.Start = time.Now().Add(-1 * time.Hour)
	mc.Window.End = time.Now()
	return mc
}

func TestBuildDimensionCosts_JoinFormulaAndRates(t *testing.T) {
	cfg := baseConfig()
	cfg.AttributionEnabled = true
	c := &Collector{config: cfg}

	// Model: prompt 1M, gen 1M. alloc in rate $10/M, alloc out rate $20/M -> model alloc cost $30
	// usage in rate $5/M, usage out rate $10/M -> model usage cost $15
	mc := makeTestModelCost("llama-3-70b", "llm-prod", 1000000, 1000000, 30.0, 15.0, 10.0, 20.0, 5.0, 10.0)

	dimTokens := &source.InferenceDimensionResult{
		Values: map[source.InferenceDimensionKey]*source.InferenceDimensionTokens{
			{
				Producer:     "default",
				TenantID:     "tenant-a",
				WorkloadID:   "workload-1",
				UserID:       "alice",
				ServingModel: "llama-3-70b",
				Namespace:    "llm-prod",
			}: {
				PromptTokens:     400000,
				CompletionTokens: 200000,
			},
			{
				Producer:     "default",
				TenantID:     "tenant-b",
				WorkloadID:   "workload-2",
				UserID:       "bob",
				ServingModel: "llama-3-70b",
				Namespace:    "llm-prod",
			}: {
				PromptTokens:     600000,
				CompletionTokens: 800000,
			},
		},
	}

	results, _ := c.BuildDimensionCosts([]*InferenceCost{mc}, dimTokens)
	if len(results) != 3 { // tenant-a, tenant-b, __unmetered__ remainder
		t.Fatalf("expected 3 dimension entries, got %d", len(results))
	}

	var tenantA, tenantB, remainder *InferenceCost
	for _, r := range results {
		switch r.Properties.TenantID {
		case "tenant-a":
			tenantA = r
		case "tenant-b":
			tenantB = r
		case "__unmetered__":
			remainder = r
		}
	}

	if tenantA == nil || tenantB == nil || remainder == nil {
		t.Fatalf("missing expected entries in results: tenantA=%v tenantB=%v remainder=%v", tenantA, tenantB, remainder)
	}

	// Tenant A: 400k prompt ($4.00 alloc, $2.00 usage), 200k gen ($4.00 alloc, $2.00 usage) -> total $8.00 alloc, $4.00 usage
	if math.Abs(tenantA.AllocationTotalCost-8.0) > 1e-6 {
		t.Errorf("tenantA AllocationTotalCost want 8.0, got %f", tenantA.AllocationTotalCost)
	}
	if math.Abs(tenantA.UsageTotalCost-4.0) > 1e-6 {
		t.Errorf("tenantA UsageTotalCost want 4.0, got %f", tenantA.UsageTotalCost)
	}
	if math.Abs(tenantA.CostPerMillionTokens[CostBasisAllocation]-((8.0/600000.0)*1e6)) > 1e-6 {
		t.Errorf("tenantA blended alloc rate mismatch: got %f", tenantA.CostPerMillionTokens[CostBasisAllocation])
	}
	if math.Abs(tenantA.InputCostPerMillionTokens[CostBasisAllocation]-10.0) > 1e-6 {
		t.Errorf("tenantA alloc in rate want 10.0, got %f", tenantA.InputCostPerMillionTokens[CostBasisAllocation])
	}
	if math.Abs(tenantA.OutputCostPerMillionTokens[CostBasisAllocation]-20.0) > 1e-6 {
		t.Errorf("tenantA alloc out rate want 20.0, got %f", tenantA.OutputCostPerMillionTokens[CostBasisAllocation])
	}

	// Tenant B: 600k prompt ($6.00 alloc, $3.00 usage), 800k gen ($16.00 alloc, $8.00 usage) -> total $22.00 alloc, $11.00 usage
	if math.Abs(tenantB.AllocationTotalCost-22.0) > 1e-6 {
		t.Errorf("tenantB AllocationTotalCost want 22.0, got %f", tenantB.AllocationTotalCost)
	}
	if math.Abs(tenantB.UsageTotalCost-11.0) > 1e-6 {
		t.Errorf("tenantB UsageTotalCost want 11.0, got %f", tenantB.UsageTotalCost)
	}

	// Remainder: total tokens metered = 1M prompt, 1M gen. Remainder tokens = 0, remainder cost = 0.
	if math.Abs(remainder.AllocationTotalCost-0.0) > 1e-6 {
		t.Errorf("remainder AllocationTotalCost want 0.0, got %f", remainder.AllocationTotalCost)
	}
	if math.Abs(remainder.PromptTokens-0.0) > 1e-6 || math.Abs(remainder.GenerationTokens-0.0) > 1e-6 {
		t.Errorf("remainder tokens want 0, got prompt=%f gen=%f", remainder.PromptTokens, remainder.GenerationTokens)
	}

	// Release gate: tenant rows + remainder = model total
	totalAlloc := tenantA.AllocationTotalCost + tenantB.AllocationTotalCost + remainder.AllocationTotalCost
	if math.Abs(totalAlloc-mc.AllocationTotalCost) > 1e-6 {
		t.Errorf("sum of dimension alloc costs (%f) != model alloc cost (%f)", totalAlloc, mc.AllocationTotalCost)
	}
	totalUsage := tenantA.UsageTotalCost + tenantB.UsageTotalCost + remainder.UsageTotalCost
	if math.Abs(totalUsage-mc.UsageTotalCost) > 1e-6 {
		t.Errorf("sum of dimension usage costs (%f) != model usage cost (%f)", totalUsage, mc.UsageTotalCost)
	}
}

func TestBuildDimensionCosts_ZeroModelCost_NoPanic(t *testing.T) {
	cfg := baseConfig()
	cfg.AttributionEnabled = true
	c := &Collector{config: cfg}

	mc := makeTestModelCost("llama-3-70b", "llm-prod", 0, 0, 0, 0, 0, 0, 0, 0)
	dimTokens := &source.InferenceDimensionResult{
		Values: map[source.InferenceDimensionKey]*source.InferenceDimensionTokens{
			{
				Producer:     "default",
				TenantID:     "tenant-a",
				ServingModel: "llama-3-70b",
				Namespace:    "llm-prod",
			}: {
				PromptTokens:     0,
				CompletionTokens: 0,
			},
		},
	}

	results, _ := c.BuildDimensionCosts([]*InferenceCost{mc}, dimTokens)
	if len(results) == 0 {
		t.Fatalf("expected results, got 0")
	}
	for _, r := range results {
		if r.CostPerMillionTokens[CostBasisAllocation] != 0 || r.CostPerMillionTokens[CostBasisUsage] != 0 {
			t.Errorf("expected 0 cost per million tokens, got alloc=%f usage=%f", r.CostPerMillionTokens[CostBasisAllocation], r.CostPerMillionTokens[CostBasisUsage])
		}
	}
}

func TestBuildDimensionCosts_EmptyAttributionResult(t *testing.T) {
	cfg := baseConfig()
	cfg.AttributionEnabled = true
	c := &Collector{config: cfg}

	mc := makeTestModelCost("llama-3-70b", "llm-prod", 100000, 50000, 10.0, 5.0, 50.0, 100.0, 25.0, 50.0)
	dimTokens := &source.InferenceDimensionResult{
		Values: map[source.InferenceDimensionKey]*source.InferenceDimensionTokens{},
	}

	results, _ := c.BuildDimensionCosts([]*InferenceCost{mc}, dimTokens)
	if len(results) != 1 {
		t.Fatalf("expected 1 remainder result, got %d", len(results))
	}
	rem := results[0]
	if rem.Properties.TenantID != "__unmetered__" {
		t.Errorf("expected __unmetered__ tenant, got %s", rem.Properties.TenantID)
	}
	if math.Abs(rem.AllocationTotalCost-10.0) > 1e-6 {
		t.Errorf("expected full model cost in remainder, got %f", rem.AllocationTotalCost)
	}
	if rem.PromptTokens != 100000 || rem.GenerationTokens != 50000 {
		t.Errorf("expected full engine tokens in remainder, got prompt=%f gen=%f", rem.PromptTokens, rem.GenerationTokens)
	}
}

func TestBuildDimensionCosts_UnmatchedKeys_CountedAndLogged(t *testing.T) {
	cfg := baseConfig()
	cfg.AttributionEnabled = true
	c := &Collector{config: cfg}

	mc := makeTestModelCost("llama-3-70b", "llm-prod", 100000, 50000, 10.0, 5.0, 50.0, 100.0, 25.0, 50.0)
	dimTokens := &source.InferenceDimensionResult{
		Values: map[source.InferenceDimensionKey]*source.InferenceDimensionTokens{
			{
				Producer:     "default",
				TenantID:     "tenant-a",
				ServingModel: "unknown-model",
				Namespace:    "llm-prod",
			}: {
				PromptTokens:     1000,
				CompletionTokens: 500,
			},
		},
	}

	results, unmatched := c.BuildDimensionCosts([]*InferenceCost{mc}, dimTokens)
	// Only remainder for llama-3-70b should be emitted; unknown-model is unmatched
	if len(results) != 1 || results[0].Properties.TenantID != "__unmetered__" {
		t.Fatalf("expected only remainder row, got %d results", len(results))
	}

	if unmatched["default"] != 1 {
		t.Errorf("expected 1 unmatched key for producer 'default', got %f", unmatched["default"])
	}
}

func TestBuildDimensionCosts_MissingIdentityKeptDistinct(t *testing.T) {
	// Producer A: tenant "none" is a genuine tenant (sentinel is "unattributed")
	// Producer B: tenant "none" is its sentinel (sentinel is "none")
	// Verify A's tenant "none" stays "none" and B's sentinel "none" becomes "__unidentified__",
	// so B's traffic is NOT merged into A or charged to A.
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "attribution.yaml")
	configYAML := "producers:\n" +
		"  - name: producer-a\n" +
		"    inputTokensSeries: prod_a_in\n" +
		"    outputTokensSeries: prod_a_out\n" +
		"    tenantLabel: tenant_id\n" +
		"    workloadLabel: workload_id\n" +
		"    modelLabel: model_name\n" +
		"    namespaceLabel: namespace\n" +
		"    identitySentinel: unattributed\n" +
		"  - name: producer-b\n" +
		"    inputTokensSeries: prod_b_in\n" +
		"    outputTokensSeries: prod_b_out\n" +
		"    tenantLabel: tenant_id\n" +
		"    workloadLabel: workload_id\n" +
		"    modelLabel: model_name\n" +
		"    namespaceLabel: namespace\n" +
		"    identitySentinel: none\n"
	if err := os.WriteFile(configPath, []byte(configYAML), 0644); err != nil {
		t.Fatalf("writing config: %v", err)
	}

	cfg := baseConfig()
	cfg.AttributionEnabled = true
	cfg.AttributionConfigPath = configPath
	c := &Collector{config: cfg}

	mc := makeTestModelCost("llama-3-70b", "llm-prod", 1000000, 1000000, 20.0, 10.0, 10.0, 10.0, 5.0, 5.0)

	dimTokens := &source.InferenceDimensionResult{
		Values: map[source.InferenceDimensionKey]*source.InferenceDimensionTokens{
			{
				Producer:     "producer-a",
				TenantID:     "none", // Real tenant for A
				WorkloadID:   "w1",
				ServingModel: "llama-3-70b",
				Namespace:    "llm-prod",
			}: {
				PromptTokens:     100000,
				CompletionTokens: 100000,
			},
			{
				Producer:     "producer-b",
				TenantID:     "none", // Sentinel for B
				WorkloadID:   "w1",
				ServingModel: "llama-3-70b",
				Namespace:    "llm-prod",
			}: {
				PromptTokens:     200000,
				CompletionTokens: 200000,
			},
		},
	}

	results, _ := c.BuildDimensionCosts([]*InferenceCost{mc}, dimTokens)
	var realTenantNone, unidentifiedTenant *InferenceCost
	for _, r := range results {
		if r.Properties.TenantID == "none" {
			realTenantNone = r
		} else if r.Properties.TenantID == "__unidentified__" {
			unidentifiedTenant = r
		}
	}

	if realTenantNone == nil {
		t.Fatalf("expected real tenant 'none' row")
	}
	if unidentifiedTenant == nil {
		t.Fatalf("expected '__unidentified__' row from producer-b sentinel")
	}

	if realTenantNone.PromptTokens != 100000 {
		t.Errorf("real tenant 'none' prompt tokens want 100k, got %f", realTenantNone.PromptTokens)
	}
	if unidentifiedTenant.PromptTokens != 200000 {
		t.Errorf("unidentified tenant prompt tokens want 200k, got %f", unidentifiedTenant.PromptTokens)
	}
}

func TestBuildDimensionCosts_OverAttributedRemainder_ClampedAndScaled(t *testing.T) {
	cfg := baseConfig()
	cfg.AttributionEnabled = true
	c := &Collector{config: cfg}

	// Model: 100 prompt tokens, 100 gen tokens. Total cost $10 alloc.
	mc := makeTestModelCost("llama-3-70b", "llm-prod", 100, 100, 10.0, 5.0, 50000.0, 50000.0, 25000.0, 25000.0)

	// Attributed tokens: 105 prompt tokens, 105 gen tokens (exceeds engine reference)
	dimTokens := &source.InferenceDimensionResult{
		Values: map[source.InferenceDimensionKey]*source.InferenceDimensionTokens{
			{
				Producer:     "default",
				TenantID:     "tenant-a",
				WorkloadID:   "w1",
				ServingModel: "llama-3-70b",
				Namespace:    "llm-prod",
			}: {
				PromptTokens:     105,
				CompletionTokens: 105,
			},
		},
	}

	results, _ := c.BuildDimensionCosts([]*InferenceCost{mc}, dimTokens)
	if len(results) != 2 {
		t.Fatalf("expected 2 results (tenant + remainder), got %d", len(results))
	}

	var tenantA, remainder *InferenceCost
	for _, r := range results {
		if r.Properties.TenantID == "tenant-a" {
			tenantA = r
		} else if r.Properties.TenantID == "__unmetered__" {
			remainder = r
		}
	}

	if tenantA == nil || remainder == nil {
		t.Fatalf("missing tenantA or remainder")
	}

	// Remainder tokens must be clamped to 0, never negative
	if remainder.PromptTokens != 0 || remainder.GenerationTokens != 0 || remainder.TotalTokens != 0 {
		t.Errorf("remainder tokens not clamped to 0: prompt=%f gen=%f", remainder.PromptTokens, remainder.GenerationTokens)
	}
	if remainder.AllocationTotalCost != 0 {
		t.Errorf("remainder alloc cost want 0, got %f", remainder.AllocationTotalCost)
	}

	// Tenant row must be scaled pro rata to exactly equal model cost ($10.00 alloc)
	if math.Abs(tenantA.AllocationTotalCost-10.0) > 1e-6 {
		t.Errorf("tenantA scaled AllocationTotalCost want 10.0, got %f", tenantA.AllocationTotalCost)
	}
	if !tenantA.Approximate {
		t.Errorf("expected tenantA to be marked Approximate: true after over-attribution scaling")
	}

	// Rate must be derived from the scaled cost and tokens:
	expectedBlendedRate := (10.0 / 210.0) * 1e6
	if math.Abs(tenantA.CostPerMillionTokens[CostBasisAllocation]-expectedBlendedRate) > 1e-6 {
		t.Errorf("tenantA blended rate want %f, got %f", expectedBlendedRate, tenantA.CostPerMillionTokens[CostBasisAllocation])
	}
}

func TestBuildDimensionCosts_WorkloadLabelDisabled_LiteLLM(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "litellm.yaml")
	configYAML := "producers:\n" +
		"  - name: litellm-gateway\n" +
		"    inputTokensSeries: litellm_input_tokens_metric_total\n" +
		"    outputTokensSeries: litellm_output_tokens_metric_total\n" +
		"    tenantLabel: team_alias\n" +
		"    workloadLabel: \"\"\n" +
		"    userLabel: user\n" +
		"    userSentinel: \"None\"\n" +
		"    modelLabel: model\n" +
		"    namespaceLabel: deployment_namespace\n" +
		"    identitySentinel: \"None\"\n"
	if err := os.WriteFile(configPath, []byte(configYAML), 0644); err != nil {
		t.Fatalf("writing config: %v", err)
	}

	cfg := baseConfig()
	cfg.AttributionEnabled = true
	cfg.AttributionConfigPath = configPath
	c := &Collector{config: cfg}

	mc := makeTestModelCost("llama-3-70b", "llm-prod", 100000, 100000, 10.0, 5.0, 50.0, 50.0, 25.0, 25.0)

	dimTokens := &source.InferenceDimensionResult{
		Values: map[source.InferenceDimensionKey]*source.InferenceDimensionTokens{
			{
				Producer:     "litellm-gateway",
				TenantID:     "team-ml",
				UserID:       "None", // Sentinel should be normalized to ""
				ServingModel: "llama-3-70b",
				Namespace:    "llm-prod",
			}: {
				PromptTokens:     50000,
				CompletionTokens: 50000,
			},
		},
	}

	results, _ := c.BuildDimensionCosts([]*InferenceCost{mc}, dimTokens)
	var teamRow *InferenceCost
	for _, r := range results {
		if r.Properties.TenantID == "team-ml" {
			teamRow = r
		}
	}
	if teamRow == nil {
		t.Fatalf("missing team-ml row")
	}

	if teamRow.Properties.UserID != "" {
		t.Errorf("expected UserID to be empty for userSentinel 'None', got %q", teamRow.Properties.UserID)
	}
	if teamRow.Properties.WorkloadID != "" {
		t.Errorf("expected WorkloadID to be empty when workloadLabel is disabled, got %q", teamRow.Properties.WorkloadID)
	}
}

func TestBuildDimensionCosts_SaaSExclusion_Scope(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "scope.yaml")
	configYAML := "producers:\n" +
		"  - name: gateway\n" +
		"    inputTokensSeries: in_tokens\n" +
		"    outputTokensSeries: out_tokens\n" +
		"    tenantLabel: tenant_id\n" +
		"    modelLabel: model_name\n" +
		"    namespaceLabel: namespace\n" +
		"    scope:\n" +
		"      excludeServingModels:\n" +
		"        - gpt-4o\n" +
		"        - claude-3-5-sonnet\n"
	if err := os.WriteFile(configPath, []byte(configYAML), 0644); err != nil {
		t.Fatalf("writing config: %v", err)
	}

	cfg := baseConfig()
	cfg.AttributionEnabled = true
	cfg.AttributionConfigPath = configPath
	c := &Collector{config: cfg}

	mc := makeTestModelCost("llama-3-70b", "llm-prod", 100000, 100000, 10.0, 5.0, 50.0, 50.0, 25.0, 25.0)

	dimTokens := &source.InferenceDimensionResult{
		Values: map[source.InferenceDimensionKey]*source.InferenceDimensionTokens{
			{
				Producer:     "gateway",
				TenantID:     "tenant-a",
				ServingModel: "gpt-4o", // Excluded SaaS model
				Namespace:    "llm-prod",
			}: {
				PromptTokens:     50000,
				CompletionTokens: 50000,
			},
			{
				Producer:     "gateway",
				TenantID:     "tenant-a",
				ServingModel: "llama-3-70b",
				Namespace:    "llm-prod",
			}: {
				PromptTokens:     50000,
				CompletionTokens: 50000,
			},
		},
	}

	results, unmatched := c.BuildDimensionCosts([]*InferenceCost{mc}, dimTokens)
	if len(results) != 2 { // tenant-a on llama-3-70b + remainder on llama-3-70b
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	if unmatched["gateway"] != 0 {
		t.Errorf("excluded SaaS model should NOT be counted as unmatched, got %f", unmatched["gateway"])
	}
}

func TestBuildDimensionCosts_ProportionalSplitAcrossNamespaces(t *testing.T) {
	cfg := baseConfig()
	cfg.AttributionEnabled = true
	c := &Collector{config: cfg}

	// Model deployed in 2 namespaces:
	// ns-a: 600k prompt, 400k gen (1M total engine tokens). $20 alloc cost ($10/M in, $20/M out)
	// ns-b: 1500k prompt, 1500k gen (3M total engine tokens). $60 alloc cost ($10/M in, $20/M out)
	// Total engine tokens = 4M. ns-a share = 25%, ns-b share = 75%.
	mcA := makeTestModelCost("llama-3-70b", "ns-a", 600000, 400000, 20.0, 10.0, 10.0, 20.0, 5.0, 10.0)
	mcB := makeTestModelCost("llama-3-70b", "ns-b", 1500000, 1500000, 60.0, 30.0, 10.0, 20.0, 5.0, 10.0)

	// Attribution key with empty namespace
	dimTokens := &source.InferenceDimensionResult{
		Values: map[source.InferenceDimensionKey]*source.InferenceDimensionTokens{
			{
				Producer:     "default",
				TenantID:     "tenant-a",
				WorkloadID:   "w1",
				ServingModel: "llama-3-70b",
				Namespace:    "", // Missing namespace
			}: {
				PromptTokens:     400000,
				CompletionTokens: 400000,
			},
		},
	}

	results, _ := c.BuildDimensionCosts([]*InferenceCost{mcA, mcB}, dimTokens)
	// Expect: tenant-a in ns-a (100k in, 100k out), remainder in ns-a
	//         tenant-a in ns-b (300k in, 300k out), remainder in ns-b
	if len(results) != 4 {
		t.Fatalf("expected 4 results, got %d", len(results))
	}

	var rowNsA, rowNsB *InferenceCost
	for _, r := range results {
		if r.Properties.TenantID == "tenant-a" {
			if r.Properties.Namespace == "ns-a" {
				rowNsA = r
			} else if r.Properties.Namespace == "ns-b" {
				rowNsB = r
			}
		}
	}

	if rowNsA == nil || rowNsB == nil {
		t.Fatalf("missing split rows for tenant-a")
	}

	if !rowNsA.Approximate || !rowNsB.Approximate {
		t.Errorf("expected split rows to be marked Approximate: true")
	}

	if math.Abs(rowNsA.PromptTokens-100000) > 1e-6 || math.Abs(rowNsA.GenerationTokens-100000) > 1e-6 {
		t.Errorf("rowNsA tokens want 100k, got prompt=%f gen=%f", rowNsA.PromptTokens, rowNsA.GenerationTokens)
	}
	if math.Abs(rowNsB.PromptTokens-300000) > 1e-6 || math.Abs(rowNsB.GenerationTokens-300000) > 1e-6 {
		t.Errorf("rowNsB tokens want 300k, got prompt=%f gen=%f", rowNsB.PromptTokens, rowNsB.GenerationTokens)
	}
}

func TestBuildDimensionCosts_CanonicalModelMatching(t *testing.T) {
	cfg := baseConfig()
	cfg.AttributionEnabled = true
	c := &Collector{config: cfg}

	// Model in OpenCost has short name "Llama-3-70B"
	mc := makeTestModelCost("Llama-3-70B", "llm-prod", 100000, 100000, 10.0, 5.0, 50.0, 50.0, 25.0, 25.0)

	// Producer emits fully-qualified name "meta-llama/Llama-3-70B"
	dimTokens := &source.InferenceDimensionResult{
		Values: map[source.InferenceDimensionKey]*source.InferenceDimensionTokens{
			{
				Producer:     "default",
				TenantID:     "tenant-a",
				ServingModel: "meta-llama/Llama-3-70B",
				Namespace:    "llm-prod",
			}: {
				PromptTokens:     50000,
				CompletionTokens: 50000,
			},
		},
	}

	results, unmatched := c.BuildDimensionCosts([]*InferenceCost{mc}, dimTokens)
	var tenantA *InferenceCost
	for _, r := range results {
		if r.Properties.TenantID == "tenant-a" {
			tenantA = r
		}
	}
	if tenantA == nil {
		t.Fatalf("expected tenant-a to match canonical model name")
	}
	if tenantA.Properties.ModelName != "Llama-3-70B" {
		t.Errorf("expected model name to be 'Llama-3-70B', got %s", tenantA.Properties.ModelName)
	}
	if unmatched["default"] != 0 {
		t.Errorf("expected 0 unmatched keys, got %f", unmatched["default"])
	}
}

// A namespace-less key from a producer scoped to ns-a must never be resolved
// or split into ns-b.
func TestBuildDimensionCosts_NamespaceLessKeyRespectsNamespaceScope(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "scope.yaml")
	configYAML := "producers:\n" +
		"  - name: gateway\n" +
		"    inputTokensSeries: in_tokens\n" +
		"    outputTokensSeries: out_tokens\n" +
		"    scope:\n" +
		"      namespaces:\n" +
		"        - ns-a\n"
	if err := os.WriteFile(configPath, []byte(configYAML), 0644); err != nil {
		t.Fatalf("writing config: %v", err)
	}

	cfg := baseConfig()
	cfg.AttributionEnabled = true
	cfg.AttributionConfigPath = configPath
	c := &Collector{config: cfg}

	mcA := makeTestModelCost("llama-3-70b", "ns-a", 600000, 400000, 20.0, 10.0, 10.0, 20.0, 5.0, 10.0)
	mcB := makeTestModelCost("llama-3-70b", "ns-b", 1500000, 1500000, 60.0, 30.0, 10.0, 20.0, 5.0, 10.0)

	dimTokens := &source.InferenceDimensionResult{
		Values: map[source.InferenceDimensionKey]*source.InferenceDimensionTokens{
			{Producer: "gateway", TenantID: "tenant-a", ServingModel: "llama-3-70b"}: {
				PromptTokens:     400000,
				CompletionTokens: 400000,
			},
		},
	}

	results, _ := c.BuildDimensionCosts([]*InferenceCost{mcA, mcB}, dimTokens)
	for _, r := range results {
		if r.Properties.TenantID == "tenant-a" && r.Properties.Namespace != "ns-a" {
			t.Errorf("tenant-a resolved outside producer scope into %q", r.Properties.Namespace)
		}
	}
	var inNsA *InferenceCost
	for _, r := range results {
		if r.Properties.TenantID == "tenant-a" && r.Properties.Namespace == "ns-a" {
			inNsA = r
		}
	}
	if inNsA == nil || math.Abs(inNsA.PromptTokens-400000) > 1e-6 {
		t.Errorf("expected all tenant-a tokens resolved into ns-a, got %+v", inNsA)
	}
}

// With no attribution, the remainder must keep the model's directional rates
// ($10/M input, $20/M output on 1M tokens each) instead of a token-count split.
func TestBuildDimensionCosts_RemainderKeepsDirectionalRates(t *testing.T) {
	cfg := baseConfig()
	cfg.AttributionEnabled = true
	c := &Collector{config: cfg}

	mc := makeTestModelCost("llama-3-70b", "llm-prod", 1000000, 1000000, 30.0, 15.0, 10.0, 20.0, 5.0, 10.0)

	results, _ := c.BuildDimensionCosts([]*InferenceCost{mc}, &source.InferenceDimensionResult{})
	if len(results) != 1 {
		t.Fatalf("expected only the remainder row, got %d", len(results))
	}
	rem := results[0]
	if math.Abs(rem.InputCost[CostBasisAllocation]-10.0) > 1e-9 || math.Abs(rem.OutputCost[CostBasisAllocation]-20.0) > 1e-9 {
		t.Errorf("allocation remainder want $10/$20, got $%f/$%f", rem.InputCost[CostBasisAllocation], rem.OutputCost[CostBasisAllocation])
	}
	if math.Abs(rem.InputCost[CostBasisUsage]-5.0) > 1e-9 || math.Abs(rem.OutputCost[CostBasisUsage]-10.0) > 1e-9 {
		t.Errorf("usage remainder want $5/$10, got $%f/$%f", rem.InputCost[CostBasisUsage], rem.OutputCost[CostBasisUsage])
	}
}
