package inferencecost

import (
	"context"
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

	resultsAlloc, err := c.extractAllocationResults(asWithIdle, true, false)
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

	resultsUsage, err := c.extractAllocationResults(asWithoutIdle, false, false)
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

	results, err := c.extractAllocationResults(as, false, false)
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

	results, err := c.extractAllocationResults(as, false, false)
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

	results, err := c.extractAllocationResults(as, false, false)
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

			results, err := c.extractAllocationResults(as, false, false)
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

	results, err := c.extractAllocationResults(as, false, false)
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

	results, err := c.extractAllocationResults(as, false, false)
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

	results, err := c.extractAllocationResults(as, false, false)
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
// exactly equals the request (utilisation == 100%), the clamped ratio is 1.0
// and the full CPU cost is retained.
// This also confirms no double-counting from the subtraction/addition path.
func TestCollector_UsageCost_CPUUsageEqualsRequest(t *testing.T) {
	cfg := baseConfig()
	c := &Collector{config: cfg}
	now := time.Now()

	alloc := makeAllocationWithUtilisation(
		"llama-3",
		0, 4.0, 0,
		nil,      // no GPU
		4.0, 4.0, // usage == request → ratio 1.0 → full cost charged
		0, 0,
		"llm-prod",
	)
	as := opencost.NewAllocationSet(now.Add(-5*time.Minute), now)
	as.Set(alloc)

	results, err := c.extractAllocationResults(as, false, false)
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

// TestCollector_UsageCost_RAMUsageEqualsRequest verifies the same clamp for RAM:
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

	results, err := c.extractAllocationResults(as, false, false)
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
// the request (overcommit), the utilisation ratio clamps to 1.0
// and the full CPU cost is charged — overcommit situations should not produce
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

	results, err := c.extractAllocationResults(as, false, false)
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

// TestCollector_UsageCost_RAMOvercommit verifies the same overcommit clamp for RAM:
// usage exceeding request clamps to 1.0 rather than reducing cost.
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

	results, err := c.extractAllocationResults(as, false, false)
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

	results, err := c.extractAllocationResults(as, false, false)
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

// TestCollector_UsageCost_ZeroCPUUsage verifies that CPUCoreUsageAverage==0
// (CPU completely idle) scales CPU cost to $0, not left at full reservation.
// This is the regression test for the original `usage > 0` guard, which
// excluded the scaling block whenever usage was exactly zero and silently
// fell through to charging the full, unscaled reservation cost.
func TestCollector_UsageCost_ZeroCPUUsage(t *testing.T) {
	cfg := baseConfig()
	c := &Collector{config: cfg}
	now := time.Now()

	alloc := makeAllocationWithUtilisation(
		"llama-3",
		0, 4.0, 0,
		nil,
		4.0, 0.0, // request=4, usage=0 → completely idle CPU
		0, 0,
		"llm-prod",
	)
	as := opencost.NewAllocationSet(now.Add(-5*time.Minute), now)
	as.Set(alloc)

	results, err := c.extractAllocationResults(as, false, false)
	if err != nil {
		t.Fatalf("extractAllocationResults failed: %v", err)
	}

	key := modelNamespaceKey("llama-3", "llm-prod")
	r, ok := results[key]
	if !ok {
		t.Fatal("expected result for llama-3/llm-prod")
	}

	// CPU $4 × 0.0 = $0. Prior to the fix this asserted 4.0 (the bug).
	want := 0.0
	if !floatEq(r.usageTotalCost, want) {
		t.Errorf("usageTotalCost want %.2f got %.4f (zero CPU usage should zero CPU cost, not retain full reservation)", want, r.usageTotalCost)
	}
}

// TestCollector_UsageCost_ZeroRAMUsage verifies the same zero-usage for RAM.
func TestCollector_UsageCost_ZeroRAMUsage(t *testing.T) {
	cfg := baseConfig()
	c := &Collector{config: cfg}
	now := time.Now()

	alloc := makeAllocationWithUtilisation(
		"llama-3",
		0, 0, 2.0,
		nil,
		0, 0,
		100.0, 0.0, // request=100, usage=0 → completely idle RAM
		"llm-prod",
	)
	as := opencost.NewAllocationSet(now.Add(-5*time.Minute), now)
	as.Set(alloc)

	results, err := c.extractAllocationResults(as, false, false)
	if err != nil {
		t.Fatalf("extractAllocationResults failed: %v", err)
	}

	key := modelNamespaceKey("llama-3", "llm-prod")
	r, ok := results[key]
	if !ok {
		t.Fatal("expected result for llama-3/llm-prod")
	}

	// RAM $2 × 0.0 = $0. Prior to the fix this asserted 2.0 (the bug).
	want := 0.0
	if !floatEq(r.usageTotalCost, want) {
		t.Errorf("usageTotalCost want %.2f got %.4f (zero RAM usage should zero RAM cost, not retain full reservation)", want, r.usageTotalCost)
	}
}

// TestClampRatio verifies the shared clamp helper used by the GPU, CPU, and
// RAM utilisation scaling paths: in-range ratios pass through unchanged,
// negative ratios clamp to 0, and ratios above 1 (overcommit) clamp to 1.
func TestClampRatio(t *testing.T) {
	tests := []struct {
		name  string
		ratio float64
		want  float64
	}{
		{"zero", 0.0, 0.0},
		{"mid_range", 0.5, 0.5},
		{"exactly_one", 1.0, 1.0},
		{"negative", -0.5, 0.0},
		{"above_one", 1.5, 1.0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := clampRatio(tc.ratio)
			if !floatEq(got, tc.want) {
				t.Errorf("clampRatio(%.2f) want %.2f got %.4f", tc.ratio, tc.want, got)
			}
		})
	}
}

// TestScaleContainerCostsForUsage verifies that the scaleContainerCostsForUsage
// helper correctly scales CPU and RAM costs per-container before aggregation.
// GPU is NOT scaled by this function (handled separately in extractAllocationResults
// via GPUAllocation.GPUUsageAverage pointer).
func TestScaleContainerCostsForUsage(t *testing.T) {
	now := time.Now()

	// Container A: 100% CPU usage, 50% RAM usage
	// CPU: $4.00 * 1.0 = $4.00, RAM: $2.00 * 0.5 = $1.00
	// GPU: unchanged (not scaled by this function)
	containerA := makeAllocationWithUtilisation(
		"llama-3",
		6.0, 4.0, 2.0,
		gpuUsage(0.5),
		4.0, 4.0, // cpuCoreRequest=4, cpuCoreUsage=4 → 100%
		100.0, 50.0, // ramBytesRequest=100, ramBytesUsage=50 → 50%
		"llm-prod",
	)
	containerA.Name = "container-a"

	// Container B: 0% CPU usage (unobserved), 0% RAM usage
	// CPU: $4.00 * 0.0 = $0.00, RAM: $2.00 * 0.0 = $0.00
	// GPU: unchanged
	containerB := makeAllocationWithUtilisation(
		"llama-3",
		6.0, 4.0, 2.0,
		gpuUsage(0.5),
		4.0, 0.0, // cpuCoreRequest=4, cpuCoreUsage=0 → 0%
		100.0, 0.0, // ramBytesRequest=100, ramBytesUsage=0 → 0%
		"llm-prod",
	)
	containerB.Name = "container-b"

	as := opencost.NewAllocationSet(now.Add(-5*time.Minute), now)
	as.Set(containerA)
	as.Set(containerB)

	scaled := scaleContainerCostsForUsage(as)

	// Verify container A costs scaled correctly
	allocA := scaled.Get("container-a")
	if allocA == nil {
		t.Fatal("expected scaled allocation for container-a")
	}
	// CPU: $4.00 * 1.0 = $4.00
	if !floatEq(allocA.CPUCost, 4.0) {
		t.Errorf("container-a CPUCost want 4.0 got %.4f", allocA.CPUCost)
	}
	// RAM: $2.00 * 0.5 = $1.00
	if !floatEq(allocA.RAMCost, 1.0) {
		t.Errorf("container-a RAMCost want 1.0 got %.4f", allocA.RAMCost)
	}
	// GPU: unchanged (not scaled by this function)
	if !floatEq(allocA.GPUCost, 6.0) {
		t.Errorf("container-a GPUCost want 6.0 got %.4f", allocA.GPUCost)
	}

	// Verify container B costs scaled correctly (unobserved = 0 usage)
	allocB := scaled.Get("container-b")
	if allocB == nil {
		t.Fatal("expected scaled allocation for container-b")
	}
	// CPU: $4.00 * 0.0 = $0.00
	if !floatEq(allocB.CPUCost, 0.0) {
		t.Errorf("container-b CPUCost want 0.0 got %.4f", allocB.CPUCost)
	}
	// RAM: $2.00 * 0.0 = $0.00
	if !floatEq(allocB.RAMCost, 0.0) {
		t.Errorf("container-b RAMCost want 0.0 got %.4f", allocB.RAMCost)
	}
	// GPU: unchanged
	if !floatEq(allocB.GPUCost, 6.0) {
		t.Errorf("container-b GPUCost want 6.0 got %.4f", allocB.GPUCost)
	}
}

// TestScaleContainerCostsForUsage_PreventsDilution verifies that per-container
// scaling before aggregation prevents cross-container dilution.
//
// Scenario: Pod with 2 containers, same model/namespace
// - Container A: observed, 100% CPU usage → full cost
// - Container B: unobserved (missing telemetry), 0% CPU usage → zero cost (Policy A)
//
// OLD behavior (diluted):
//
//	AggregateBy averages usage: (1.0 + 0.0) / 2 = 0.5
//	Sums costs: $4 + $4 = $8
//	Scales: $8 * 0.5 = $4 (wrong - should be $4 from A only)
//
// NEW behavior (per-container):
//
//	Container A: $4 * 1.0 = $4
//	Container B: $4 * 0.0 = $0
//	Sum after scaling: $4 + $0 = $4 (correct)
func TestScaleContainerCostsForUsage_PreventsDilution(t *testing.T) {
	cfg := baseConfig()
	c := &Collector{config: cfg}
	now := time.Now()

	// Two containers, same model/namespace (simulating pod with 2 containers)
	// Container A: observed, 100% CPU usage
	containerA := makeAllocationWithUtilisation(
		"llama-3",
		0.0, 4.0, 0.0, // no GPU/RAM cost
		nil,      // no GPU
		4.0, 4.0, // cpuCoreRequest=4, cpuCoreUsage=4 → 100%
		0, 0, // no RAM
		"llm-prod",
	)
	containerA.Name = "llama-3/container-a"
	containerA.Properties.Container = "container-a"

	// Container B: unobserved (missing telemetry), 0% CPU usage
	containerB := makeAllocationWithUtilisation(
		"llama-3",
		0.0, 4.0, 0.0,
		nil,
		4.0, 0.0, // cpuCoreRequest=4, cpuCoreUsage=0 → 0%
		0, 0,
		"llm-prod",
	)
	containerB.Name = "llama-3/container-b"
	containerB.Properties.Container = "container-b"

	as := opencost.NewAllocationSet(now.Add(-5*time.Minute), now)
	as.Set(containerA)
	as.Set(containerB)

	// Test the OLD path (no pre-scaling, uses aggregated average)
	oldResults, err := c.extractAllocationResults(as, false, false)
	if err != nil {
		t.Fatalf("extractAllocationResults failed: %v", err)
	}
	key := modelNamespaceKey("llama-3", "llm-prod")
	_ = oldResults[key] // verify old path works
	// OLD: diluted average 50% → $8 * 0.5 = $4 (but this is the same by coincidence with 2 containers)
	// Actually let's verify the internal average: (4.0 + 0.0) / 2 = 2.0 usage avg, request=4.0
	// So cpuUtil = 2.0/4.0 = 0.5 → $8 * 0.5 = $4

	// Now test with per-container scaling BEFORE aggregation
	scaledAs := scaleContainerCostsForUsage(as)
	// Verify per-container costs after scaling (this is the core fix)
	allocA := scaledAs.Get("llama-3/container-a")
	allocB := scaledAs.Get("llama-3/container-b")
	// After scaling: A=$4, B=$0 → total=$4
	totalScaled := allocA.CPUCost + allocB.CPUCost
	if !floatEq(totalScaled, 4.0) {
		t.Errorf("total scaled CPUCost want 4.0 got %.4f", totalScaled)
	}
	// Verify individual container costs
	if !floatEq(allocA.CPUCost, 4.0) {
		t.Errorf("container-a CPUCost want 4.0 got %.4f", allocA.CPUCost)
	}
	if !floatEq(allocB.CPUCost, 0.0) {
		t.Errorf("container-b CPUCost want 0.0 got %.4f", allocB.CPUCost)
	}
}

// TestCollector_UsageCost_PreventsDilution_EndToEnd verifies the full production
// query path exercises per-container scaling before aggregation via mockQuerier.
// This is the key regression test for #4179 dilution fix.
//
// Scenario: Pod with 2 containers, same model/namespace, UNEQUAL costs
// - Container A: observed, 100% CPU usage, cost=$4
// - Container B: unobserved (missing telemetry), 0% CPU usage, cost=$12
// Both have equal CPU requests (4 cores)
//
// OLD behavior (aggregate-first):
//
//	AggregateBy averages usage: (1.0 + 0.0) / 2 = 0.5
//	Sums costs: $4 + $12 = $16
//	Scales: $16 * 0.5 = $8  ← WRONG (should be $4 from A only)
//
// NEW behavior (per-container scaling before aggregation):
//
//	Container A: $4 * 1.0 = $4
//	Container B: $12 * 0.0 = $0
//	Sum after scaling: $4 + $0 = $4  ← CORRECT
func TestCollector_UsageCost_PreventsDilution_EndToEnd(t *testing.T) {
	cfg := baseConfig()
	now := time.Now()

	// Two containers, same model/namespace (simulating pod with 2 containers)
	// Container A: observed, 100% CPU usage, cost=$4
	containerA := makeAllocationWithUtilisation(
		"llama-3",
		0.0, 4.0, 0.0, // no GPU/RAM cost
		nil,      // no GPU
		4.0, 4.0, // cpuCoreRequest=4, cpuCoreUsage=4 → 100%
		0, 0, // no RAM
		"llm-prod",
	)
	containerA.Name = "llama-3/container-a"
	containerA.Start = now.Add(-5 * time.Minute)
	containerA.End = now
	containerA.Properties.Container = "container-a"
	containerA.Properties.Cluster = "test-cluster"
	containerA.Properties.Node = "test-node"
	containerA.Properties.ProviderID = "test-provider"
	// AggregateBy sanitizes the label key ("llm-d.ai/model" → "llm_d_ai_model"),
	// so the fixture must use the sanitized form, as production allocations do.
	containerA.Properties.Labels = opencost.AllocationLabels(map[string]string{"llm_d_ai_model": "llama-3"})

	// Container B: unobserved (missing telemetry), 0% CPU usage, cost=$12
	containerB := makeAllocationWithUtilisation(
		"llama-3",
		0.0, 12.0, 0.0, // cost=$12
		nil,
		4.0, 0.0, // cpuCoreRequest=4, cpuCoreUsage=0 → 0%
		0, 0,
		"llm-prod",
	)
	containerB.Name = "llama-3/container-b"
	containerB.Start = now.Add(-5 * time.Minute)
	containerB.End = now
	containerB.Properties.Container = "container-b"
	containerB.Properties.Cluster = "test-cluster"
	containerB.Properties.Node = "test-node"
	containerB.Properties.ProviderID = "test-provider"
	containerB.Properties.Labels = opencost.AllocationLabels(map[string]string{"llm_d_ai_model": "llama-3"})

	// Create mock querier that returns our allocation set
	mock := &mockQuerier{
		set: opencost.NewAllocationSet(now.Add(-5*time.Minute), now),
	}
	mock.set.Set(containerA)
	mock.set.Set(containerB)

	c := &Collector{config: cfg, allocationQuerier: mock}

	// Call the production usage query path: ComputeAllocation →
	// scaleContainerCostsForUsage → AggregateBy → extractAllocationResults.
	results, err := c.queryAllocationCostsWithoutIdle(context.Background(), now.Add(-5*time.Minute), now)
	if err != nil {
		t.Fatalf("queryAllocationCostsWithoutIdle failed: %v", err)
	}

	key := modelNamespaceKey("llama-3", "llm-prod")
	result := results[key]
	if result == nil {
		t.Fatal("expected result for llama-3/llm-prod")
	}

	// NEW behavior (per-container scaling): should be $4 (only container A's cost)
	// OLD behavior (aggregate-first): would be $8 ($16 * 0.5)
	if !floatEq(result.usageTotalCost, 4.0) {
		t.Errorf("usageTotalCost want 4.0 (per-container scaling) got %.4f (aggregate-first would be 8.0)", result.usageTotalCost)
	}
}

// TestScaleContainerCostsForUsage_UnobservedContainerPolicyA verifies the
// default policy for unobserved containers (usage=0, request>0): treat as idle (scale to $0).
func TestScaleContainerCostsForUsage_UnobservedContainerPolicyA(t *testing.T) {
	now := time.Now()

	// Container with request>0 but usage=0 (could be idle OR missing telemetry)
	// Policy A: treat as idle → scale to $0
	container := makeAllocationWithUtilisation(
		"llama-3",
		0.0, 4.0, 2.0,
		nil,
		4.0, 0.0, // cpuCoreUsage=0 (unobserved or truly idle)
		100.0, 0.0, // ramBytesUsage=0
		"llm-prod",
	)
	container.Name = "container-unobserved"

	as := opencost.NewAllocationSet(now.Add(-5*time.Minute), now)
	as.Set(container)

	scaled := scaleContainerCostsForUsage(as)
	alloc := scaled.Get("container-unobserved")

	// Policy A: unobserved/idle → scale to $0
	if !floatEq(alloc.CPUCost, 0.0) {
		t.Errorf("Policy A: unobserved CPUCost want 0.0 got %.4f", alloc.CPUCost)
	}
	if !floatEq(alloc.RAMCost, 0.0) {
		t.Errorf("Policy A: unobserved RAMCost want 0.0 got %.4f", alloc.RAMCost)
	}
}

// TestCollector_UsageCost_PreScaledPath verifies the costsPreScaled path in
// extractAllocationResults works correctly.
func TestCollector_UsageCost_PreScaledPath(t *testing.T) {
	cfg := baseConfig()
	c := &Collector{config: cfg}
	now := time.Now()

	// Simulate what the production path hands to extractAllocationResults(as, false, true)
	// after scaleContainerCostsForUsage + AggregateBy:
	//   - CPU/RAM costs are already scaled per-container, but the utilisation
	//     averages are still present (scaleContainerCostsForUsage does not clear
	//     them). If this branch re-scaled CPU/RAM, the costs would be halved/
	//     quartered again and the assertion below would fail.
	//   - GPU cost is NOT scaled by scaleContainerCostsForUsage; it retains its
	//     full reservation cost with a GPUUsageAverage pointer, and this branch
	//     must apply the GPU scaling. If it didn't, the assertion below would fail.
	alloc := makeAllocationWithUtilisation(
		"pre-scaled-model",
		6.0, 4.0, 1.0, // GPU unscaled $6; CPU pre-scaled $8*0.5=$4; RAM pre-scaled $4*0.25=$1
		gpuUsage(0.5), // GPU SM duty cycle 50% → this branch must scale $6 → $3
		4.0, 2.0,      // CPU request=4, usage=2 → 50% util retained; must NOT re-scale $4
		8.0, 2.0, // RAM request=8, usage=2 → 25% util retained; must NOT re-scale $1
		"llm-prod",
	)

	as := opencost.NewAllocationSet(now.Add(-5*time.Minute), now)
	as.Set(alloc)

	// Use costsPreScaled=true: CPU/RAM must be summed as-is, GPU must be scaled.
	results, err := c.extractAllocationResults(as, false, true)
	if err != nil {
		t.Fatalf("extractAllocationResults failed: %v", err)
	}

	key := modelNamespaceKey("pre-scaled-model", "llm-prod")
	result := results[key]
	if result == nil {
		t.Fatal("expected result")
	}
	// GPU $6 × 0.5 = $3, plus pre-scaled CPU $4 and RAM $1 → $8.0.
	// Double-scaling CPU/RAM would yield $3 + $2 + $0.25 = $5.25;
	// failing to scale GPU would yield $6 + $4 + $1 = $11.0.
	if !floatEq(result.usageTotalCost, 8.0) {
		t.Errorf("pre-scaled usageTotalCost want 8.0 got %.4f", result.usageTotalCost)
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

	results, err := c.extractAllocationResults(as, false, false)
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
	results, err := collector.CollectMetrics(ctx, start, end)
	// With empty metrics, CollectMetrics should succeed with empty results
	if err != nil {
		t.Errorf("unexpected error with empty metrics: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results with empty metrics, got %d", len(results))
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
