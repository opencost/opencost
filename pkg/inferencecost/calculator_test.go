package inferencecost

import (
	"math"
	"testing"
)

func defaultConfig() *Config {
	return &Config{
		AllocationMode:            AllocationModeComputeTime,
		OutputTokenCostMultiplier: 2.5,
	}
}

func newCalc(cfg *Config) *Calculator { return NewCalculator(cfg) }

// floatEq returns true if a and b differ by less than 1e-9.
func floatEq(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

// ---- blended per-million-tokens ----

func TestCalculator_BlendedCostPerMillionTokens(t *testing.T) {
	cfg := defaultConfig()
	m := &InferenceCost{
		AllocationTotalCost:  4.0,
		UsageTotalCost:       1.0,
		PromptTokens:         800_000,
		GenerationTokens:     200_000,
		TotalTokens:          1_000_000,
		EffectiveInputTokens: 800_000,
		// no timing data → multiplier fallback
	}
	newCalc(cfg).CalculateCosts([]*InferenceCost{m})

	if !floatEq(m.CostPerMillionTokens[CostBasisAllocation], 4.0) {
		t.Errorf("allocation blended want 4.0 got %f", m.CostPerMillionTokens[CostBasisAllocation])
	}
	if !floatEq(m.CostPerMillionTokens[CostBasisUsage], 1.0) {
		t.Errorf("usage blended want 1.0 got %f", m.CostPerMillionTokens[CostBasisUsage])
	}
}

func TestCalculator_BlendedZeroTokens(t *testing.T) {
	m := &InferenceCost{AllocationTotalCost: 1.0, UsageTotalCost: 0.5}
	newCalc(defaultConfig()).CalculateCosts([]*InferenceCost{m})

	if m.CostPerMillionTokens[CostBasisAllocation] != 0 {
		t.Error("expected zero blended cost when TotalTokens == 0")
	}
}

// ---- compute-time split ----

func TestCalculator_ComputeTimeSplit_BothBases(t *testing.T) {
	cfg := defaultConfig()
	m := &InferenceCost{
		AllocationTotalCost:  4.0,
		UsageTotalCost:       1.0,
		PromptTokens:         600_000,
		GenerationTokens:     400_000,
		TotalTokens:          1_000_000,
		EffectiveInputTokens: 600_000, // no cache correction
		InputProcessingTime:  70.0,
		OutputProcessingTime: 30.0,
	}
	newCalc(cfg).CalculateCosts([]*InferenceCost{m})

	// inputFraction = 0.7, outputFraction = 0.3
	// usage: inputCost=0.7, outputCost=0.3
	wantUsageInput := 0.7 / 600_000 * 1_000_000
	wantUsageOutput := 0.3 / 400_000 * 1_000_000
	wantAllocInput := (4.0 * 0.7) / 600_000 * 1_000_000
	wantAllocOutput := (4.0 * 0.3) / 400_000 * 1_000_000

	if !floatEq(m.InputCostPerMillionTokens[CostBasisUsage], wantUsageInput) {
		t.Errorf("usage input want %f got %f", wantUsageInput, m.InputCostPerMillionTokens[CostBasisUsage])
	}
	if !floatEq(m.OutputCostPerMillionTokens[CostBasisUsage], wantUsageOutput) {
		t.Errorf("usage output want %f got %f", wantUsageOutput, m.OutputCostPerMillionTokens[CostBasisUsage])
	}
	if !floatEq(m.InputCostPerMillionTokens[CostBasisAllocation], wantAllocInput) {
		t.Errorf("alloc input want %f got %f", wantAllocInput, m.InputCostPerMillionTokens[CostBasisAllocation])
	}
	if !floatEq(m.OutputCostPerMillionTokens[CostBasisAllocation], wantAllocOutput) {
		t.Errorf("alloc output want %f got %f", wantAllocOutput, m.OutputCostPerMillionTokens[CostBasisAllocation])
	}
	if m.AllocationMethod != AllocationMethodComputeTime {
		t.Errorf("expected compute_time (no block size), got %s", m.AllocationMethod)
	}
}

func TestCalculator_ComputeTimeSplit_InputOutputSumToTotal(t *testing.T) {
	cfg := defaultConfig()
	m := &InferenceCost{
		AllocationTotalCost:  10.0,
		UsageTotalCost:       3.0,
		PromptTokens:         500_000,
		GenerationTokens:     500_000,
		TotalTokens:          1_000_000,
		EffectiveInputTokens: 500_000,
		InputProcessingTime:  60.0,
		OutputProcessingTime: 40.0,
	}
	newCalc(cfg).CalculateCosts([]*InferenceCost{m})

	// input_cost + output_cost must equal total for each basis.
	// Back-compute dollar amounts from the per-million rates using PromptTokens.
	for _, basis := range []CostBasis{CostBasisUsage, CostBasisAllocation} {
		var totalCost float64
		if basis == CostBasisUsage {
			totalCost = m.UsageTotalCost
		} else {
			totalCost = m.AllocationTotalCost
		}
		inputCost := m.InputCostPerMillionTokens[basis] / 1_000_000 * m.PromptTokens
		outputCost := m.OutputCostPerMillionTokens[basis] / 1_000_000 * m.GenerationTokens
		if !floatEq(inputCost+outputCost, totalCost) {
			t.Errorf("basis=%s: input+output=%.6f want %.6f", basis, inputCost+outputCost, totalCost)
		}
	}
}

// TestCalculator_ComputeTimeSplit_DecodeDominatesRealisticWorkload guards the
// unit compatibility of the two timing inputs.
//
// The timings model 100 requests at 0.2s prefill and 5s decode each — a typical
// chat workload, where decode dominates because it runs once per output token.
// Generation must therefore carry the large majority of cost.
//
// This is a regression test for pairing prefill seconds with
// vllm:request_time_per_output_token_seconds_sum, whose _sum is a sum of
// per-request mean seconds-per-token rather than a duration. At 200 output
// tokens per request that metric would have yielded 2.5s here instead of 500s,
// inverting the split to ~88.9% input. Round-number fixtures such as
// 60/40 pass under either metric, so the realistic ratio is the point.
func TestCalculator_ComputeTimeSplit_DecodeDominatesRealisticWorkload(t *testing.T) {
	m := &InferenceCost{
		AllocationTotalCost:  10.0,
		UsageTotalCost:       10.0,
		PromptTokens:         100_000,
		GenerationTokens:     20_000,
		TotalTokens:          120_000,
		EffectiveInputTokens: 100_000,
		InputProcessingTime:  20.0,  // 100 requests x 0.2s prefill
		OutputProcessingTime: 500.0, // 100 requests x 5s decode
	}
	newCalc(defaultConfig()).CalculateCosts([]*InferenceCost{m})

	// Expected split: 20 / 520 = 3.846% input, 96.154% output.
	wantInputCost := 10.0 * (20.0 / 520.0)
	wantOutputCost := 10.0 * (500.0 / 520.0)

	if !floatEq(m.InputCost[CostBasisAllocation], wantInputCost) {
		t.Errorf("input cost want %.6f got %.6f", wantInputCost, m.InputCost[CostBasisAllocation])
	}
	if !floatEq(m.OutputCost[CostBasisAllocation], wantOutputCost) {
		t.Errorf("output cost want %.6f got %.6f", wantOutputCost, m.OutputCost[CostBasisAllocation])
	}

	// Directional guard: generation must dominate. This is what fails loudly if a
	// seconds-per-token metric is substituted for decode duration.
	if m.OutputCost[CostBasisAllocation] <= m.InputCost[CostBasisAllocation] {
		t.Errorf("decode-dominant workload must allocate more to output: input=%.6f output=%.6f",
			m.InputCost[CostBasisAllocation], m.OutputCost[CostBasisAllocation])
	}
	if m.AllocationMethod != AllocationMethodComputeTime {
		t.Errorf("expected compute_time, got %s", m.AllocationMethod)
	}
}

// TestCalculator_ComputeTimeSplit_PrefillDominantStillApplied documents that a
// prefill-dominant split is logged as suspicious but still honoured — it is
// legitimate for long prompts with very short generations.
func TestCalculator_ComputeTimeSplit_PrefillDominantStillApplied(t *testing.T) {
	m := &InferenceCost{
		AllocationTotalCost:  10.0,
		UsageTotalCost:       10.0,
		PromptTokens:         100_000,
		GenerationTokens:     100,
		TotalTokens:          100_100,
		EffectiveInputTokens: 100_000,
		InputProcessingTime:  90.0,
		OutputProcessingTime: 10.0,
	}
	newCalc(defaultConfig()).CalculateCosts([]*InferenceCost{m})

	if !floatEq(m.InputCost[CostBasisAllocation], 9.0) {
		t.Errorf("input cost want 9.0 got %.6f", m.InputCost[CostBasisAllocation])
	}
	if !floatEq(m.OutputCost[CostBasisAllocation], 1.0) {
		t.Errorf("output cost want 1.0 got %.6f", m.OutputCost[CostBasisAllocation])
	}
	if m.AllocationMethod != AllocationMethodComputeTime {
		t.Errorf("expected compute_time, got %s", m.AllocationMethod)
	}
}

// ---- KV cache savings fraction ----

func TestCalculator_CacheSavingsFraction(t *testing.T) {
	cfg := &Config{
		AllocationMode:            AllocationModeComputeTime,
		OutputTokenCostMultiplier: 2.5,
	}
	// 8 cached tokens out of 20 prompt tokens → 40% savings
	m := &InferenceCost{
		AllocationTotalCost:  1.0,
		UsageTotalCost:       1.0,
		PromptTokens:         20,
		GenerationTokens:     10,
		TotalTokens:          30,
		CachedTokens:         8,
		CacheConfigKnown:     true,
		PrefixCachingEnabled: true,
		EffectiveInputTokens: 12,
		InputProcessingTime:  60,
		OutputProcessingTime: 40,
	}
	newCalc(cfg).CalculateCosts([]*InferenceCost{m})

	// inputCostPerMillionTokens uses PromptTokens (20) as denominator.
	wantInputCPM := (1.0 * 0.6 / 20) * 1_000_000
	got := m.InputCostPerMillionTokens[CostBasisUsage]
	if !floatEq(got, wantInputCPM) {
		t.Errorf("input CPM want %f got %f", wantInputCPM, got)
	}
	// CacheSavingsFraction = 8/20 = 0.4
	if !floatEq(m.CacheSavingsFraction, 0.4) {
		t.Errorf("CacheSavingsFraction want 0.4 got %f", m.CacheSavingsFraction)
	}
	// Method collapses to compute_time regardless of cache hits.
	if m.AllocationMethod != AllocationMethodComputeTime {
		t.Errorf("expected compute_time, got %s", m.AllocationMethod)
	}
}

func TestCalculator_CacheCorrection_Disabled_WhenConfigUnknown(t *testing.T) {
	cfg := &Config{
		AllocationMode:            AllocationModeComputeTime,
		OutputTokenCostMultiplier: 2.5,
	}
	// CacheConfigKnown=false simulates vllm:cache_config_info being unavailable.
	m := &InferenceCost{
		AllocationTotalCost:  1.0,
		UsageTotalCost:       1.0,
		PromptTokens:         20,
		GenerationTokens:     10,
		TotalTokens:          30,
		CachedTokens:         0,
		CacheConfigKnown:     false,
		EffectiveInputTokens: 20,
		InputProcessingTime:  60,
		OutputProcessingTime: 40,
	}
	newCalc(cfg).CalculateCosts([]*InferenceCost{m})

	if m.AllocationMethod != AllocationMethodComputeTime {
		t.Errorf("expected compute_time when cache config unknown, got %s", m.AllocationMethod)
	}
	wantInput := (1.0 * 0.6 / 20) * 1_000_000
	if !floatEq(m.InputCostPerMillionTokens[CostBasisUsage], wantInput) {
		t.Errorf("want %f got %f", wantInput, m.InputCostPerMillionTokens[CostBasisUsage])
	}
	if m.CacheSavingsFraction != 0 {
		t.Errorf("CacheSavingsFraction want 0 when config unknown, got %f", m.CacheSavingsFraction)
	}
}

func TestCalculator_PrefixCachingOff_WhenConfigKnownAndDisabled(t *testing.T) {
	cfg := &Config{
		AllocationMode:            AllocationModeComputeTime,
		OutputTokenCostMultiplier: 2.5,
	}
	m := &InferenceCost{
		AllocationTotalCost:  1.0,
		UsageTotalCost:       1.0,
		PromptTokens:         100,
		GenerationTokens:     50,
		TotalTokens:          150,
		CachedTokens:         0,
		CacheConfigKnown:     true,
		PrefixCachingEnabled: false, // explicitly disabled
		EffectiveInputTokens: 100,
		InputProcessingTime:  70,
		OutputProcessingTime: 30,
	}
	newCalc(cfg).CalculateCosts([]*InferenceCost{m})

	if m.AllocationMethod != AllocationMethodPrefixCachingOff {
		t.Errorf("expected prefix_caching_off, got %s", m.AllocationMethod)
	}
}

func TestCalculator_CacheCorrection_Disabled_WhenNoCacheHits(t *testing.T) {
	cfg := &Config{
		AllocationMode:            AllocationModeComputeTime,
		OutputTokenCostMultiplier: 2.5,
	}
	m := &InferenceCost{
		AllocationTotalCost:  1.0,
		UsageTotalCost:       1.0,
		PromptTokens:         100,
		GenerationTokens:     50,
		TotalTokens:          150,
		CachedTokens:         0, // no hits in this window
		CacheConfigKnown:     true,
		PrefixCachingEnabled: true, // caching is on, just no hits occurred
		EffectiveInputTokens: 100,
		InputProcessingTime:  70,
		OutputProcessingTime: 30,
	}
	newCalc(cfg).CalculateCosts([]*InferenceCost{m})

	if m.AllocationMethod != AllocationMethodComputeTime {
		t.Errorf("expected compute_time when prefix caching enabled but no hits in window, got %s", m.AllocationMethod)
	}
}

// ---- multiplier fallback ----

func TestCalculator_MultiplierFallback_BothBases(t *testing.T) {
	cfg := &Config{
		AllocationMode:            AllocationModeComputeTime,
		OutputTokenCostMultiplier: 2.5,
	}
	// No timing data → multiplier fallback
	m := &InferenceCost{
		AllocationTotalCost:  5.0,
		UsageTotalCost:       2.0,
		PromptTokens:         800_000,
		GenerationTokens:     200_000,
		TotalTokens:          1_000_000,
		EffectiveInputTokens: 800_000,
	}
	newCalc(cfg).CalculateCosts([]*InferenceCost{m})

	if m.AllocationMethod != AllocationMethodMultiplier {
		t.Errorf("expected multiplier method, got %s", m.AllocationMethod)
	}

	// weightedTokens = 800000 + 200000*2.5 = 1300000
	// usage: inputCPT = 2.0/1300000; inputCPM = inputCPT*1e6
	// alloc: inputCPT = 5.0/1300000
	for _, tc := range []struct {
		basis     CostBasis
		totalCost float64
	}{
		{CostBasisUsage, 2.0},
		{CostBasisAllocation, 5.0},
	} {
		weighted := 800_000.0 + 200_000.0*2.5
		wantInput := (tc.totalCost / weighted) * 1_000_000
		wantOutput := wantInput * 2.5
		if !floatEq(m.InputCostPerMillionTokens[tc.basis], wantInput) {
			t.Errorf("basis=%s input want %f got %f", tc.basis, wantInput, m.InputCostPerMillionTokens[tc.basis])
		}
		if !floatEq(m.OutputCostPerMillionTokens[tc.basis], wantOutput) {
			t.Errorf("basis=%s output want %f got %f", tc.basis, wantOutput, m.OutputCostPerMillionTokens[tc.basis])
		}
	}
}

func TestCalculator_MultiplierFallback_ZeroTokens(t *testing.T) {
	m := &InferenceCost{AllocationTotalCost: 1.0, UsageTotalCost: 0.5}
	// EffectiveInputTokens and GenerationTokens are both 0
	cfg := &Config{AllocationMode: AllocationModeMultiplier, OutputTokenCostMultiplier: 2.5}
	newCalc(cfg).CalculateCosts([]*InferenceCost{m})

	if m.InputCostPerMillionTokens[CostBasisUsage] != 0 ||
		m.OutputCostPerMillionTokens[CostBasisAllocation] != 0 {
		t.Error("expected zero derived costs when tokens are zero")
	}
}

func TestCalculator_IncompleteTimingData_FallsBackToMultiplier(t *testing.T) {
	cfg := &Config{
		AllocationMode:            AllocationModeComputeTime,
		OutputTokenCostMultiplier: 2.5,
	}
	m := &InferenceCost{
		AllocationTotalCost:  5.0,
		UsageTotalCost:       2.0,
		PromptTokens:         800_000,
		GenerationTokens:     200_000,
		TotalTokens:          1_000_000,
		EffectiveInputTokens: 800_000,
		InputProcessingTime:  60,
		OutputProcessingTime: 0, // incomplete timing data
	}
	newCalc(cfg).CalculateCosts([]*InferenceCost{m})

	if m.AllocationMethod != AllocationMethodMultiplier {
		t.Fatalf("expected multiplier fallback for incomplete timing data, got %s", m.AllocationMethod)
	}

	weighted := 800_000.0 + 200_000.0*2.5
	wantInput := (2.0 / weighted) * 1_000_000
	wantOutput := wantInput * 2.5

	if !floatEq(m.InputCostPerMillionTokens[CostBasisUsage], wantInput) {
		t.Errorf("usage input want %f got %f", wantInput, m.InputCostPerMillionTokens[CostBasisUsage])
	}
	if !floatEq(m.OutputCostPerMillionTokens[CostBasisUsage], wantOutput) {
		t.Errorf("usage output want %f got %f", wantOutput, m.OutputCostPerMillionTokens[CostBasisUsage])
	}
}
