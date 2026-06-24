package inferencecost

import "github.com/opencost/opencost/core/pkg/log"

// Calculator computes derived cost metrics for a slice of InferenceCost structs.
type Calculator struct {
	config *Config
}

// NewCalculator creates a Calculator with the given config.
func NewCalculator(config *Config) *Calculator {
	return &Calculator{config: config}
}

// CalculateCosts populates derived cost fields on each InferenceCost in-place.
func (c *Calculator) CalculateCosts(metrics []*InferenceCost) {
	for _, m := range metrics {
		c.calculateModelCosts(m)
	}
}

func (c *Calculator) calculateModelCosts(m *InferenceCost) {
	m.CostPerMillionTokens = make(map[CostBasis]float64)
	m.InputCostPerMillionTokens = make(map[CostBasis]float64)
	m.OutputCostPerMillionTokens = make(map[CostBasis]float64)
	m.InputCost = make(map[CostBasis]float64)
	m.OutputCost = make(map[CostBasis]float64)

	// Usage cost requires evidence of actual token processing. Without tokens,
	// the pod was provisioned but idle: there is no active compute to charge for.
	if m.TotalTokens == 0 {
		m.UsageTotalCost = 0
	}

	// Blended cost per million tokens (all delivered tokens, including cached).
	// Uses TotalTokens — answers "average cost per delivered token".
	if m.TotalTokens > 0 {
		m.CostPerMillionTokens[CostBasisAllocation] = m.AllocationTotalCost / m.TotalTokens * 1_000_000
		m.CostPerMillionTokens[CostBasisUsage] = m.UsageTotalCost / m.TotalTokens * 1_000_000
	}

	// Case 1: no tokens or no cost — allocation method not applicable.
	if m.TotalTokens == 0 || (m.AllocationTotalCost == 0 && m.UsageTotalCost == 0) {
		m.AllocationMethod = ""
		return
	}

	// Cache savings fraction: fraction of prompt tokens served from KV cache.
	// Clamped to [0, 1]: vllm:prefix_cache_hits_total counts tokens retrieved
	// from cache per request, while vllm:prompt_tokens_total counts new input
	// tokens. In workloads with heavy prefix reuse (e.g. benchmarks), cached
	// tokens can exceed prompt tokens within a short window because cache hits
	// reflect prefixes established by earlier requests, including those outside
	// the current window. Values >1 before clamping indicate extreme cache reuse.
	if m.PromptTokens > 0 {
		m.CacheSavingsFraction = min(m.CachedTokens/m.PromptTokens, 1.0)
	}

	// Input/output split — choose the allocation method.
	// Require both timing components to be present for compute-time allocation.
	// One-sided timing data is treated as incomplete and falls back to multiplier.
	hasCompleteTimingData := m.InputProcessingTime > 0 && m.OutputProcessingTime > 0
	if c.config.AllocationMode == AllocationModeComputeTime && hasCompleteTimingData {
		c.calculateComputeTimeSplit(m)
	} else {
		if c.config.AllocationMode == AllocationModeComputeTime && !hasCompleteTimingData {
			log.Debugf("InferenceCost: incomplete timing data for model %s/%s (input=%f output=%f), using multiplier fallback",
				m.Properties.ModelName, m.Properties.Namespace, m.InputProcessingTime, m.OutputProcessingTime)
		}
		c.calculateMultiplierSplit(m)
	}
}

// calculateComputeTimeSplit allocates costs proportionally by vLLM processing time.
// Uses EffectiveInputTokens (cache-corrected) as the input denominator.
func (c *Calculator) calculateComputeTimeSplit(m *InferenceCost) {
	totalTime := m.InputProcessingTime + m.OutputProcessingTime
	if totalTime == 0 {
		// Timing data present but both zero — fall back.
		c.calculateMultiplierSplit(m)
		return
	}

	inputFraction := m.InputProcessingTime / totalTime
	outputFraction := 1 - inputFraction

	// Determine allocation method based on cache config.
	// Only set prefix_caching_off when the config was successfully retrieved
	// and explicitly indicates caching is disabled — not when the metric is absent.
	if m.CacheConfigKnown && !m.PrefixCachingEnabled {
		m.AllocationMethod = AllocationMethodPrefixCachingOff
	} else {
		m.AllocationMethod = AllocationMethodComputeTime
	}

	for _, basis := range []CostBasis{CostBasisUsage, CostBasisAllocation} {
		var totalCost float64
		if basis == CostBasisUsage {
			totalCost = m.UsageTotalCost
		} else {
			totalCost = m.AllocationTotalCost
		}

		inputCost := totalCost * inputFraction
		outputCost := totalCost * outputFraction

		m.InputCost[basis] = inputCost
		m.OutputCost[basis] = outputCost

		if m.PromptTokens > 0 {
			m.InputCostPerMillionTokens[basis] = inputCost / m.PromptTokens * 1_000_000
		}
		if m.GenerationTokens > 0 {
			m.OutputCostPerMillionTokens[basis] = outputCost / m.GenerationTokens * 1_000_000
		}
	}

	log.Debugf("InferenceCost: compute-time split model=%s/%s input=%.1f%% output=%.1f%% method=%s",
		m.Properties.ModelName, m.Properties.Namespace,
		inputFraction*100, outputFraction*100, m.AllocationMethod)
}

// calculateMultiplierSplit allocates costs using a fixed output/input ratio.
// Uses EffectiveInputTokens for cost allocation; InputCostPerMillionTokens uses PromptTokens as denominator.
func (c *Calculator) calculateMultiplierSplit(m *InferenceCost) {
	m.AllocationMethod = AllocationMethodMultiplier

	multiplier := c.config.OutputTokenCostMultiplier
	if multiplier <= 0 {
		multiplier = defaultOutputTokenCostMultiplier
	}

	// weightedTokens based on effective input tokens (cache-corrected).
	weightedTokens := m.EffectiveInputTokens + m.GenerationTokens*multiplier
	if weightedTokens == 0 {
		return
	}

	for _, basis := range []CostBasis{CostBasisUsage, CostBasisAllocation} {
		var totalCost float64
		if basis == CostBasisUsage {
			totalCost = m.UsageTotalCost
		} else {
			totalCost = m.AllocationTotalCost
		}

		inputCostPerToken := totalCost / weightedTokens

		inputCost := inputCostPerToken * m.EffectiveInputTokens
		outputCost := inputCostPerToken * multiplier * m.GenerationTokens

		m.InputCost[basis] = inputCost
		m.OutputCost[basis] = outputCost

		if m.PromptTokens > 0 {
			m.InputCostPerMillionTokens[basis] = inputCost / m.PromptTokens * 1_000_000
		}
		if m.GenerationTokens > 0 {
			m.OutputCostPerMillionTokens[basis] = outputCost / m.GenerationTokens * 1_000_000
		}
	}
}
