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

	// Blended cost per million tokens (all delivered tokens, including cached).
	// Uses TotalTokens — answers "average cost per delivered token".
	if m.TotalTokens > 0 {
		m.CostPerMillionTokens[CostBasisAllocation] = m.AllocationTotalCost / m.TotalTokens * 1_000_000
		m.CostPerMillionTokens[CostBasisUsage] = m.UsageTotalCost / m.TotalTokens * 1_000_000
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

	// Determine allocation method label based on whether cache correction applied.
	if m.BlockSize > 0 && m.CacheHitBlocks > 0 {
		m.AllocationMethod = AllocationMethodComputeTime
	} else {
		m.AllocationMethod = AllocationMethodComputeTimeUncorrected
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

		if m.EffectiveInputTokens > 0 {
			m.InputCostPerMillionTokens[basis] = inputCost / m.EffectiveInputTokens * 1_000_000
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
// Uses EffectiveInputTokens as the input denominator for consistency.
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

		if m.EffectiveInputTokens > 0 {
			m.InputCostPerMillionTokens[basis] = inputCostPerToken * 1_000_000
		}
		if m.GenerationTokens > 0 {
			m.OutputCostPerMillionTokens[basis] = inputCostPerToken * multiplier * 1_000_000
		}
	}
}
