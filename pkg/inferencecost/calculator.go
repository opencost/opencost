package inferencecost

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/log"
)

// Calculator calculates inference costs
type Calculator struct {
	config *Config
}

// NewCalculator creates a new cost calculator
func NewCalculator(config *Config) *Calculator {
	return &Calculator{
		config: config,
	}
}

// CalculateCosts calculates cost metrics for each model/namespace combination
func (c *Calculator) CalculateCosts(metrics []*ModelMetrics) error {
	for _, m := range metrics {
		if err := c.calculateModelCosts(m); err != nil {
			return fmt.Errorf("failed to calculate costs for model %s in namespace %s: %w",
				m.ModelName, m.Namespace, err)
		}
	}
	return nil
}

// calculateModelCosts calculates costs for a single model/namespace
func (c *Calculator) calculateModelCosts(m *ModelMetrics) error {
	// Avoid division by zero
	if m.TotalTokens == 0 {
		m.CostPerToken = 0
		m.CostPerMillionTokens = 0
		m.InputCostPerToken = 0
		m.OutputCostPerToken = 0
		m.InputCostPerMillionTokens = 0
		m.OutputCostPerMillionTokens = 0
		return nil
	}

	// Calculate blended cost per token (for backward compatibility)
	m.CostPerToken = m.TotalCost / m.TotalTokens
	m.CostPerMillionTokens = m.CostPerToken * 1000000

	// Calculate differentiated costs based on allocation mode
	if c.config.AllocationMode == ModeComputeTime {
		if err := c.calculateComputeTimeBasedCosts(m); err != nil {
			log.Warnf("Compute-time allocation failed for model %s in namespace %s, falling back to multiplier mode: %v",
				m.ModelName, m.Namespace, err)
			c.calculateMultiplierBasedCosts(m)
		}
	} else {
		c.calculateMultiplierBasedCosts(m)
	}

	return nil
}

// calculateComputeTimeBasedCosts allocates costs based on actual processing time
func (c *Calculator) calculateComputeTimeBasedCosts(m *ModelMetrics) error {
	totalTime := m.InputProcessingTime + m.OutputProcessingTime

	// Check if we have timing data
	if totalTime == 0 {
		return fmt.Errorf("no timing data available (total time is 0)")
	}

	// Allocate costs proportionally based on time spent
	m.InputCost = m.TotalCost * (m.InputProcessingTime / totalTime)
	m.OutputCost = m.TotalCost * (m.OutputProcessingTime / totalTime)

	// Calculate per-token costs
	if m.PromptTokens > 0 {
		m.InputCostPerToken = m.InputCost / m.PromptTokens
		m.InputCostPerMillionTokens = m.InputCostPerToken * 1000000
	} else {
		m.InputCostPerToken = 0
		m.InputCostPerMillionTokens = 0
	}

	if m.GenerationTokens > 0 {
		m.OutputCostPerToken = m.OutputCost / m.GenerationTokens
		m.OutputCostPerMillionTokens = m.OutputCostPerToken * 1000000
	} else {
		m.OutputCostPerToken = 0
		m.OutputCostPerMillionTokens = 0
	}

	log.Debugf("Compute-time allocation for %s/%s: input_time=%.2fs (%.1f%%), output_time=%.2fs (%.1f%%)",
		m.ModelName, m.Namespace,
		m.InputProcessingTime, (m.InputProcessingTime/totalTime)*100,
		m.OutputProcessingTime, (m.OutputProcessingTime/totalTime)*100)

	return nil
}

// calculateMultiplierBasedCosts allocates costs using a fixed multiplier for output tokens
func (c *Calculator) calculateMultiplierBasedCosts(m *ModelMetrics) {
	multiplier := c.config.OutputTokenCostMultiplier
	if multiplier <= 0 {
		multiplier = 2.5 // Default multiplier
	}

	// Calculate weighted tokens
	weightedTokens := m.PromptTokens + (m.GenerationTokens * multiplier)

	if weightedTokens == 0 {
		m.InputCostPerToken = 0
		m.OutputCostPerToken = 0
		m.InputCostPerMillionTokens = 0
		m.OutputCostPerMillionTokens = 0
		return
	}

	// Base cost per token (for input)
	m.InputCostPerToken = m.TotalCost / weightedTokens

	// Output cost per token (multiplied)
	m.OutputCostPerToken = m.InputCostPerToken * multiplier

	// Per million tokens
	m.InputCostPerMillionTokens = m.InputCostPerToken * 1000000
	m.OutputCostPerMillionTokens = m.OutputCostPerToken * 1000000

	// Calculate allocated costs
	if m.PromptTokens > 0 {
		m.InputCost = m.InputCostPerToken * m.PromptTokens
	}
	if m.GenerationTokens > 0 {
		m.OutputCost = m.OutputCostPerToken * m.GenerationTokens
	}

	log.Debugf("Multiplier-based allocation for %s/%s: multiplier=%.1fx, input_cost=$%.6f/M, output_cost=$%.6f/M",
		m.ModelName, m.Namespace, multiplier, m.InputCostPerMillionTokens, m.OutputCostPerMillionTokens)
}

// Made with Bob
