package inferencecost

import "fmt"

// Calculator calculates inference costs
type Calculator struct{}

// NewCalculator creates a new cost calculator
func NewCalculator() *Calculator {
	return &Calculator{}
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
		return nil
	}

	// Calculate cost per token
	m.CostPerToken = m.TotalCost / m.TotalTokens

	// Calculate cost per million tokens
	m.CostPerMillionTokens = m.CostPerToken * 1000000

	return nil
}

// Made with Bob
