package inferencecost

import "time"

// ModelMetrics holds metrics for a specific model in a specific namespace
type ModelMetrics struct {
	ModelName    string
	ModelVersion string
	Namespace    string

	// Token metrics
	PromptTokens     float64
	GenerationTokens float64
	TotalTokens      float64

	// Cost metrics
	GPUCost   float64
	TotalCost float64

	// Calculated metrics
	CostPerToken         float64
	CostPerMillionTokens float64

	// Metadata
	Timestamp time.Time
}

// Config holds configuration for the inference cost collector
type Config struct {
	PrometheusURL      string
	CollectionInterval time.Duration
	Enabled            bool
}

// Made with Bob
