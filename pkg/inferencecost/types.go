package inferencecost

import "time"

// CostAllocationMode defines how costs are allocated between input and output tokens
type CostAllocationMode string

const (
	// ModeComputeTime allocates costs based on actual processing time (most accurate)
	ModeComputeTime CostAllocationMode = "compute_time"
	// ModeMultiplier applies a fixed multiplier to output tokens (simple fallback)
	ModeMultiplier CostAllocationMode = "multiplier"
)

// ModelMetrics holds metrics for a specific model in a specific namespace
type ModelMetrics struct {
	ModelName    string
	ModelVersion string
	Namespace    string

	// Token metrics
	PromptTokens     float64
	GenerationTokens float64
	TotalTokens      float64

	// Time metrics (for compute-time based allocation)
	InputProcessingTime  float64 // Total seconds spent processing input tokens
	OutputProcessingTime float64 // Total seconds spent generating output tokens

	// Cost metrics
	GPUCost   float64
	TotalCost float64

	// Calculated metrics (blended - for backward compatibility)
	CostPerToken         float64
	CostPerMillionTokens float64

	// Differentiated cost metrics
	InputCost                  float64 // Total cost allocated to input processing
	OutputCost                 float64 // Total cost allocated to output generation
	InputCostPerToken          float64
	OutputCostPerToken         float64
	InputCostPerMillionTokens  float64
	OutputCostPerMillionTokens float64

	// Metadata
	Timestamp time.Time
}

// Config holds configuration for the inference cost collector
type Config struct {
	PrometheusURL             string
	CollectionInterval        time.Duration
	Enabled                   bool
	AllocationMode            CostAllocationMode
	OutputTokenCostMultiplier float64 // Used when AllocationMode is ModeMultiplier
}

// Made with Bob
