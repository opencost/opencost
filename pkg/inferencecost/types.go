package inferencecost

import "time"

// CostBasis defines whether costs are usage-based or allocation-based.
type CostBasis string

const (
	// CostBasisUsage measures actual resource consumption only.
	// Does not reconcile to the infrastructure bill — idle and waste are unattributed.
	CostBasisUsage CostBasis = "usage"

	// CostBasisAllocation measures max(request,usage) × runtime × price plus idle
	// (ShareWeighted) and shared infra (EPP, gateway). Reconciles to the bill.
	CostBasisAllocation CostBasis = "allocation"
)

// AllocationMethod indicates how the input/output cost split was computed.
type AllocationMethod string

const (
	// AllocationMethodComputeTime splits costs by vLLM prefill/decode time, with
	// KV cache denominator correction applied.
	AllocationMethodComputeTime AllocationMethod = "compute_time"

	// AllocationMethodComputeTimeUncorrected splits by compute time but uses
	// PromptTokens as the denominator (KVCacheBlockSize not configured or
	// prefix_cache_hits_total unavailable).
	AllocationMethodComputeTimeUncorrected AllocationMethod = "compute_time_uncorrected"

	// AllocationMethodMultiplier splits using a fixed output/input cost multiplier
	// (vLLM timing metrics unavailable).
	AllocationMethodMultiplier AllocationMethod = "multiplier"
)

// InferenceCostProperties identifies a unique inference cost entity.
type InferenceCostProperties struct {
	ModelName    string
	ModelVersion string
	Namespace    string
	Cluster      string
}

// InferenceCost holds all cost data for a single model/namespace over a
// collection interval.
type InferenceCost struct {
	Properties InferenceCostProperties

	// Costs from OpenCost allocation layer.
	// AllocationTotalCost = max(request,usage)×price + idle share + shared infra share.
	AllocationTotalCost float64
	// UsageTotalCost = actual_usage×price only; does not reconcile to bill.
	UsageTotalCost float64

	// Token counts from vLLM Prometheus metrics.
	PromptTokens     float64
	GenerationTokens float64
	TotalTokens      float64

	// Processing times from vLLM Prometheus metrics (seconds in collection window).
	InputProcessingTime  float64
	OutputProcessingTime float64

	// KV cache data from vLLM Prometheus metrics.
	// CacheHitBlocks is zero when prefix_cache_hits_total is unavailable.
	CacheHitBlocks float64
	// BlockSize is the deployment config constant (tokens per KV block).
	// Zero means cache correction is disabled.
	BlockSize float64
	// CachedTokens = CacheHitBlocks * BlockSize (derived in combineMetrics).
	CachedTokens float64

	// EffectiveInputTokens is PromptTokens - CachedTokens when cache correction
	// is enabled, otherwise equals PromptTokens.
	EffectiveInputTokens float64

	// AllocationMethod records which input/output split path was used.
	AllocationMethod AllocationMethod

	// Derived cost-per-million-token metrics, keyed by CostBasis.
	// Blended (input+output together), using TotalTokens as denominator.
	CostPerMillionTokens map[CostBasis]float64
	// Per-million input tokens, using EffectiveInputTokens as denominator.
	InputCostPerMillionTokens map[CostBasis]float64
	// Per-million output tokens, using GenerationTokens as denominator.
	OutputCostPerMillionTokens map[CostBasis]float64

	Timestamp time.Time
}

// Config holds configuration for the inference cost collector.
type Config struct {
	// PrometheusURL is the Prometheus server endpoint for vLLM metric queries.
	PrometheusURL string

	// CollectionInterval is how often metrics are collected.
	CollectionInterval time.Duration

	// Enabled controls whether the inference cost collector runs.
	Enabled bool

	// ModelLabel is the Kubernetes pod label whose value equals the vLLM
	// model_name metric label. Used to aggregate allocation costs by model.
	ModelLabel string

	// SharedInfraLabel and SharedInfraLabelValue identify shared inference
	// infrastructure pods (EPP, gateway) that lack ModelLabel.
	SharedInfraLabel      string
	SharedInfraLabelValue string

	// KVCacheBlockSize is the number of tokens per KV cache block, matching the
	// vLLM --block-size deployment parameter. Zero disables cache correction.
	KVCacheBlockSize float64

	// AllocationMode controls the input/output split method.
	// "compute_time" uses vLLM timing metrics (preferred).
	// "multiplier" uses a fixed ratio (fallback).
	AllocationMode string

	// OutputTokenCostMultiplier is the output/input cost ratio used when
	// AllocationMode is "multiplier".
	OutputTokenCostMultiplier float64

	// UsageCostShareSplit controls how shared infrastructure costs are handled
	// for usage-based costs (cost_basis=usage).
	// "none" (default): Shared costs are not included in usage costs.
	// "weighted": Shared costs are distributed proportionally (like allocation costs).
	// "even": Shared costs are distributed evenly across models.
	// Note: Allocation costs always use ShareWeighted for shared infrastructure.
	UsageCostShareSplit string
}

const (
	AllocationModeComputeTime = "compute_time"
	AllocationModeMultiplier  = "multiplier"

	// UsageCostShareSplit options
	UsageCostShareSplitNone     = "none"
	UsageCostShareSplitWeighted = "weighted"
	UsageCostShareSplitEven     = "even"

	defaultOutputTokenCostMultiplier = 2.5
	defaultCollectionInterval        = 5 * time.Minute
	defaultModelLabel                = "llm-d.ai/model"
	defaultSharedInfraLabel          = "llm-d.ai/inference-serving"
	defaultSharedInfraLabelValue     = "true"
	defaultUsageCostShareSplit       = UsageCostShareSplitNone
)

// DefaultConfig returns a Config populated from environment variables via the
// env package. Callers should check Enabled before starting the collector.
func DefaultConfig() *Config {
	return &Config{
		PrometheusURL:             getPrometheusURL(),
		CollectionInterval:        defaultCollectionInterval,
		Enabled:                   isInferenceCostEnabled(),
		ModelLabel:                getModelLabel(),
		SharedInfraLabel:          getSharedInfraLabel(),
		SharedInfraLabelValue:     getSharedInfraLabelValue(),
		KVCacheBlockSize:          getKVCacheBlockSize(),
		AllocationMode:            AllocationModeComputeTime,
		OutputTokenCostMultiplier: defaultOutputTokenCostMultiplier,
		UsageCostShareSplit:       defaultUsageCostShareSplit,
	}
}
