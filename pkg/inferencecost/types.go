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
	// AllocationMethodComputeTimeWithCacheHits splits costs by input / output token
	// time with KV cache denominator correction applied. Most accurate.  
	AllocationMethodComputeTimeWithCacheHits AllocationMethod = "compute_time_with_cache_hits"

	// AllocationMethodComputeTime splits input / output token time without cache
	// correction. Used when block size is unknown (cache_config_info join failed or
	// metric absent) or when there were no cache hits in the window. Causes artificial decrease in 
	// per million token costs for input tokens.
	AllocationMethodComputeTime AllocationMethod = "compute_time"

	// AllocationMethodPrefixCachingOff splits by input / output token time; prefix
	// caching is explicitly disabled on the vLLM instance so cache correction is
	// not applicable.
	AllocationMethodPrefixCachingOff AllocationMethod = "prefix_caching_off"

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
	// BlockSize is the tokens-per-block value from vllm:cache_config_info.
	// Zero when the cache_config_info metric is absent or the pod join failed.
	BlockSize float64
	// PrefixCachingEnabled reflects the enable_prefix_caching label from
	// vllm:cache_config_info. False when the metric is absent.
	PrefixCachingEnabled bool
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
	// Configurable via INFERENCE_COLLECTION_INTERVAL environment variable.
	// Default is 2 minutes to match the core metrics emitter query window.
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

	// AllocationMode controls the input/output split method.
	// "compute_time" uses vLLM timing metrics (preferred).
	// "multiplier" uses a fixed ratio (fallback).
	AllocationMode string

	// OutputTokenCostMultiplier is the output/input cost ratio used when
	// AllocationMode is "multiplier".
	OutputTokenCostMultiplier float64

}

const (
	AllocationModeComputeTime = "compute_time"
	AllocationModeMultiplier  = "multiplier"

	defaultOutputTokenCostMultiplier = 2.5
	defaultModelLabel                = "llm-d.ai/model"
	defaultSharedInfraLabel          = "llm-d.ai/inference-shared"
	defaultSharedInfraLabelValue     = "true"
)

// DefaultConfig returns a Config populated from environment variables via the
// env package. Callers should check Enabled before starting the collector.
func DefaultConfig() *Config {
	return &Config{
		PrometheusURL:             getPrometheusURL(),
		CollectionInterval:        getCollectionInterval(),
		Enabled:                   isInferenceCostEnabled(),
		ModelLabel:                getModelLabel(),
		SharedInfraLabel:          getSharedInfraLabel(),
		SharedInfraLabelValue:     getSharedInfraLabelValue(),
		AllocationMode:            AllocationModeComputeTime,
		OutputTokenCostMultiplier: defaultOutputTokenCostMultiplier,
	}
}
