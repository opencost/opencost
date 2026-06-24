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
	// AllocationMethodComputeTime splits costs proportionally by vLLM prefill and
	// decode time. inputCostPerMillionTokens uses PromptTokens as the denominator
	// (delivered tokens); see cacheSavingsFraction for KV cache benefit.
	AllocationMethodComputeTime AllocationMethod = "compute_time"

	// AllocationMethodPrefixCachingOff splits by input / output token time; prefix
	// caching is explicitly disabled on the vLLM instance. cacheSavingsFraction
	// will be zero by configuration, not by absence of cache hits.
	AllocationMethodPrefixCachingOff AllocationMethod = "prefix_caching_off"

	// AllocationMethodMultiplier splits using a fixed output/input cost multiplier
	// (vLLM timing metrics unavailable).
	AllocationMethodMultiplier AllocationMethod = "multiplier"
)

// InferenceCostProperties identifies a unique inference cost entity.
type InferenceCostProperties struct {
	ModelName      string
	ModelVersion   string
	Namespace      string
	Cluster        string
	Pod            string
	Controller     string
	ControllerKind string
	Container      string
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
	// CachedTokens is the number of prompt tokens served from the KV cache,
	// sourced directly from vllm:prefix_cache_hits_total (token-level counter).
	// Zero when the metric is unavailable.
	CachedTokens float64
	// PrefixCachingEnabled reflects the enable_prefix_caching label from
	// vllm:cache_config_info. False when the metric is absent.
	PrefixCachingEnabled bool
	// CacheConfigKnown is true when vllm:cache_config_info was successfully
	// joined for this model. When false, PrefixCachingEnabled is meaningless.
	CacheConfigKnown bool

	// EffectiveInputTokens is PromptTokens - CachedTokens when cache correction
	// is enabled, otherwise equals PromptTokens.
	EffectiveInputTokens float64

	// CacheSavingsFraction is CachedTokens / PromptTokens, clamped to [0, 1].
	// Represents the fraction of prompt tokens served from the KV cache. Zero
	// when PromptTokens is zero or prefix caching is disabled. The raw ratio can
	// exceed 1 in high-reuse workloads; see apitypes.go for the full explanation.
	CacheSavingsFraction float64

	// InputCost and OutputCost are the dollar amounts attributed to input and
	// output processing respectively.
	InputCost  map[CostBasis]float64
	OutputCost map[CostBasis]float64

	// AllocationMethod records which input/output split path was used.
	AllocationMethod AllocationMethod

	// Derived cost-per-million-token metrics, keyed by CostBasis.
	// Blended (input+output together), using TotalTokens as denominator.
	CostPerMillionTokens map[CostBasis]float64
	// Per-million input tokens, using PromptTokens as denominator.
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
