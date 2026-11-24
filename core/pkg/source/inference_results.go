package source

// InferenceCacheConfig holds cache configuration for a model.
type InferenceCacheConfig struct {
	PrefixCachingEnabled bool
}

// InferenceTokensResult represents token counts.
// Used for prompt tokens, generation tokens, and cached tokens.
type InferenceTokensResult struct {
	// Values maps "model_name:namespace" to token count
	Values map[string]float64
}

// InferenceProcessingTimeResult represents processing time in seconds.
// Used for input processing time and output processing time.
type InferenceProcessingTimeResult struct {
	// Values maps "model_name:namespace" to processing time in seconds
	Values map[string]float64
}

// InferenceCacheConfigResult represents cache configuration.
type InferenceCacheConfigResult struct {
	// Configs maps "model_name:namespace" to cache configuration
	Configs map[string]*InferenceCacheConfig
}

// InferenceDimensionKey identifies a (producer, user_id, tenant_id, workload_id, serving_model, namespace)
// combination. Producer is part of the key so that per-producer checks survive until they have run.
// UserID is empty when the user label is not configured or absent from the series.
type InferenceDimensionKey struct {
	Producer     string // name of the producer mapping that emitted the series; "default" for a single env-var mapping
	UserID       string
	TenantID     string
	WorkloadID   string
	ServingModel string
	Namespace    string // deployment namespace
}

// InferenceDimensionTokens holds the prompt and completion token counts for one dimension key.
type InferenceDimensionTokens struct {
	PromptTokens     float64
	CompletionTokens float64

	// Estimated subtotals: tokens the producer flagged as estimates rather than exact counts.
	// Subsets of the totals above, carried separately so the exporter can compute the estimated share.
	EstimatedPromptTokens     float64
	EstimatedCompletionTokens float64
}

// InferenceDimensionResult holds per-dimension token counts from the attribution series.
type InferenceDimensionResult struct {
	Values map[InferenceDimensionKey]*InferenceDimensionTokens
}

// Made with Bob
