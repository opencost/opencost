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

// Made with Bob
