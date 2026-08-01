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

// InferenceServerMetricResult holds one window-aggregated scheduler metric
// value for a single model-server pod. These are the saturation signals
// standardized by the Gateway API Inference Extension Model Server Protocol
// (queue depth, running requests, KV-cache utilization).
//
// Identity is the pod UID, not the (namespace, pod) name pair: names are
// reused across a pod's lifetime, and every other entity in the KubeModel
// joins by UID. ModelName is a dimension of the measurement rather than part
// of the identity, since it names what the pod is serving.
type InferenceServerMetricResult struct {
	ModelName    string
	PodUID       string
	NamespaceUID string
	Value        float64
}

// Made with Bob
