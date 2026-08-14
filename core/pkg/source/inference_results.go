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

// InferenceEngineMetricResult holds one window-aggregated scheduler metric
// value for a single model-server pod. These are the saturation signals
// standardized by the Gateway API Inference Extension Model Server Protocol
// (queue depth, running requests, KV-cache utilization).
//
// Identity is (pod UID, engine index), not the (namespace, pod) name pair:
// names are reused across a pod's lifetime, and every other entity in the
// KubeModel joins by UID. The engine index is part of identity because one pod
// can run several engine cores: vLLM labels every metric with
// ["model_name", "engine"], so a data-parallel deployment emits one series per
// rank from the same pod, and keying on pod UID alone would keep only one of
// them. A single-engine deployment reports "0".
//
// ModelName is a dimension of the measurement rather than part of the
// identity, since it names what the engine is serving.
type InferenceEngineMetricResult struct {
	ModelName    string
	PodUID       string
	NamespaceUID string
	EngineIndex  string
	Value        float64
}

// Made with Bob
