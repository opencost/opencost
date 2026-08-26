package env

import (
	"github.com/opencost/opencost/core/pkg/env"
)

const (
	CollectorEnvVarPrefix   = "COLLECTOR_"
	CollectorScrapeInterval = "COLLECTOR_SCRAPE_INTERVAL"
	NetworkPortEnvVar       = "NETWORK_PORT"

	// InferenceModelLabelEnvVar names the pod label whose presence identifies
	// a model-server pod and whose value is the served model name. The default
	// matches the label used by the inference cost feature (see
	// InferenceModelLabelEnvVar in the root module's pkg/env, which this
	// module cannot import) and by llm-d.
	InferenceModelLabelEnvVar = "INFERENCE_MODEL_LABEL"

	// InferenceScrapePortEnvVar overrides the default metrics port used when a
	// model-server pod carries no prometheus.io/port annotation. The default
	// matches the vLLM OpenAI-compatible server port.
	InferenceScrapePortEnvVar = "INFERENCE_SCRAPE_PORT"
)

func GetNetworkPort() int {
	return env.GetInt(NetworkPortEnvVar, 3001)
}

// GetInferenceModelLabel returns the pod label key that identifies model-server
// pods for inference scraping.
func GetInferenceModelLabel() string {
	return env.Get(InferenceModelLabelEnvVar, "llm-d.ai/model")
}

// GetInferenceScrapePort returns the metrics port used for model-server pods
// that carry no prometheus.io/port annotation.
func GetInferenceScrapePort() int {
	return env.GetInt(InferenceScrapePortEnvVar, 8000)
}

func GetCollectorResolution10mRetention() int {
	return env.GetPrefixInt(CollectorEnvVarPrefix, env.Resolution10mRetentionEnvVar, 36)
}

func GetCollectorResolution1hRetention() int {
	return env.GetPrefixInt(CollectorEnvVarPrefix, env.Resolution1hRetentionEnvVar, 49)
}

func GetCollectionResolution1dRetention() int {
	return env.GetPrefixInt(CollectorEnvVarPrefix, env.Resolution1dRetentionEnvVar, 15)
}

func GetCollectorScrapeIntervalSeconds() string {
	return env.Get(CollectorScrapeInterval, "30s")
}
