package env

import (
	"github.com/opencost/opencost/core/pkg/env"
)

const (
	CollectorEnvVarPrefix   = "COLLECTOR_"
	CollectorScrapeInterval = "COLLECTOR_SCRAPE_INTERVAL"
	NetworkPortEnvVar       = "NETWORK_PORT"
)

func GetNetworkPort() int {
	return env.GetInt(NetworkPortEnvVar, 3001)
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
