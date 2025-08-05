package env

import (
	"github.com/opencost/opencost/core/pkg/env"
)

const (
	NetworkPortEnvVar               = "NETWORK_PORT"
	Collector10mResolutionRetention = "COLLECTOR_10M_RESOLUTION_RETENTION"
	Collector1hResolutionRetention  = "COLLECTOR_1H_RESOLUTION_RETENTION"
	Collection1dResolutionRetention = "COLLECTOR_1D_RESOLUTION_RETENTION"
	CollectorScrapeInterval         = "COLLECTOR_SCRAPE_INTERVAL"
)

func GetNetworkPort() int {
	return env.GetInt(NetworkPortEnvVar, 3001)
}

func GetCollector10mResolutionRetention() int {
	return env.GetInt(Collector10mResolutionRetention, 36)
}

func GetCollector1hResolutionRetention() int {
	return env.GetInt(Collector1hResolutionRetention, 49)
}

func GetCollection1dResolutionRetention() int {
	return env.GetInt(Collection1dResolutionRetention, 15)
}

func GetCollectorScrapeIntervalSeconds() string {
	return env.Get(CollectorScrapeInterval, "30s")
}
