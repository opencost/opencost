package collector

import (
	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/modules/collector-source/pkg/env"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
)

type CollectorConfig struct {
	Resolutions    []util.ResolutionConfiguration `json:"resolutions"`
	ScrapeInterval string                         `json:"scrape_interval"`
	ClusterID      string                         `json:"cluster_id"`
	NetworkPort    int                            `json:"network_port"`
}

func NewOpenCostCollectorConfigFromEnv() CollectorConfig {
	return CollectorConfig{
		Resolutions: []util.ResolutionConfiguration{
			{
				Interval:  "10m",
				Retention: env.GetCollectorResolution10mRetention(),
			},
			{
				Interval:  "1h",
				Retention: env.GetCollectorResolution1hRetention(),
			},
			{
				Interval:  "1d",
				Retention: env.GetCollectionResolution1dRetention(),
			},
		},
		ScrapeInterval: env.GetCollectorScrapeIntervalSeconds(),
		ClusterID:      coreenv.GetClusterID(),
		NetworkPort:    env.GetNetworkPort(),
	}
}
