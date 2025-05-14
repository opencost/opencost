package collector

import (
	"time"

	"github.com/opencost/opencost/modules/collector-source/pkg/env"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
)

type CollectorConfig struct {
	Resolutions    []util.ResolutionConfiguration `json:"resolutions"`
	ScrapeInterval time.Duration                  `json:"scrape_interval"`
	ClusterID      string                         `json:"cluster_id"`
	ReleaseName    string                         `json:"release_name"`
	NetworkPort    int                            `json:"network_port"`
}

func NewOpenCostCollectorConfigFromEnv() CollectorConfig {
	return CollectorConfig{
		Resolutions: []util.ResolutionConfiguration{
			{
				Interval:  "10m",
				Retention: env.GetCollector10mResolutionRetention(),
			},
			{
				Interval:  "1h",
				Retention: env.GetCollector1hResolutionRetention(),
			},
			{
				Interval:  "1d",
				Retention: env.GetCollection1dResolutionRetention(),
			},
		},
		ScrapeInterval: time.Second * time.Duration(env.GetCollectorScrapeIntervalSeconds()),
		ClusterID:      env.GetClusterID(),
		ReleaseName:    env.GetReleaseName(),
		NetworkPort:    env.GetNetworkPort(),
	}
}
