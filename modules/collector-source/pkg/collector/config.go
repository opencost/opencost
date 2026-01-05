package collector

import (
	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/modules/collector-source/pkg/env"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
)

type CollectorConfig struct {
	Resolutions     []util.ResolutionConfiguration
	ScrapeInterval  string
	ClusterUID      string
	ClusterName     string
	ApplicationName string
	NetworkPort     int
}

func NewOpenCostCollectorConfigFromEnv(clusterUID string) CollectorConfig {
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
		ScrapeInterval:  env.GetCollectorScrapeIntervalSeconds(),
		ClusterUID:      clusterUID,
		ClusterName:     coreenv.GetClusterID(),
		ApplicationName: coreenv.GetAppName(),
		NetworkPort:     env.GetNetworkPort(),
	}
}
