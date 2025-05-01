package collector

import (
	"time"

	"github.com/opencost/opencost/modules/collector-source/pkg/util"
)

type CollectorConfig struct {
	Resolutions    []util.ResolutionConfiguration `json:"resolutions"`
	ScrapeInterval time.Duration                  `json:"scrape_interval"`
	ReleaseName    string                         `json:"release_name"`
	NetworkPort    int                            `json:"network_port"`
}

func DefaultCollectorConfig() CollectorConfig {
	return CollectorConfig{
		Resolutions: []util.ResolutionConfiguration{
			{
				Interval:  "10m",
				Retention: 36,
			},
			{
				Interval:  "1h",
				Retention: 24,
			},
			{
				Interval:  "1d",
				Retention: 15,
			},
		},
		ScrapeInterval: time.Second * 30,
	}
}
