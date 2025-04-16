package collector

import (
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/modules/collector-source/pkg/metrics/parser"
	"github.com/opencost/opencost/modules/collector-source/pkg/metrics/target"
)

type TargetScraper struct {
	targetProvider target.TargetProvider
	collector      MetricsCollector
}

func (s *TargetScraper) Scrape() {
	targets := s.targetProvider.GetTargets()
	for _, target := range targets {
		f, err := target.Load()
		if err != nil {
			log.Errorf("failed to scrape target: %s", err.Error())
			continue
		}
		results, err := parser.Parse(f)
		if err != nil {
			log.Errorf("failed to parse target: %s", err.Error())
			continue
		}

		for _, result := range results {
			s.collector.Update(result.Name, result.Labels, result.Value, result.Timestamp, nil)
		}
	}
}
