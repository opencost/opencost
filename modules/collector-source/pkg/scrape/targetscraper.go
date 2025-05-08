package scrape

import (
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/scrape/parser"
	"github.com/opencost/opencost/modules/collector-source/pkg/scrape/target"
)

type TargetScraper struct {
	targetProvider target.TargetProvider
	metricUpdater  metric.MetricUpdater
	metricNames    map[string]struct{} // filter for which metrics will be processed
	includeMetrics bool                // toggle to make metrics an include or exclude list
}

func newTargetScrapper(provider target.TargetProvider, updater metric.MetricUpdater, metricNames []string, includeMetrics bool) *TargetScraper {
	metricSet := make(map[string]struct{})
	for _, metricName := range metricNames {
		metricSet[metricName] = struct{}{}
	}
	return &TargetScraper{
		targetProvider: provider,
		metricUpdater:  updater,
		metricNames:    metricSet,
		includeMetrics: includeMetrics,
	}
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
			// filter metrics to be processed by name
			if _, ok := s.metricNames[result.Name]; ok != s.includeMetrics {
				continue
			}
			s.metricUpdater.Update(result.Name, result.Labels, result.Value, result.Timestamp, nil)
		}
	}
}
