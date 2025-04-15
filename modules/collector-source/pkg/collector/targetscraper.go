package collector

import (
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/modules/collector-source/pkg/metrics/parser"
	"github.com/opencost/opencost/modules/collector-source/pkg/metrics/target"
)

type TargetScraper struct {
	targetProvider target.TargetProvider
	collector      MetricsCollector
	metrics        map[string]struct{} // filter for which metrics will be processed
	includeMetrics bool                // toggle to make metrics an include or exclude list
}

func NewTargetScrapper(provider target.TargetProvider, collector MetricsCollector, metrics []string, includeMetrics bool) *TargetScraper {
	metricSet := make(map[string]struct{})
	for _, metric := range metrics {
		metricSet[metric] = struct{}{}
	}
	return &TargetScraper{
		targetProvider: provider,
		collector:      collector,
		metrics:        metricSet,
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
			if _, ok := s.metrics[result.Name]; ok != s.includeMetrics {
				continue
			}
			s.collector.Update(result.Name, result.Labels, result.Value, result.Timestamp, nil)
		}
	}
}

func NewOpencostTargetScraper(provider target.TargetProvider, collector MetricsCollector) *TargetScraper {
	return NewTargetScrapper(
		provider,
		collector,
		[]string{
			KubecostClusterManagementCost,
			KubecostNetworkZoneEgressCost,
			KubecostNetworkRegionEgressCost,
			KubecostNetworkInternetEgressCost,
			PVHourlyCost,
			KubecostLoadBalancerCost,
			NodeTotalHourlyCost,
			NodeCPUHourlyCost,
			NodeRAMHourlyCost,
			NodeGPUHourlyCost,
			NodeGPUCount,
			KubecostNodeIsSpot,
			ContainerCPUAllocation,
			ContainerMemoryAllocationBytes,
			ContainerGPUAllocation,
			PodPVCAllocation,
		},
		true)
}
