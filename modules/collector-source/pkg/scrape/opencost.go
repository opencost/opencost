package scrape

import (
	"github.com/opencost/opencost/modules/collector-source/pkg/event"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/scrape/target"
)

func newOpenCostTargetProvider() target.TargetProvider {
	// localhost is used here because we are hitting an endpoint of this container
	return target.NewDefaultTargetProvider(target.NewUrlTarget("http://localhost:9003/metrics"))
}

func newOpenCostScraper() Scraper {
	return newOpencostTargetScraper(newOpenCostTargetProvider())
}

func newOpencostTargetScraper(provider target.TargetProvider) *TargetScraper {
	return newTargetScrapper(
		event.OpenCostScraperName,
		provider,
		[]string{
			metric.KubecostClusterManagementCost,
			metric.KubecostNetworkZoneEgressCost,
			metric.KubecostNetworkRegionEgressCost,
			metric.KubecostNetworkInternetEgressCost,
			metric.PVHourlyCost,
			metric.KubecostLoadBalancerCost,
			metric.NodeTotalHourlyCost,
			metric.NodeCPUHourlyCost,
			metric.NodeRAMHourlyCost,
			metric.NodeGPUHourlyCost,
			metric.NodeGPUCount,
			metric.KubecostNodeIsSpot,
			metric.ContainerCPUAllocation,
			metric.ContainerMemoryAllocationBytes,
			metric.ContainerGPUAllocation,
			metric.PodPVCAllocation,
		},
		true)
}
