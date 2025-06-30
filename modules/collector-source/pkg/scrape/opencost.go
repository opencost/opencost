package scrape

import (
	"github.com/opencost/opencost/modules/collector-source/pkg/constants"
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
		provider,
		[]string{
			constants.KubecostClusterManagementCost,
			constants.KubecostNetworkZoneEgressCost,
			constants.KubecostNetworkRegionEgressCost,
			constants.KubecostNetworkInternetEgressCost,
			constants.PVHourlyCost,
			constants.KubecostLoadBalancerCost,
			constants.NodeTotalHourlyCost,
			constants.NodeCPUHourlyCost,
			constants.NodeRAMHourlyCost,
			constants.NodeGPUHourlyCost,
			constants.NodeGPUCount,
			constants.KubecostNodeIsSpot,
			constants.ContainerCPUAllocation,
			constants.ContainerMemoryAllocationBytes,
			constants.ContainerGPUAllocation,
			constants.PodPVCAllocation,
		},
		true)
}
