package scrape

import (
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/scrape/target"
)

// Opencost Metrics
const (
	KubecostClusterManagementCost     = "kubecost_cluster_management_cost"
	KubecostNetworkZoneEgressCost     = "kubecost_network_zone_egress_cost"
	KubecostNetworkRegionEgressCost   = "kubecost_network_region_egress_cost"
	KubecostNetworkInternetEgressCost = "kubecost_network_internet_egress_cost"
	PVHourlyCost                      = "pv_hourly_cost"
	KubecostLoadBalancerCost          = "kubecost_load_balancer_cost"
	NodeTotalHourlyCost               = "node_total_hourly_cost"
	NodeCPUHourlyCost                 = "node_cpu_hourly_cost"
	NodeRAMHourlyCost                 = "node_ram_hourly_cost"
	NodeGPUHourlyCost                 = "node_gpu_hourly_cost"
	NodeGPUCount                      = "node_gpu_count"
	KubecostNodeIsSpot                = "kubecost_node_is_spot"
	ContainerCPUAllocation            = "container_cpu_allocation"
	ContainerMemoryAllocationBytes    = "container_memory_allocation_bytes"
	ContainerGPUAllocation            = "container_gpu_allocation"
	PodPVCAllocation                  = "pod_pvc_allocation"
)

func newOpenCostTargetProvider() target.TargetProvider {
	return nil
}

func newOpenCostScraper(updater metric.MetricUpdater) Scraper {
	return newOpencostTargetScraper(newOpenCostTargetProvider(), updater)
}

func newOpencostTargetScraper(provider target.TargetProvider, updater metric.MetricUpdater) *TargetScraper {
	return newTargetScrapper(
		provider,
		updater,
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
