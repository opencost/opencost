package scrape

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
