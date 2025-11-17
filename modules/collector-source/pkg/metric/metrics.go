package metric

const (
	// Cluster Cache Metrics
	KubeNodeStatusCapacityCPUCores                        = "kube_node_status_capacity_cpu_cores"
	KubeNodeStatusCapacityMemoryBytes                     = "kube_node_status_capacity_memory_bytes"
	KubeNodeStatusAllocatableCPUCores                     = "kube_node_status_allocatable_cpu_cores"
	KubeNodeStatusAllocatableMemoryBytes                  = "kube_node_status_allocatable_memory_bytes"
	KubeNodeLabels                                        = "kube_node_labels"
	KubePodLabels                                         = "kube_pod_labels"
	KubePodAnnotations                                    = "kube_pod_annotations"
	KubePodOwner                                          = "kube_pod_owner"
	KubePodContainerStatusRunning                         = "kube_pod_container_status_running"
	KubePodContainerResourceRequests                      = "kube_pod_container_resource_requests"
	KubePodContainerResourceLimits                        = "kube_pod_container_resource_limits"
	KubePersistentVolumeClaimInfo                         = "kube_persistentvolumeclaim_info"
	KubePersistentVolumeClaimResourceRequestsStorageBytes = "kube_persistentvolumeclaim_resource_requests_storage_bytes"
	KubecostPVInfo                                        = "kubecost_pv_info"
	KubePersistentVolumeCapacityBytes                     = "kube_persistentvolume_capacity_bytes"
	DeploymentMatchLabels                                 = "deployment_match_labels"
	KubeNamespaceLabels                                   = "kube_namespace_labels"
	KubeNamespaceAnnotations                              = "kube_namespace_annotations"
	ServiceSelectorLabels                                 = "service_selector_labels"
	StatefulSetMatchLabels                                = "statefulSet_match_labels"
	KubeReplicasetOwner                                   = "kube_replicaset_owner"
	KubeResourceQuotaSpecResourceRequests                 = "resourcequota_spec_resource_requests"
	KubeResourceQuotaSpecResourceLimits                   = "resourcequota_spec_resource_limits"
	KubeResourceQuotaStatusUsedResourceRequests           = "resourcequota_status_used_resource_requests"
	KubeResourceQuotaStatusUsedResourceLimits             = "resourcequota_status_used_resource_limits"

	// DCGM Metrics
	DCGMFIPROFGRENGINEACTIVE = "DCGM_FI_PROF_GR_ENGINE_ACTIVE"
	DCGMFIDEVDECUTIL         = "DCGM_FI_DEV_DEC_UTIL"

	// Network Metrics
	KubecostPodNetworkEgressBytesTotal  = "kubecost_pod_network_egress_bytes_total"
	KubecostPodNetworkIngressBytesTotal = "kubecost_pod_network_ingress_bytes_total"

	// Opencost Metrics
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

	// Stat Summary Metrics
	NodeCPUSecondsTotal                = "node_cpu_seconds_total"
	NodeFSCapacityBytes                = "node_fs_capacity_bytes" // replaces container_fs_limit_bytes
	ContainerNetworkReceiveBytesTotal  = "container_network_receive_bytes_total"
	ContainerNetworkTransmitBytesTotal = "container_network_transmit_bytes_total"
	ContainerCPUUsageSecondsTotal      = "container_cpu_usage_seconds_total"
	ContainerMemoryWorkingSetBytes     = "container_memory_working_set_bytes"
	ContainerFSUsageBytes              = "container_fs_usage_bytes"
	KubeletVolumeStatsUsedBytes        = "kubelet_volume_stats_used_bytes"
)
