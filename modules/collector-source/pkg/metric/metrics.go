package metric

const (
	// Cluster Cache Metrics
	ClusterInfo                                           = "cluster_info"
	NodeInfo                                              = "node_info"
	NodeResourceCapacities                                = "node_resource_capacities"
	NodeResourcesAllocatable                              = "node_resources_allocatable"
	PodInfo                                               = "pod_info"
	PodPVCVolume                                          = "pod_pvc_volume"
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
	DeploymentInfo                                        = "deployment_info"
	DeploymentLabels                                      = "deployment_labels"
	DeploymentAnnotations                                 = "deployment_annotations"
	DeploymentMatchLabels                                 = "deployment_match_labels"
	StatefulSetInfo                                       = "statefulset_info"
	StatefulSetLabels                                     = "statefulset_labels"
	StatefulSetAnnotations                                = "statefulset_annotations"
	StatefulSetMatchLabels                                = "statefulSet_match_labels"
	DaemonSetInfo                                         = "daemonset_info"
	DaemonSetLabels                                       = "daemonset_labels"
	DaemonSetAnnotations                                  = "daemonset_annotations"
	JobInfo                                               = "job_info"
	JobLabels                                             = "job_labels"
	JobAnnotations                                        = "job_annotations"
	CronJobInfo                                           = "cronjob_info"
	CronJobLabels                                         = "cronjob_labels"
	CronJobAnnotations                                    = "cronjob_annotations"
	ReplicaSetInfo                                        = "replicaset_info"
	ReplicaSetLabels                                      = "replicaset_labels"
	ReplicaSetAnnotations                                 = "replicaset_annotations"
	NamespaceInfo                                         = "namespace_info"
	KubeNamespaceLabels                                   = "kube_namespace_labels"
	KubeNamespaceAnnotations                              = "kube_namespace_annotations"
	ServiceInfo                                           = "service_info"
	ServiceSelectorLabels                                 = "service_selector_labels"
	KubeReplicasetOwner                                   = "kube_replicaset_owner"
	ContainerCPUAllocation                                = "container_cpu_allocation"
	ContainerMemoryAllocationBytes                        = "container_memory_allocation_bytes"
	ContainerGPUAllocation                                = "container_gpu_allocation"
	PodPVCAllocation                                      = "pod_pvc_allocation"
	ResourceQuotaInfo                                     = "resourcequota_info"
	KubeResourceQuotaSpecResourceRequests                 = "resourcequota_spec_resource_requests"
	KubeResourceQuotaSpecResourceLimits                   = "resourcequota_spec_resource_limits"
	KubeResourceQuotaStatusUsedResourceRequests           = "resourcequota_status_used_resource_requests"
	KubeResourceQuotaStatusUsedResourceLimits             = "resourcequota_status_used_resource_limits"

	// DCGM Metrics
	DCGMFIPROFGRENGINEACTIVE = "DCGM_FI_PROF_GR_ENGINE_ACTIVE"
	DCGMFIDEVDECUTIL         = "DCGM_FI_DEV_DEC_UTIL"

	// DCGM saturation metrics (default dcgm-exporter configuration)
	DCGMFIDEVPOWERVIOLATION      = "DCGM_FI_DEV_POWER_VIOLATION"
	DCGMFIDEVTHERMALVIOLATION    = "DCGM_FI_DEV_THERMAL_VIOLATION"
	DCGMFIDEVSYNCBOOSTVIOLATION  = "DCGM_FI_DEV_SYNC_BOOST_VIOLATION"
	DCGMFIDEVBOARDLIMITVIOLATION = "DCGM_FI_DEV_BOARD_LIMIT_VIOLATION"
	DCGMFIDEVFBUSED              = "DCGM_FI_DEV_FB_USED"
	DCGMFIDEVFBFREE              = "DCGM_FI_DEV_FB_FREE"
	DCGMFIDEVXIDERRORS           = "DCGM_FI_DEV_XID_ERRORS"
	DCGMFIDEVPOWERUSAGE          = "DCGM_FI_DEV_POWER_USAGE"
	DCGMFIDEVGPUTEMP             = "DCGM_FI_DEV_GPU_TEMP"

	// DCGM saturation metrics requiring explicit enablement in the
	// dcgm-exporter configuration. The clock throttle reasons bitmask was
	// renamed in DCGM 3.3+; both names are scraped, at most one exists.
	DCGMFIDEVCLOCKTHROTTLEREASONS = "DCGM_FI_DEV_CLOCK_THROTTLE_REASONS"
	DCGMFIDEVCLOCKSEVENTREASONS   = "DCGM_FI_DEV_CLOCKS_EVENT_REASONS"

	// DCGM DCP profiling saturation metrics (require Volta+ GPUs;
	// SM_ACTIVE, SM_OCCUPANCY, and NVLINK additionally require explicit
	// enablement in the dcgm-exporter configuration)
	DCGMFIPROFDRAMACTIVE    = "DCGM_FI_PROF_DRAM_ACTIVE"
	DCGMFIPROFSMACTIVE      = "DCGM_FI_PROF_SM_ACTIVE"
	DCGMFIPROFSMOCCUPANCY   = "DCGM_FI_PROF_SM_OCCUPANCY"
	DCGMFIPROFPCIETXBYTES   = "DCGM_FI_PROF_PCIE_TX_BYTES"
	DCGMFIPROFPCIERXBYTES   = "DCGM_FI_PROF_PCIE_RX_BYTES"
	DCGMFIPROFNVLINKTXBYTES = "DCGM_FI_PROF_NVLINK_TX_BYTES"
	DCGMFIPROFNVLINKRXBYTES = "DCGM_FI_PROF_NVLINK_RX_BYTES"

	// Synthetic metrics generated from DCGM scrapes (see pkg/metric/synthetic)
	// OpencostGPUMemoryUsedRatio is the per-sample framebuffer occupancy
	// ratio FB_USED / (FB_USED + FB_FREE), joined per scrape
	OpencostGPUMemoryUsedRatio = "opencost_gpu_memory_used_ratio"

	// Network Metrics
	KubecostPodNetworkEgressBytesTotal  = "kubecost_pod_network_egress_bytes_total"
	KubecostPodNetworkIngressBytesTotal = "kubecost_pod_network_ingress_bytes_total"

	// Opencost Metrics
	KubecostClusterManagementCost        = "kubecost_cluster_management_cost"
	KubecostNetworkZoneEgressCost        = "kubecost_network_zone_egress_cost"
	KubecostNetworkRegionEgressCost      = "kubecost_network_region_egress_cost"
	KubecostNetworkInternetEgressCost    = "kubecost_network_internet_egress_cost"
	KubecostNetworkNatGatewayEgressCost  = "kubecost_network_nat_gateway_egress_cost"
	KubecostNetworkNatGatewayIngressCost = "kubecost_network_nat_gateway_ingress_cost"
	PVHourlyCost                         = "pv_hourly_cost"
	KubecostLoadBalancerCost             = "kubecost_load_balancer_cost"
	NodeTotalHourlyCost                  = "node_total_hourly_cost"
	NodeCPUHourlyCost                    = "node_cpu_hourly_cost"
	NodeRAMHourlyCost                    = "node_ram_hourly_cost"
	NodeGPUHourlyCost                    = "node_gpu_hourly_cost"
	NodeGPUCount                         = "node_gpu_count"
	KubecostNodeIsSpot                   = "kubecost_node_is_spot"

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
