package collector

//	avg(
//		avg_over_time(
//			pv_hourly_cost{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (cluster_id, persistentvolume, volumename, provider_id)

func NewPVPricePerGiBHourMetricCollector() *MetricCollector {
	return NewMetricCollector(
		PVPricePerGiBHourID,
		PVHourlyCost,
		[]string{"cluster", "persistentvolume", "volumename", "provider_id"},
		AverageOverTime,
	)
}

//	avg(
//		avg_over_time(
//			kubelet_volume_stats_used_bytes{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (cluster_id, persistentvolumeclaim, namespace)

func NewPVUsedAverageMetricCollector() *MetricCollector {
	return NewMetricCollector(
		PVUsedAverageID,
		KubeletVolumeStatsUsedBytes,
		[]string{"cluster", "persistentvolumeclaim", "namespace"},
		AverageOverTime,
	)
}

//	max(
//		max_over_time(
//			kubelet_volume_stats_used_bytes{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (cluster_id, persistentvolumeclaim, namespace)

func NewPVUsedMaxMetricCollector() *MetricCollector {
	return NewMetricCollector(
		PVUsedMaxID,
		KubeletVolumeStatsUsedBytes,
		[]string{"cluster", "persistentvolumeclaim", "namespace"},
		MaxOverTime,
	)
}

//	avg(
//		kube_persistentvolumeclaim_info{
//			volumename != "",
//			<some_custom_filter>
//		}
//	) by (persistentvolumeclaim, storageclass, volumename, namespace, cluster_id)[0:10m]

func NewPVCInfoMetricCollector() *MetricCollector {
	return NewMetricCollector(
		PVCInfoID,
		KubePersistenVolumeClaimInfo,
		[]string{"persistentvolumeclaim", "storageclass", "volumename", "namespace", "cluster"},
		Info,
	)
}

//	avg(
//		kube_persistentvolume_capacity_bytes{
//			<some_custom_filter>
//		}
//	) by (cluster_id, persistentvolume)[0:10m]

func NewPVActiveMinutesMetricCollector() *MetricCollector {
	return NewMetricCollector(
		PVActiveMinutesID,
		KubePersistentVolumeCapacityBytes,
		[]string{"cluster", "persistentvolume"},
		ActiveMinutes,
	)
}

// todo revisit this
//
//	sum_over_time(
//		sum(
//			container_fs_limit_bytes{
//				device=~"/dev/(nvme|sda).*",
//				id="/",
//				<some_custom_filter>
//			}
//		) by (instance, device, cluster_id)[%s:%dm]
//	) / 1024 / 1024 / 1024 * %f * %f
func NewLocalStorageCostMetricCollector() *MetricCollector {
	return NewMetricCollector(
		LocalStorageCostID,
		ContainerFSLimitBytes,
		[]string{"instance", "device", "cluster"},
		AverageOverTime,
	)
}

// sum_over_time(
//
//	sum(
//		container_fs_usage_bytes{
//			device=~"/dev/(nvme|sda).*",
//			id="/",
//			<some_custom_filter>
//		}
//	) by (instance, device, cluster_id)[%s:%dm]
//
// ) / 1024 / 1024 / 1024 * %f * %f`
func NewLocalStorageUsedCostMetricCollector() *MetricCollector {
	return NewMetricCollector(
		LocalStorageUsedCostID,
		ContainerFSUsageBytes,
		[]string{"instance", "device", "cluster"},
		AverageOverTime,
	)
}

//	avg(
//		sum(
//			avg_over_time(
//				container_fs_usage_bytes{
//					device=~"/dev/(nvme|sda).*",
//					id="/",
//					<some_custom_filter>
//				}[1h]
//			)
//		) by (instance, device, cluster_id, job)
//	) by (instance, device, cluster_id)

func NewLocalStorageUsedAverageMetricCollector() *MetricCollector {
	return NewMetricCollector(
		LocalStorageUsedAverageID,
		ContainerFSUsageBytes,
		[]string{"instance", "device", "cluster"},
		AverageOverTime,
	)
}

// max(
//
//	sum(
//		max_over_time(
//			container_fs_usage_bytes{
//				device=~"/dev/(nvme|sda).*",
//				id="/",
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (instance, device, cluster_id, job)
//
// ) by (instance, device, cluster_id)
func NewLocalStorageUsedMaxMetricCollector() *MetricCollector {
	return NewMetricCollector(
		LocalStorageUsedMaxID,
		ContainerFSUsageBytes,
		[]string{"instance", "device", "cluster"},
		MaxOverTime,
	)
}

// avg_over_time(
//
//	sum(
//		container_fs_limit_bytes{
//			device=~"/dev/(nvme|sda).*",
//			id="/",
//			<some_custom_filter>
//		}
//	) by (instance, device, cluster_id)[%s:%dm]
//
// )
func NewLocalStorageBytesMetricCollector() *MetricCollector {
	return NewMetricCollector(
		LocalStorageBytesID,
		ContainerFSLimitBytes,
		[]string{"instance", "device", "cluster"},
		AverageOverTime,
	)
}

// count(
//
//	node_total_hourly_cost{
//		<some_custom_filter>
//	}
//
// ) by (cluster_id, node, instance, provider_id)[%s:%dm]
func NewLocalStorageActiveMinutesMetricCollector() *MetricCollector {
	return NewMetricCollector(
		LocalStorageActiveMinutesID,
		NodeTotalHourlyCost,
		[]string{"cluster", "node", "instance", "provider_id"},
		ActiveMinutes,
	)
}

// avg(
//
//	avg_over_time(
//		kube_node_status_capacity_cpu_cores{
//			<some_custom_filter>
//		}[1h]
//	)
//
// ) by (cluster_id, node)
func NewNodeCPUCoresCapacityMetricCollector() *MetricCollector {
	return NewMetricCollector(
		NodeCPUCoresCapacityID,
		KubeNodeStatusCapacityCPUCores,
		[]string{"cluster", "node"},
		AverageOverTime,
	)
}

//	avg(
//		avg_over_time(
//			kube_node_status_allocatable_cpu_cores{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (cluster_id, node)

func NewNodeCPUCoresAllocatableMetricCollector() *MetricCollector {
	return NewMetricCollector(
		NodeCPUCoresAllocatableID,
		KubeNodeStatusAllocatableCPUCores,
		[]string{"cluster", "node"},
		AverageOverTime,
	)
}

//	avg(
//		avg_over_time(
//			kube_node_status_capacity_memory_bytes{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (cluster_id, node)

func NewNodeRAMBytesCapacityMetricCollector() *MetricCollector {
	return NewMetricCollector(
		NodeRAMBytesCapacityID,
		KubeNodeStatusCapacityMemoryBytes,
		[]string{"cluster", "node"},
		AverageOverTime,
	)
}

//	avg(
//		avg_over_time(
//			kube_node_status_allocatable_memory_bytes{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (cluster_id, node)

func NewNodeRAMBytesAllocatableMetricCollector() *MetricCollector {
	return NewMetricCollector(
		NodeRAMBytesAllocatableID,
		KubeNodeStatusAllocatableMemoryBytes,
		[]string{"cluster", "node"},
		AverageOverTime,
	)
}

//	avg(
//		avg_over_time(
//			node_gpu_count{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (cluster_id, node, provider_id)

func NewNodeGPUCountMetricCollector() *MetricCollector {
	return NewMetricCollector(
		NodeGPUCountID,
		NodeGPUCount,
		[]string{"cluster", "node", "provider_id"},
		AverageOverTime,
	)
}

//	avg_over_time(
//		kube_node_labels{
//			<some_custom_filter>
//		}[1h]
//	)

func NewNodeLabelsMetricCollector() *MetricCollector {
	return NewMetricCollector(
		NodeLabelsID,
		KubeNodeLabels,
		[]string{},
		Info,
	)
}

//	avg(
//		node_total_hourly_cost{
//			<some_custom_filter>
//		}
//	) by (node, cluster_id, provider_id)[%s:%dm]

func NewNodeActiveMinutesMetricCollector() *MetricCollector {
	return NewMetricCollector(
		NodeActiveMinutesID,
		NodeTotalHourlyCost,
		[]string{"node", "cluster", "provider_id"},
		ActiveMinutes,
	)
}

//	sum(
//		rate(
//			node_cpu_seconds_total{
//				<some_custom_filter>
//			}[%s:%dm]
//		)
//	) by (kubernetes_node, cluster_id, mode)

func NewNodeCPUModeTotalMetricCollector() *MetricCollector {
	return NewMetricCollector(
		NodeCPUModeTotalID,
		NodeCPUSecondsTotal,
		[]string{"kubernetes_node", "cluster", "mode"},
		Increase,
	)
}

//	avg(
//		avg_over_time(
//			container_memory_working_set_bytes{
//				container_name!="POD",
//				container_name!="",
//				namespace="kube-system",
//				<some_custom_filter>
//			}[%s:%dm]
//		)
//	) by (instance, cluster_id)

func NewNodeRAMSystemUsageAverageMetricCollector() *MetricCollector {
	return NewMetricCollector(
		NodeRAMSystemUsageAverageID,
		ContainerMemoryWorkingSetBytes,
		[]string{"instance", "cluster"},
		AverageOverTime,
	)
}

//	avg(
//		avg_over_time(
//			container_memory_working_set_bytes{
//				container_name!="POD",
//				container_name!="",
//				namespace!="kube-system",
//				<some_custom_filter>
//			}[%s:%dm]
//		)
//	) by (instance, cluster_id)

func NewNodeRAMUserUsageAverageMetricCollector() *MetricCollector {
	return NewMetricCollector(
		NodeRAMUserUsageAverageID,
		ContainerMemoryWorkingSetBytes,
		[]string{"instance", "cluster"},
		AverageOverTime,
	)
}

//	avg(
//		avg_over_time(
//			kubecost_load_balancer_cost{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (namespace, service_name, ingress_ip, cluster_id)

func NewLBPricePerHourMetricCollector() *MetricCollector {
	return NewMetricCollector(
		LBPricePerHourID,
		KubecostLoadBalancerCost,
		[]string{"namespace", "service_name", "ingress_ip", "cluster"},
		AverageOverTime,
	)
}

//	avg(
//		kubecost_load_balancer_cost{
//			<some_custom_filter>
//		}
//	) by (namespace, service_name, cluster_id, ingress_ip)[%s:%dm]

func NewLBActiveMinutesMetricCollector() *MetricCollector {
	return NewMetricCollector(
		LBActiveMinutesID,
		KubecostLoadBalancerCost,
		[]string{"namespace", "service_name", "cluster", "ingress_ip"},
		ActiveMinutes,
	)
}

//	avg(
//		kubecost_cluster_management_cost{
//			<some_custom_filter>
//		}
//	) by (cluster_id, provisioner_name)[%s:%dm]

func NewClusterManagementDurationMetricCollector() *MetricCollector {
	return NewMetricCollector(
		ClusterManagementDurationID,
		KubecostClusterManagementCost,
		[]string{"cluster", "provisioner_name"},
		ActiveMinutes,
	)
}

//	avg(
//		avg_over_time(
//			kubecost_cluster_management_cost{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (cluster_id, provisioner_name)

func NewClusterManagementPricePerHourMetricCollector() *MetricCollector {
	return NewMetricCollector(
		ClusterManagementPricePerHourID,
		KubecostClusterManagementCost,
		[]string{"cluster", "provisioner_name"},
		AverageOverTime,
	)
}

//	avg(
//		kube_pod_container_status_running{
//			<some_custom_filter>
//		} != 0
//	) by (pod, namespace, uid, cluster_id)[%s:%s]

func NewPodActiveMinutesMetricCollector() *MetricCollector {
	return NewMetricCollector(
		PodActiveMinutesID,
		KubePodContainerStatusRunning,
		[]string{"pod", "namespace", "uid", "cluster"},
		ActiveMinutes,
	)
}

//	avg(
//		avg_over_time(
//			container_memory_allocation_bytes{
//				container!="",
//				container!="POD",
//				node!="",
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (container, pod, namespace, node, cluster_id, provider_id)

func NewRAMBytesAllocatedMetricCollector() *MetricCollector {
	return NewMetricCollector(
		RAMBytesAllocatedID,
		ContainerMemoryAllocationBytes,
		[]string{"container", "pod", "uid", "namespace", "node", "cluster", "provider_id"},
		AverageOverTime,
	)
}

// avg(
//	avg_over_time(
//		kube_pod_container_resource_requests{
//			resource="memory",
//			unit="byte",
//			container!="",
//			container!="POD",
//			node!="",
//			<some_custom_filter>
//		}[1h]
//	)
//) by (container, pod, namespace, node, %s)

func NewRAMRequestsMetricCollector() *MetricCollector {
	return NewMetricCollector(
		RAMRequestsID,
		KubePodContainerResourceRequests,
		[]string{"container", "pod", "uid", "namespace", "node", "cluster"},
		AverageOverTime,
	)
}

// avg(
// 		avg_over_time(
// 			container_memory_working_set_bytes{
// 				container!="",
// 				container!="POD",
// 				<some_custom_filter>
// 			}[1h]
// 		)
// ) by (container, pod, namespace, instance, cluster_id)

func NewRAMUsageAverageMetricCollector() *MetricCollector {
	return NewMetricCollector(
		RAMUsageAverageID,
		ContainerMemoryWorkingSetBytes,
		[]string{"container", "uid", "pod", "namespace", "instance", "node", "cluster"},
		AverageOverTime,
	)
}

//	max(
//		max_over_time(
//			container_memory_working_set_bytes{
//				container!="",
//				container_name!="POD",
//				container!="POD",
//				<some_custom_filter>
//			}[%s]
//		)
//	) by (container_name, container, pod_name, pod, namespace, node, instance, %s)

func NewRAMUsageMaxMetricCollector() *MetricCollector {
	return NewMetricCollector(
		RAMUsageMaxID,
		ContainerMemoryWorkingSetBytes,
		[]string{"container", "uid", "pod", "namespace", "instance", "node", "cluster"},
		MaxOverTime,
	)
}

//	avg(
//		avg_over_time(
//			container_cpu_allocation{
//				container!="",
//				container!="POD",
//				node!="",
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (container, pod, namespace, node, cluster_id)

func NewCPUCoresAllocatedMetricCollector() *MetricCollector {
	return NewMetricCollector(
		CPUCoresAllocatedID,
		ContainerCPUAllocation,
		[]string{"container", "uid", "pod", "namespace", "node", "cluster"},
		AverageOverTime,
	)
}

//	avg(
//		avg_over_time(
//			kube_pod_container_resource_requests{
//				resource="cpu",
//				unit="core",
//				container!="",
//				container!="POD",
//				node!="",
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (container, pod, namespace, node, cluster_id)

func NewCPURequestsMetricCollector() *MetricCollector {
	return NewMetricCollector(
		CPURequestsID,
		KubePodContainerResourceRequests,
		[]string{"container", "uid", "pod", "namespace", "node", "cluster"},
		AverageOverTime,
	)
}

//	avg(
//		rate(
//			container_cpu_usage_seconds_total{
//				container!="",
//				container_name!="POD",
//				container!="POD",
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (container_name, container, pod_name, pod, namespace, node, instance, cluster_id)

func NewCPUUsageAverageMetricCollector() *MetricCollector {
	return NewMetricCollector(
		CPUUsageAverageID,
		ContainerCPUUsageSecondsTotal,
		[]string{"container", "uid", "pod", "namespace", "node", "instance", "cluster"},
		Increase,
	)
}

// TODO this is a special case
func NewCPUUsageMaxMetricCollector() *MetricCollector {
	return NewMetricCollector(
		CPUUsageMaxID,
		ContainerCPUUsageSecondsTotal,
		[]string{"container", "uid", "pod", "namespace", "node", "instance", "cluster"},
		MaxOverTime,
	)
}

//	avg(
//		avg_over_time(
//			kube_pod_container_resource_requests{
//				resource="nvidia_com_gpu",
//				container!="",
//				container!="POD",
//				node!="",
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (container, pod, namespace, node, cluster_id)

func NewGPUsRequestedMetricCollector() *MetricCollector {
	return NewMetricCollector(
		GPUsRequestedID,
		KubePodContainerResourceRequests,
		[]string{"container", "uid", "pod", "namespace", "node", "cluster"},
		AverageOverTime,
	)
}

//	avg(
//		avg_over_time(
//			DCGM_FI_PROF_GR_ENGINE_ACTIVE{
//				container!=""
//			}[1h]
//		)
//	) by (container, pod, namespace, cluster_id)

func NewGPUsUsageAverageMetricCollector() *MetricCollector {
	return NewMetricCollector(
		GPUsUsageAverageID,
		DCGMFIPROFGRENGINEACTIVE,
		[]string{"container", "uid", "pod", "namespace", "cluster"},
		AverageOverTime,
	)
}

//	max(
//		max_over_time(
//			DCGM_FI_PROF_GR_ENGINE_ACTIVE{
//				container!=""
//			}[1h]
//		)
//	) by (container, pod, namespace, cluster_id)

func NewGPUsUsageMaxMetricCollector() *MetricCollector {
	return NewMetricCollector(
		GPUsUsageMaxID,
		DCGMFIPROFGRENGINEACTIVE,
		[]string{"container", "uid", "pod", "namespace", "cluster"},
		MaxOverTime,
	)
}

//	avg(
//		avg_over_time(
//			container_gpu_allocation{
//				container!="",
//				container!="POD",
//				node!="",
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (container, pod, namespace, node, cluster_id)

func NewGPUsAllocatedMetricCollector() *MetricCollector {
	return NewMetricCollector(
		GPUsAllocatedID,
		ContainerGPUAllocation,
		[]string{"container", "uid", "pod", "namespace", "node", "cluster"},
		AverageOverTime,
	)
}

//	avg(
//		avg_over_time(
//			kube_pod_container_resource_requests{
//				container!="",
//				node != "",
//				pod != "",
//				container!= "",
//				unit = "integer",
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (container, pod, namespace, node, resource) // TODO is this missing cluster

func NewIsGPUSharedMetricCollector() *MetricCollector {
	return NewMetricCollector(
		IsGPUSharedID,
		KubePodContainerResourceRequests,
		[]string{"container", "uid", "pod", "namespace", "node", "cluster"},
		AverageOverTime,
	)
}

//	avg(
//		avg_over_time(
//			DCGM_FI_DEV_DEC_UTIL{
//				container!="",
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (container, pod, namespace, device, modelName, UUID) // TODO is this missing cluster

func NewGPUInfoMetricCollector() *MetricCollector {
	return NewMetricCollector(
		GPUInfoID,
		DCGMFIDEVDECUTIL,
		[]string{"container", "uid", "pod", "namespace", "device", "modelName", "uuid", "cluster"},
		AverageOverTime,
	)
}

//	avg(
//		avg_over_time(
//			node_cpu_hourly_cost{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (node, cluster_id, instance_type, provider_id)

func NewNodeCPUPricePerHourMetricCollector() *MetricCollector {
	return NewMetricCollector(
		NodeCPUPricePerHourID,
		NodeCPUHourlyCost,
		[]string{"node", "cluster", "instance_type", "provider_id"},
		AverageOverTime,
	)
}

//	avg(
//		avg_over_time(
//			node_ram_hourly_cost{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (node, cluster_id, instance_type, provider_id)

func NewNodeRAMPricePerGiBHourMetricCollector() *MetricCollector {
	return NewMetricCollector(
		NodeRAMPricePerGiBHourID,
		NodeRAMHourlyCost,
		[]string{"node", "cluster", "instance_type", "provider_id"},
		AverageOverTime,
	)
}

//	avg(
//		avg_over_time(
//			node_gpu_hourly_cost{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (node, cluster_id, instance_type, provider_id)

func NewNodeGPUPricePerHourMetricCollector() *MetricCollector {
	return NewMetricCollector(
		NodeGPUPricePerHourID,
		NodeGPUHourlyCost,
		[]string{"node", "cluster", "instance_type", "provider_id"},
		AverageOverTime,
	)
}

//	avg_over_time(
//		kubecost_node_is_spot{
//			<some_custom_filter>
//		}[1h]
//	)

func NewNodeIsSpotMetricCollector() *MetricCollector {
	return NewMetricCollector(
		NodeIsSpotID,
		KubecostNodeIsSpot,
		[]string{"node", "cluster"}, // Todo are these the correct labels
		AverageOverTime,
	)
}

//	avg(
//		avg_over_time(
//			pod_pvc_allocation{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (persistentvolume, persistentvolumeclaim, pod, namespace, cluster_id)

func NewPodPVCAllocationMetricCollector() *MetricCollector {
	return NewMetricCollector(
		PodPVCAllocationID,
		PodPVCAllocation,
		[]string{"persistentvolume", "persistentvolumeclaim", "pod", "namespace", "cluster"},
		AverageOverTime,
	)
}

//	avg(
//		avg_over_time(
//			kube_persistentvolumeclaim_resource_requests_storage_bytes{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (persistentvolumeclaim, namespace, cluster_id)

func NewPVCBytesRequestedMetricCollector() *MetricCollector {
	return NewMetricCollector(
		PVCBytesRequestedID,
		KubePersistentVolumeClaimResourceRequestsStorageBytes,
		[]string{"persistentvolumeclaim", "namespace", "cluster"},
		AverageOverTime,
	)
}

//	avg(
//		avg_over_time(
//			kube_persistentvolume_capacity_bytes{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (persistentvolume, cluster_id)

func NewPVBytesMetricCollector() *MetricCollector {
	return NewMetricCollector(
		PVBytesID,
		KubePersistentVolumeCapacityBytes,
		[]string{"persistentvolume", "cluster"},
		AverageOverTime,
	)
}

//	avg(
//		avg_over_time(
//			pv_hourly_cost{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (volumename, cluster_id)

func NewPVCostPerGiBHourMetricCollector() *MetricCollector {
	return NewMetricCollector(
		PVCostPerGiBHourID,
		PVHourlyCost,
		[]string{"volumename", "cluster"},
		AverageOverTime,
	)
}

//	avg(
//		avg_over_time(
//			kubecost_pv_info{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (cluster_id, storageclass, persistentvolume, provider_id)

func NewPVInfoMetricCollector() *MetricCollector {
	return NewMetricCollector(
		PVInfoID,
		KubecostPVInfo,
		[]string{"cluster", "storageclass", "persistentvolume", "provider_id"},
		AverageOverTime,
	)
}

//	sum(
//		increase(
//			kubecost_pod_network_egress_bytes_total{
//				internet="false",
//				same_zone="false",
//				same_region="true",
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (pod_name, namespace, cluster_id) / 1024 / 1024 / 1024

func NewNetZoneGiBMetricCollector() *MetricCollector {
	return NewMetricCollector(
		NetZoneGiBID,
		KubecostPodNetworkEgressBytesTotal,
		[]string{"pod", "namespace", "cluster"},
		Increase,
	)
}

//	avg(
//		avg_over_time(
//			kubecost_network_zone_egress_cost{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (cluster_id)

func NewNetZonePricePerGiBMetricCollector() *MetricCollector {
	return NewMetricCollector(
		NetZonePricePerGiBID,
		KubecostNetworkZoneEgressCost,
		[]string{"cluster"},
		AverageOverTime,
	)
}

//	sum(
//		increase(
//			kubecost_pod_network_egress_bytes_total{
//				internet="false",
//				same_zone="false",
//				same_region="false",
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (pod_name, namespace, cluster_id) / 1024 / 1024 / 1024

func NewNetRegionGiBMetricCollector() *MetricCollector {
	return NewMetricCollector(
		NetRegionGiBID,
		KubecostPodNetworkEgressBytesTotal,
		[]string{"pod", "namespace", "cluster"},
		Increase,
	)
}

//	avg(
//		avg_over_time(
//			kubecost_network_region_egress_cost{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (cluster_id)

func NewNetRegionPricePerGiBMetricCollector() *MetricCollector {
	return NewMetricCollector(
		NetRegionPricePerGiBID,
		KubecostNetworkRegionEgressCost,
		[]string{"cluster"},
		AverageOverTime,
	)
}

//	sum(
//		increase(
//			kubecost_pod_network_egress_bytes_total{
//				internet="true",
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (pod_name, namespace, cluster_id) / 1024 / 1024 / 1024

func NewNetInternetGiBMetricCollector() *MetricCollector {
	return NewMetricCollector(
		NetInternetGiBID,
		KubecostPodNetworkEgressBytesTotal,
		[]string{"pod", "namespace", "cluster"},
		Increase,
	)
}

//	avg(
//		avg_over_time(
//			kubecost_network_internet_egress_cost{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (cluster_id)

func NewNetInternetPricePerGiBMetricCollector() *MetricCollector {
	return NewMetricCollector(
		NetInternetPricePerGiBID,
		KubecostNetworkInternetEgressCost,
		[]string{"cluster"},
		AverageOverTime,
	)
}

//	sum(
//		increase(
//			container_network_receive_bytes_total{
//				pod!="",
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (pod_name, pod, namespace, cluster_id)

func NewNetReceiveBytesMetricCollector() *MetricCollector {
	return NewMetricCollector(
		NetReceiveBytesID,
		ContainerNetworkReceiveBytesTotal,
		[]string{"pod", "namespace", "cluster"},
		Increase,
	)
}

//	sum(
//		increase(
//			container_network_transmit_bytes_total{
//				pod!="",
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (pod_name, pod, namespace, cluster_id)

func NewNetTransferBytesMetricCollector() *MetricCollector {
	return NewMetricCollector(
		NetTransferBytesID,
		ContainerNetworkTransmitBytesTotal,
		[]string{"pod", "namespace", "cluster"},
		Increase,
	)
}

//	avg_over_time(
//		kube_namespace_labels{
//			<some_custom_filter>
//		}[1h]
//	)

func NewNamespaceLabelsMetricCollector() *MetricCollector {
	return NewMetricCollector(
		NamespaceLabelsID,
		KubeNamespaceLabels,
		[]string{},
		Info,
	)
}

//	avg_over_time(
//		kube_namespace_annotations{
//			<some_custom_filter>
//		}[1h]
//	)

func NewNamespaceAnnotationsMetricCollector() *MetricCollector {
	return NewMetricCollector(
		NamespaceAnnotationsID,
		KubeNamespaceAnnotations,
		[]string{},
		Info,
	)
}

//	avg_over_time(
//		kube_pod_labels{
//			<some_custom_filter>
//		}[1h]
//	)

func NewPodLabelsMetricCollector() *MetricCollector {
	return NewMetricCollector(
		PodLabelsID,
		KubePodLabels,
		[]string{},
		Info,
	)
}

//	avg_over_time(
//		kube_pod_annotations{
//			<some_custom_filter>
//		}[1h]
//	)

func NewPodAnnotationsMetricCollector() *MetricCollector {
	return NewMetricCollector(
		PodAnnotationsID,
		KubePodAnnotations,
		[]string{},
		Info,
	)
}

//	avg_over_time(
//		service_selector_labels{
//			<some_custom_filter>
//		}[1h]
//	)

func NewServiceLabelsMetricCollector() *MetricCollector {
	return NewMetricCollector(
		ServiceLabelsID,
		ServiceSelectorLabels,
		[]string{},
		Info,
	)
}

//	avg_over_time(
//		deployment_match_labels{
//			<some_custom_filter>
//		}[1h]
//	)

func NewDeploymentLabelsMetricCollector() *MetricCollector {
	return NewMetricCollector(
		DeploymentLabelsID,
		DeploymentMatchLabels,
		[]string{},
		Info,
	)
}

//	avg_over_time(
//		statefulSet_match_labels{
//			<some_custom_filter>
//		}[1h]
//	)

func NewStatefulSetLabelsMetricCollector() *MetricCollector {
	return NewMetricCollector(
		StatefulSetLabelsID,
		StatefulSetMatchLabels,
		[]string{},
		Info,
	)
}

//	sum(
//		avg_over_time(
//			kube_pod_owner{
//				owner_kind="DaemonSet",
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (pod, owner_name, namespace, cluster_id)

func NewDaemonSetLabelsMetricCollector() *MetricCollector {
	return NewMetricCollector(
		DaemonSetLabelsID,
		KubePodOwner,
		[]string{"pod", "owner_name", "namespace", "cluster_id"},
		Info,
	)
}

//	sum(
//		avg_over_time(
//			kube_pod_owner{
//				owner_kind="Job",
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (pod, owner_name, namespace, cluster_id)

func NewJobLabelsMetricCollector() *MetricCollector {
	return NewMetricCollector(
		JobLabelsID,
		KubePodOwner,
		[]string{"pod", "owner_name", "namespace", "cluster_id"},
		Info,
	)
}

//	sum(
//		avg_over_time(
//			kube_pod_owner{
//				owner_kind="ReplicaSet",
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (pod, owner_name, namespace, cluster_id)

func NewPodsWithReplicaSetOwnerMetricCollector() *MetricCollector {
	return NewMetricCollector(
		PodsWithReplicaSetOwnerID,
		KubePodOwner,
		[]string{"pod", "owner_name", "namespace", "cluster_id"},
		Info,
	)
}

//	sum(
//		avg_over_time(
//			kube_replicaset_owner{
//				owner_kind="<none>",
//				owner_name="<none>",
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (replicaset, namespace, cluster_id)

func NewReplicaSetsWithoutOwnersMetricCollector() *MetricCollector {
	return NewMetricCollector(
		ReplicaSetsWithoutOwnersID,
		KubeReplicasetOwner,
		[]string{"replicaset", "namespace", "cluster_id"},
		Info,
	)
}

//	sum(
//		avg_over_time(
//			kube_replicaset_owner{
//				owner_kind="Rollout",
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (replicaset, namespace, owner_kind, owner_name, cluster_id)

func NewReplicaSetsWithRolloutMetricCollector() *MetricCollector {
	return NewMetricCollector(
		ReplicaSetsWithRolloutID,
		KubeReplicasetOwner,
		[]string{"replicaset", "namespace", "owner_kind", "owner_name", "cluster_id"},
		Info,
	)
}
