package collector

import (
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric/aggregator"
)

// NewOpenCostMetricStore creates a new MetricStore which has registered all MetricCollector instances required
// for OpenCost
func NewOpenCostMetricStore() metric.MetricStore {
	memStore := metric.NewInMemoryMetricStore()

	// Register all the metrics
	memStore.Register(NewPVPricePerGiBHourMetricCollector())
	memStore.Register(NewPVUsedAverageMetricCollector())
	memStore.Register(NewPVUsedMaxMetricCollector())
	memStore.Register(NewPVCInfoMetricCollector())
	memStore.Register(NewPVActiveMinutesMetricCollector())
	memStore.Register(NewLocalStorageUsedActiveMinutesMetricCollector())
	memStore.Register(NewLocalStorageUsedAverageMetricCollector())
	memStore.Register(NewLocalStorageUsedMaxMetricCollector())
	memStore.Register(NewLocalStorageBytesMetricCollector())
	memStore.Register(NewLocalStorageActiveMinutesMetricCollector())
	memStore.Register(NewNodeCPUCoresCapacityMetricCollector())
	memStore.Register(NewNodeCPUCoresAllocatableMetricCollector())
	memStore.Register(NewNodeRAMBytesCapacityMetricCollector())
	memStore.Register(NewNodeRAMBytesAllocatableMetricCollector())
	memStore.Register(NewNodeGPUCountMetricCollector())
	memStore.Register(NewNodeLabelsMetricCollector())
	memStore.Register(NewNodeActiveMinutesMetricCollector())
	memStore.Register(NewNodeCPUModeTotalMetricCollector())
	memStore.Register(NewNodeRAMSystemUsageAverageMetricCollector())
	memStore.Register(NewNodeRAMUserUsageAverageMetricCollector())
	memStore.Register(NewLBPricePerHourMetricCollector())
	memStore.Register(NewLBActiveMinutesMetricCollector())
	memStore.Register(NewClusterManagementDurationMetricCollector())
	memStore.Register(NewClusterManagementPricePerHourMetricCollector())
	memStore.Register(NewPodActiveMinutesMetricCollector())
	memStore.Register(NewRAMBytesAllocatedMetricCollector())
	memStore.Register(NewRAMRequestsMetricCollector())
	memStore.Register(NewRAMLimitsMetricCollector())
	memStore.Register(NewRAMUsageAverageMetricCollector())
	memStore.Register(NewRAMUsageMaxMetricCollector())
	memStore.Register(NewCPUCoresAllocatedMetricCollector())
	memStore.Register(NewCPURequestsMetricCollector())
	memStore.Register(NewCPULimitsMetricCollector())
	memStore.Register(NewCPUUsageAverageMetricCollector())
	memStore.Register(NewCPUUsageMaxMetricCollector())
	memStore.Register(NewGPUsRequestedMetricCollector())
	memStore.Register(NewGPUsUsageAverageMetricCollector())
	memStore.Register(NewGPUsUsageMaxMetricCollector())
	memStore.Register(NewGPUsAllocatedMetricCollector())
	memStore.Register(NewIsGPUSharedMetricCollector())
	memStore.Register(NewGPUInfoMetricCollector())
	memStore.Register(NewNodeCPUPricePerHourMetricCollector())
	memStore.Register(NewNodeRAMPricePerGiBHourMetricCollector())
	memStore.Register(NewNodeGPUPricePerHourMetricCollector())
	memStore.Register(NewNodeIsSpotMetricCollector())
	memStore.Register(NewPodPVCAllocationMetricCollector())
	memStore.Register(NewPVCBytesRequestedMetricCollector())
	memStore.Register(NewPVBytesMetricCollector())
	memStore.Register(NewPVInfoMetricCollector())
	memStore.Register(NewNetZoneGiBMetricCollector())
	memStore.Register(NewNetZonePricePerGiBMetricCollector())
	memStore.Register(NewNetRegionGiBMetricCollector())
	memStore.Register(NewNetRegionPricePerGiBMetricCollector())
	memStore.Register(NewNetInternetGiBMetricCollector())
	memStore.Register(NewNetInternetPricePerGiBMetricCollector())
	memStore.Register(NewNetInternetServiceGiBMetricCollector())
	memStore.Register(NewNetReceiveBytesMetricCollector())
	memStore.Register(NewNetZoneIngressGiBMetricCollector())
	memStore.Register(NewNetRegionIngressGiBMetricCollector())
	memStore.Register(NewNetInternetIngressGiBMetricCollector())
	memStore.Register(NewNetInternetServiceIngressGiBMetricCollector())
	memStore.Register(NewNetTransferBytesMetricCollector())
	memStore.Register(NewNamespaceLabelsMetricCollector())
	memStore.Register(NewNamespaceAnnotationsMetricCollector())
	memStore.Register(NewPodLabelsMetricCollector())
	memStore.Register(NewPodAnnotationsMetricCollector())
	memStore.Register(NewServiceLabelsMetricCollector())
	memStore.Register(NewDeploymentLabelsMetricCollector())
	memStore.Register(NewStatefulSetLabelsMetricCollector())
	memStore.Register(NewDaemonSetLabelsMetricCollector())
	memStore.Register(NewJobLabelsMetricCollector())
	memStore.Register(NewPodsWithReplicaSetOwnerMetricCollector())
	memStore.Register(NewReplicaSetsWithoutOwnersMetricCollector())
	memStore.Register(NewReplicaSetsWithRolloutMetricCollector())
	memStore.Register(NewResourceQuotaSpecCPURequestAverageMetricCollector())
	memStore.Register(NewResourceQuotaSpecCPURequestMaxMetricCollector())
	memStore.Register(NewResourceQuotaSpecRAMRequestAverageMetricCollector())
	memStore.Register(NewResourceQuotaSpecRAMRequestMaxMetricCollector())
	memStore.Register(NewResourceQuotaSpecCPULimitAverageMetricCollector())
	memStore.Register(NewResourceQuotaSpecCPULimitMaxMetricCollector())
	memStore.Register(NewResourceQuotaSpecRAMLimitAverageMetricCollector())
	memStore.Register(NewResourceQuotaSpecRAMLimitMaxMetricCollector())
	memStore.Register(NewResourceQuotaStatusUsedCPURequestAverageMetricCollector())
	memStore.Register(NewResourceQuotaStatusUsedCPURequestMaxMetricCollector())
	memStore.Register(NewResourceQuotaStatusUsedRAMRequestAverageMetricCollector())
	memStore.Register(NewResourceQuotaStatusUsedRAMRequestMaxMetricCollector())
	memStore.Register(NewResourceQuotaStatusUsedCPULimitAverageMetricCollector())
	memStore.Register(NewResourceQuotaStatusUsedCPULimitMaxMetricCollector())
	memStore.Register(NewResourceQuotaStatusUsedRAMLimitAverageMetricCollector())
	memStore.Register(NewResourceQuotaStatusUsedRAMLimitMaxMetricCollector())

	return memStore
}

//	avg(
//		avg_over_time(
//			pv_hourly_cost{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (cluster_id, persistentvolume, volumename, provider_id)

func NewPVPricePerGiBHourMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.PVPricePerGiBHourID,
		metric.PVHourlyCost,
		[]string{
			source.VolumeNameLabel,
			source.PVLabel,
			source.ProviderIDLabel,
			source.UIDLabel,
		},
		aggregator.AverageOverTime,
		nil,
	)
}

//	avg(
//		avg_over_time(
//			kubelet_volume_stats_used_bytes{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (cluster_id, persistentvolumeclaim, namespace)

func NewPVUsedAverageMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.PVUsedAverageID,
		metric.KubeletVolumeStatsUsedBytes,
		[]string{
			source.NamespaceLabel,
			source.PVCLabel,
			source.UIDLabel,
		},
		aggregator.AverageOverTime,
		nil,
	)
}

//	max(
//		max_over_time(
//			kubelet_volume_stats_used_bytes{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (cluster_id, persistentvolumeclaim, namespace)

func NewPVUsedMaxMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.PVUsedMaxID,
		metric.KubeletVolumeStatsUsedBytes,
		[]string{
			source.NamespaceLabel,
			source.PVCLabel,
			source.UIDLabel,
		},
		aggregator.MaxOverTime,
		nil,
	)
}

//	avg(
//		kube_persistentvolumeclaim_info{
//			volumename != "",
//			<some_custom_filter>
//		}
//	) by (persistentvolumeclaim, storageclass, volumename, namespace, cluster_id)[0:10m]

func NewPVCInfoMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.PVCInfoID,
		metric.KubePersistentVolumeClaimInfo,
		[]string{
			source.NamespaceLabel,
			source.VolumeNameLabel,
			source.PVCLabel,
			source.StorageClassLabel,
			source.UIDLabel,
		},
		aggregator.ActiveMinutes,
		func(labels map[string]string) bool {
			return labels[source.VolumeNameLabel] != ""
		},
	)
}

//	avg(
//		kube_persistentvolume_capacity_bytes{
//			<some_custom_filter>
//		}
//	) by (cluster_id, persistentvolume)[0:10m]

func NewPVActiveMinutesMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.PVActiveMinutesID,
		metric.KubePersistentVolumeCapacityBytes,
		[]string{
			source.PVLabel,
			source.UIDLabel,
		},
		aggregator.ActiveMinutes,
		nil,
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
// NewLocalStorageUsedActiveMinutesMetricCollector does not have an associated query end point but is used in the results
// of QueryLocalStorageUsedCost
func NewLocalStorageUsedActiveMinutesMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.LocalStorageUsedActiveMinutesID,
		metric.ContainerFSUsageBytes,
		[]string{
			source.InstanceLabel,
			source.DeviceLabel,
			source.UIDLabel,
		},
		aggregator.ActiveMinutes,
		nil, // filter not required here because only container root file system is being scraped
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

func NewLocalStorageUsedAverageMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.LocalStorageUsedAverageID,
		metric.ContainerFSUsageBytes,
		[]string{
			source.InstanceLabel,
			source.DeviceLabel,
			source.UIDLabel,
		},
		aggregator.AverageOverTime,
		nil, // filter not required here because only container root file system is being scraped
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
func NewLocalStorageUsedMaxMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.LocalStorageUsedMaxID,
		metric.ContainerFSUsageBytes,
		[]string{
			source.InstanceLabel,
			source.DeviceLabel,
			source.UIDLabel,
		},
		aggregator.MaxOverTime,
		nil, // filter not required here because only container root file system is being scraped
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
func NewLocalStorageBytesMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.LocalStorageBytesID,
		metric.NodeFSCapacityBytes,
		[]string{
			source.InstanceLabel,
			source.DeviceLabel,
			source.UIDLabel,
		},
		aggregator.AverageOverTime,
		nil, // filter not required here because only node root file system is being scraped
	)
}

// count(
//
//	node_total_hourly_cost{
//		<some_custom_filter>
//	}
//
// ) by (cluster_id, node, instance, provider_id)[%s:%dm]
func NewLocalStorageActiveMinutesMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.LocalStorageActiveMinutesID,
		metric.NodeTotalHourlyCost,
		[]string{
			source.NodeLabel,
			source.ProviderIDLabel,
			source.UIDLabel,
		},
		aggregator.ActiveMinutes,
		nil,
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
func NewNodeCPUCoresCapacityMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NodeCPUCoresCapacityID,
		metric.KubeNodeStatusCapacityCPUCores,
		[]string{
			source.NodeLabel,
			source.UIDLabel,
		},
		aggregator.AverageOverTime,
		nil,
	)
}

//	avg(
//		avg_over_time(
//			kube_node_status_allocatable_cpu_cores{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (cluster_id, node)

func NewNodeCPUCoresAllocatableMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NodeCPUCoresAllocatableID,
		metric.KubeNodeStatusAllocatableCPUCores,
		[]string{
			source.NodeLabel,
			source.UIDLabel,
		},
		aggregator.AverageOverTime,
		nil,
	)
}

//	avg(
//		avg_over_time(
//			kube_node_status_capacity_memory_bytes{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (cluster_id, node)

func NewNodeRAMBytesCapacityMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NodeRAMBytesCapacityID,
		metric.KubeNodeStatusCapacityMemoryBytes,
		[]string{
			source.NodeLabel,
			source.UIDLabel,
		},
		aggregator.AverageOverTime,
		nil,
	)
}

//	avg(
//		avg_over_time(
//			kube_node_status_allocatable_memory_bytes{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (cluster_id, node)

func NewNodeRAMBytesAllocatableMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NodeRAMBytesAllocatableID,
		metric.KubeNodeStatusAllocatableMemoryBytes,
		[]string{
			source.NodeLabel,
			source.UIDLabel,
		},
		aggregator.AverageOverTime,
		nil,
	)
}

//	avg(
//		avg_over_time(
//			node_gpu_count{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (cluster_id, node, provider_id)

func NewNodeGPUCountMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NodeGPUCountID,
		metric.NodeGPUCount,
		[]string{
			source.NodeLabel,
			source.ProviderIDLabel,
			source.UIDLabel,
		},
		aggregator.AverageOverTime,
		nil,
	)
}

//	avg_over_time(
//		kube_node_labels{
//			<some_custom_filter>
//		}[1h]
//	)

func NewNodeLabelsMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NodeLabelsID,
		metric.KubeNodeLabels,
		[]string{
			source.NodeLabel,
			source.UIDLabel,
		},
		aggregator.Info,
		nil,
	)
}

//	avg(
//		node_total_hourly_cost{
//			<some_custom_filter>
//		}
//	) by (node, cluster_id, provider_id)[%s:%dm]

func NewNodeActiveMinutesMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NodeActiveMinutesID,
		metric.NodeTotalHourlyCost,
		[]string{
			source.NodeLabel,
			source.ProviderIDLabel,
			source.UIDLabel,
		},
		aggregator.ActiveMinutes,
		nil,
	)
}

//	sum(
//		rate(
//			node_cpu_seconds_total{
//				<some_custom_filter>
//			}[%s:%dm]
//		)
//	) by (kubernetes_node, cluster_id, mode)

func NewNodeCPUModeTotalMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NodeCPUModeTotalID,
		metric.NodeCPUSecondsTotal,
		[]string{
			source.KubernetesNodeLabel,
			source.ModeLabel,
			source.UIDLabel,
		},
		aggregator.Rate,
		nil,
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

func NewNodeRAMSystemUsageAverageMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NodeRAMSystemUsageAverageID,
		metric.ContainerMemoryWorkingSetBytes,
		[]string{
			source.InstanceLabel,
			source.UIDLabel,
		},
		aggregator.AverageOverTime,
		func(labels map[string]string) bool {
			return labels[source.ContainerLabel] != "POD" && labels[source.ContainerLabel] != "" && labels[source.NamespaceLabel] == "kube-system"
		},
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

func NewNodeRAMUserUsageAverageMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NodeRAMUserUsageAverageID,
		metric.ContainerMemoryWorkingSetBytes,
		[]string{
			source.InstanceLabel,
			source.UIDLabel,
		},
		aggregator.AverageOverTime,
		func(labels map[string]string) bool {
			return labels[source.ContainerLabel] != "POD" && labels[source.ContainerLabel] != "" && labels[source.NamespaceLabel] != "kube-system"
		},
	)
}

//	avg(
//		avg_over_time(
//			kubecost_load_balancer_cost{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (namespace, service_name, ingress_ip, cluster_id)

func NewLBPricePerHourMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.LBPricePerHourID,
		metric.KubecostLoadBalancerCost,
		[]string{
			source.NamespaceLabel,
			source.ServiceNameLabel,
			source.IngressIPLabel,
			source.UIDLabel,
		},
		aggregator.AverageOverTime,
		nil,
	)
}

//	avg(
//		kubecost_load_balancer_cost{
//			<some_custom_filter>
//		}
//	) by (namespace, service_name, cluster_id, ingress_ip)[%s:%dm]

func NewLBActiveMinutesMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.LBActiveMinutesID,
		metric.KubecostLoadBalancerCost,
		[]string{
			source.NamespaceLabel,
			source.ServiceNameLabel,
			source.IngressIPLabel,
			source.UIDLabel,
		},
		aggregator.ActiveMinutes,
		nil,
	)
}

//	avg(
//		kubecost_cluster_management_cost{
//			<some_custom_filter>
//		}
//	) by (cluster_id, provisioner_name)[%s:%dm]

func NewClusterManagementDurationMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.ClusterManagementDurationID,
		metric.KubecostClusterManagementCost,
		[]string{
			source.ProvisionerNameLabel,
			source.UIDLabel,
		},
		aggregator.ActiveMinutes,
		nil,
	)
}

//	avg(
//		avg_over_time(
//			kubecost_cluster_management_cost{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (cluster_id, provisioner_name)

func NewClusterManagementPricePerHourMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.ClusterManagementPricePerHourID,
		metric.KubecostClusterManagementCost,
		[]string{
			source.ProvisionerNameLabel,
			source.UIDLabel,
		},
		aggregator.AverageOverTime,
		nil,
	)
}

//	avg(
//		kube_pod_container_status_running{
//			<some_custom_filter>
//		} != 0
//	) by (pod, namespace, uid, cluster_id)[%s:%s]

func NewPodActiveMinutesMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.PodActiveMinutesID,
		metric.KubePodContainerStatusRunning,
		[]string{
			source.UIDLabel,
			source.NamespaceLabel,
			source.PodLabel,
		},
		aggregator.ActiveMinutes,
		nil,
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

func NewRAMBytesAllocatedMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.RAMBytesAllocatedID,
		metric.ContainerMemoryAllocationBytes,
		[]string{
			source.NodeLabel,
			source.InstanceLabel,
			source.NamespaceLabel,
			source.PodLabel,
			source.UIDLabel,
			source.ContainerLabel,
		},
		aggregator.AverageOverTime,
		func(labels map[string]string) bool {
			return labels[source.ContainerLabel] != "POD" && labels[source.ContainerLabel] != "" && labels[source.NodeLabel] != ""
		},
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
//) by (container, pod, namespace, node, cluster_id)

func NewRAMRequestsMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.RAMRequestsID,
		metric.KubePodContainerResourceRequests,
		[]string{
			source.NodeLabel,
			source.InstanceLabel,
			source.NamespaceLabel,
			source.PodLabel,
			source.UIDLabel,
			source.ContainerLabel,
		},
		aggregator.AverageOverTime,
		func(labels map[string]string) bool {
			return labels[source.ResourceLabel] == "memory" && labels[source.UnitLabel] == "byte" && labels[source.ContainerLabel] != "POD" && labels[source.ContainerLabel] != "" && labels[source.NodeLabel] != ""
		},
	)
}

// avg(
//	avg_over_time(
//		kube_pod_container_resource_limits{
//			resource="memory",
//			unit="byte",
//			container!="",
//			container!="POD",
//			node!="",
//			<some_custom_filter>
//		}[1h]
//	)
//) by (container, pod, namespace, node, cluster_id)

func NewRAMLimitsMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.RAMLimitsID,
		metric.KubePodContainerResourceLimits,
		[]string{
			source.NodeLabel,
			source.InstanceLabel,
			source.NamespaceLabel,
			source.PodLabel,
			source.ContainerLabel,
		},
		aggregator.AverageOverTime,
		func(labels map[string]string) bool {
			return labels[source.ResourceLabel] == "memory" && labels[source.UnitLabel] == "byte" && labels[source.ContainerLabel] != "POD" && labels[source.ContainerLabel] != "" && labels[source.NodeLabel] != ""
		},
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

func NewRAMUsageAverageMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.RAMUsageAverageID,
		metric.ContainerMemoryWorkingSetBytes,
		[]string{
			source.NodeLabel,
			source.InstanceLabel,
			source.NamespaceLabel,
			source.PodLabel,
			source.UIDLabel,
			source.ContainerLabel,
		},
		aggregator.AverageOverTime,
		func(labels map[string]string) bool {
			return labels[source.ContainerLabel] != "POD" && labels[source.ContainerLabel] != ""
		},
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

func NewRAMUsageMaxMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.RAMUsageMaxID,
		metric.ContainerMemoryWorkingSetBytes,
		[]string{
			source.NodeLabel,
			source.InstanceLabel,
			source.NamespaceLabel,
			source.PodLabel,
			source.UIDLabel,
			source.ContainerLabel,
		},
		aggregator.MaxOverTime,
		func(labels map[string]string) bool {
			return labels[source.ContainerLabel] != "" && labels[source.ContainerLabel] != "POD" && labels[source.NodeLabel] != ""
		},
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

func NewCPUCoresAllocatedMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.CPUCoresAllocatedID,
		metric.ContainerCPUAllocation,
		[]string{
			source.NodeLabel,
			source.InstanceLabel,
			source.NamespaceLabel,
			source.PodLabel,
			source.UIDLabel,
			source.ContainerLabel,
		},
		aggregator.AverageOverTime,
		func(labels map[string]string) bool {
			return labels[source.ContainerLabel] != "POD" && labels[source.ContainerLabel] != "" && labels[source.NodeLabel] != ""
		},
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

func NewCPURequestsMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.CPURequestsID,
		metric.KubePodContainerResourceRequests,
		[]string{
			source.NodeLabel,
			source.InstanceLabel,
			source.NamespaceLabel,
			source.PodLabel,
			source.UIDLabel,
			source.ContainerLabel,
		},
		aggregator.AverageOverTime,
		func(labels map[string]string) bool {
			return labels[source.ResourceLabel] == "cpu" && labels[source.UnitLabel] == "core" && labels[source.ContainerLabel] != "POD" && labels[source.ContainerLabel] != "" && labels[source.NodeLabel] != ""
		},
	)
}

//	avg(
//		avg_over_time(
//			kube_pod_container_resource_limits{
//				resource="cpu",
//				unit="core",
//				container!="",
//				container!="POD",
//				node!="",
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (container, pod, namespace, node, cluster_id)

func NewCPULimitsMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.CPULimitsID,
		metric.KubePodContainerResourceLimits,
		[]string{
			source.NodeLabel,
			source.InstanceLabel,
			source.NamespaceLabel,
			source.PodLabel,
			source.ContainerLabel,
		},
		aggregator.AverageOverTime,
		func(labels map[string]string) bool {
			return labels[source.ResourceLabel] == "cpu" && labels[source.UnitLabel] == "core" && labels[source.ContainerLabel] != "POD" && labels[source.ContainerLabel] != "" && labels[source.NodeLabel] != ""
		},
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

func NewCPUUsageAverageMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.CPUUsageAverageID,
		metric.ContainerCPUUsageSecondsTotal,
		[]string{
			source.NodeLabel,
			source.InstanceLabel,
			source.NamespaceLabel,
			source.PodLabel,
			source.UIDLabel,
			source.ContainerLabel,
		},
		aggregator.Rate,
		func(labels map[string]string) bool {
			return labels[source.ContainerLabel] != "" && labels[source.ContainerLabel] != "POD"
		},
	)
}

// max(
//
//	max_over_time(
//		irate(
//			container_cpu_usage_seconds_total{
//				container!="POD",
//				container!="",
//				<some_custom_filter>
//			}[1h]
//		)[%s:%s]
//	)
//
// ) by (container, pod_name, pod, namespace, node, instance, cluster_id)
func NewCPUUsageMaxMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.CPUUsageMaxID,
		metric.ContainerCPUUsageSecondsTotal,
		[]string{
			source.NodeLabel,
			source.InstanceLabel,
			source.NamespaceLabel,
			source.PodLabel,
			source.UIDLabel,
			source.ContainerLabel,
		},
		aggregator.IRateMax,
		func(labels map[string]string) bool {
			return labels[source.ContainerLabel] != "" && labels[source.ContainerLabel] != "POD"
		},
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

func NewGPUsRequestedMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.GPUsRequestedID,
		metric.KubePodContainerResourceRequests,
		[]string{
			source.NamespaceLabel,
			source.PodLabel,
			source.UIDLabel,
			source.ContainerLabel,
		},
		aggregator.AverageOverTime,
		func(labels map[string]string) bool {
			return labels[source.ResourceLabel] == "nvidia_com_gpu" && labels[source.ContainerLabel] != "POD" && labels[source.ContainerLabel] != "" && labels[source.NodeLabel] != ""
		},
	)
}

//	avg(
//		avg_over_time(
//			DCGM_FI_PROF_GR_ENGINE_ACTIVE{
//				container!=""
//			}[1h]
//		)
//	) by (container, pod, namespace, cluster_id)

func NewGPUsUsageAverageMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.GPUsUsageAverageID,
		metric.DCGMFIPROFGRENGINEACTIVE,
		[]string{
			source.NamespaceLabel,
			source.PodLabel,
			source.UIDLabel,
			source.ContainerLabel,
		},
		aggregator.AverageOverTime,
		func(labels map[string]string) bool {
			return labels[source.ContainerLabel] != ""
		},
	)
}

//	max(
//		max_over_time(
//			DCGM_FI_PROF_GR_ENGINE_ACTIVE{
//				container!=""
//			}[1h]
//		)
//	) by (container, pod, namespace, cluster_id)

func NewGPUsUsageMaxMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.GPUsUsageMaxID,
		metric.DCGMFIPROFGRENGINEACTIVE,
		[]string{
			source.NamespaceLabel,
			source.PodLabel,
			source.UIDLabel,
			source.ContainerLabel,
		},
		aggregator.MaxOverTime,
		func(labels map[string]string) bool {
			return labels[source.ContainerLabel] != ""
		},
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

func NewGPUsAllocatedMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.GPUsAllocatedID,
		metric.ContainerGPUAllocation,
		[]string{
			source.NamespaceLabel,
			source.PodLabel,
			source.UIDLabel,
			source.ContainerLabel,
		},
		aggregator.AverageOverTime,
		func(labels map[string]string) bool {
			return labels[source.ContainerLabel] != "" && labels[source.ContainerLabel] != "POD" && labels[source.NodeLabel] != ""
		},
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
//	) by (container, pod, namespace, node, resource, cluster_id)

func NewIsGPUSharedMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.IsGPUSharedID,
		metric.KubePodContainerResourceRequests,
		[]string{
			source.NamespaceLabel,
			source.PodLabel,
			source.UIDLabel,
			source.ContainerLabel,
			source.ResourceLabel,
		},
		aggregator.AverageOverTime,
		func(labels map[string]string) bool {
			return labels[source.ContainerLabel] != "" && labels[source.NodeLabel] != "" && labels[source.PodLabel] != "" && labels[source.UnitLabel] == "integer"
		},
	)
}

//	avg(
//		avg_over_time(
//			DCGM_FI_DEV_DEC_UTIL{
//				container!="",
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (container, pod, namespace, device, modelName, UUID, cluster_id)

func NewGPUInfoMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.GPUInfoID,
		metric.DCGMFIDEVDECUTIL,
		[]string{
			source.NamespaceLabel,
			source.PodLabel,
			source.UIDLabel,
			source.ContainerLabel,
			source.DeviceLabel,
			source.ModelNameLabel,
			source.UUIDLabel,
		},
		aggregator.Info,
		func(labels map[string]string) bool {
			return labels[source.ContainerLabel] != ""
		},
	)
}

//	avg(
//		avg_over_time(
//			node_cpu_hourly_cost{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (node, cluster_id, instance_type, provider_id)

func NewNodeCPUPricePerHourMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NodeCPUPricePerHourID,
		metric.NodeCPUHourlyCost,
		[]string{
			source.NodeLabel,
			source.InstanceTypeLabel,
			source.ProviderIDLabel,
			source.UIDLabel,
		},
		aggregator.AverageOverTime,
		nil,
	)
}

//	avg(
//		avg_over_time(
//			node_ram_hourly_cost{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (node, cluster_id, instance_type, provider_id)

func NewNodeRAMPricePerGiBHourMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NodeRAMPricePerGiBHourID,
		metric.NodeRAMHourlyCost,
		[]string{
			source.NodeLabel,
			source.InstanceTypeLabel,
			source.ProviderIDLabel,
			source.UIDLabel,
		},
		aggregator.AverageOverTime,
		nil,
	)
}

//	avg(
//		avg_over_time(
//			node_gpu_hourly_cost{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (node, cluster_id, instance_type, provider_id)

func NewNodeGPUPricePerHourMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NodeGPUPricePerHourID,
		metric.NodeGPUHourlyCost,
		[]string{
			source.NodeLabel,
			source.InstanceTypeLabel,
			source.ProviderIDLabel,
			source.UIDLabel,
		},
		aggregator.AverageOverTime,
		nil,
	)
}

//	avg_over_time(
//		kubecost_node_is_spot{
//			<some_custom_filter>
//		}[1h]
//	)

func NewNodeIsSpotMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NodeIsSpotID,
		metric.KubecostNodeIsSpot,
		[]string{
			source.NodeLabel,
			source.ProviderIDLabel,
			source.UIDLabel,
		},
		aggregator.AverageOverTime,
		nil,
	)
}

//	avg(
//		avg_over_time(
//			pod_pvc_allocation{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (persistentvolume, persistentvolumeclaim, pod, namespace, cluster_id)

func NewPodPVCAllocationMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.PodPVCAllocationID,
		metric.PodPVCAllocation,
		[]string{
			source.NamespaceLabel,
			source.PodLabel,
			source.UIDLabel,
			source.PVLabel,
			source.PVCLabel,
		},
		aggregator.AverageOverTime,
		nil,
	)
}

//	avg(
//		avg_over_time(
//			kube_persistentvolumeclaim_resource_requests_storage_bytes{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (persistentvolumeclaim, namespace, cluster_id)

func NewPVCBytesRequestedMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.PVCBytesRequestedID,
		metric.KubePersistentVolumeClaimResourceRequestsStorageBytes,
		[]string{
			source.NamespaceLabel,
			source.PVCLabel,
			source.UIDLabel,
		},
		aggregator.AverageOverTime,
		nil,
	)
}

//	avg(
//		avg_over_time(
//			kube_persistentvolume_capacity_bytes{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (persistentvolume, cluster_id)

func NewPVBytesMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.PVBytesID,
		metric.KubePersistentVolumeCapacityBytes,
		[]string{
			source.PVLabel,
			source.UIDLabel,
		},
		aggregator.AverageOverTime,
		nil,
	)
}

//	avg(
//		avg_over_time(
//			kubecost_pv_info{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (cluster_id, storageclass, persistentvolume, provider_id)

func NewPVInfoMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.PVInfoID,
		metric.KubecostPVInfo,
		[]string{
			source.PVLabel,
			source.StorageClassLabel,
			source.ProviderIDLabel,
			source.UIDLabel,
		},
		aggregator.AverageOverTime,
		nil,
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
//

func NewNetZoneGiBMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NetZoneGiBID,
		metric.KubecostPodNetworkEgressBytesTotal,
		[]string{
			source.NamespaceLabel,
			source.PodNameLabel,
			source.UIDLabel,
		},
		aggregator.Increase,
		func(labels map[string]string) bool {
			return labels[source.InternetLabel] == "false" && labels[source.SameZoneLabel] == "false" && labels[source.SameRegionLabel] == "true"
		},
	)
}

//	avg(
//		avg_over_time(
//			kubecost_network_zone_egress_cost{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (cluster_id)
//

func NewNetZonePricePerGiBMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NetZonePricePerGiBID,
		metric.KubecostNetworkZoneEgressCost,
		[]string{},
		aggregator.AverageOverTime,
		nil,
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

func NewNetRegionGiBMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NetRegionGiBID,
		metric.KubecostPodNetworkEgressBytesTotal,
		[]string{
			source.NamespaceLabel,
			source.PodNameLabel,
			source.UIDLabel,
		},
		aggregator.Increase,
		func(labels map[string]string) bool {
			return labels[source.InternetLabel] == "false" && labels[source.SameZoneLabel] == "false" && labels[source.SameRegionLabel] == "false"
		},
	)
}

//	avg(
//		avg_over_time(
//			kubecost_network_region_egress_cost{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (cluster_id)

func NewNetRegionPricePerGiBMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NetRegionPricePerGiBID,
		metric.KubecostNetworkRegionEgressCost,
		[]string{},
		aggregator.AverageOverTime,
		nil,
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

func NewNetInternetGiBMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NetInternetGiBID,
		metric.KubecostPodNetworkEgressBytesTotal,
		[]string{
			source.NamespaceLabel,
			source.PodNameLabel,
			source.UIDLabel,
		},
		aggregator.Increase,
		func(labels map[string]string) bool {
			return labels[source.InternetLabel] == "true"
		},
	)
}

//	avg(
//		avg_over_time(
//			kubecost_network_internet_egress_cost{
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (cluster_id)

func NewNetInternetPricePerGiBMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NetInternetPricePerGiBID,
		metric.KubecostNetworkInternetEgressCost,
		[]string{},
		aggregator.AverageOverTime,
		nil,
	)
}

//	sum(
//		increase(
//			kubecost_pod_network_egress_bytes_total{
//				internet="true",
//				<some_custom_filter>
//			}[%s]
//		)
//	) by (pod_name, namespace, service, %s) / 1024 / 1024 / 1024

func NewNetInternetServiceGiBMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NetInternetServiceGiBID,
		metric.KubecostPodNetworkEgressBytesTotal,
		[]string{
			source.NamespaceLabel,
			source.PodNameLabel,
			source.ServiceLabel,
			source.UIDLabel,
		},
		aggregator.Increase,
		func(labels map[string]string) bool {
			return labels[source.InternetLabel] == "true"
		},
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

func NewNetReceiveBytesMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NetReceiveBytesID,
		metric.ContainerNetworkReceiveBytesTotal,
		[]string{
			source.NamespaceLabel,
			source.PodLabel,
			source.UIDLabel,
		},
		aggregator.Increase,
		func(labels map[string]string) bool {
			return labels[source.PodLabel] != ""
		},
	)
}

//	sum(
//		increase(
//			kubecost_pod_network_ingress_bytes_total{
//				internet="false",
//				same_zone="false",
//				same_region="true",
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (pod_name, namespace, cluster_id) / 1024 / 1024 / 1024

func NewNetZoneIngressGiBMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NetZoneIngressGiBID,
		metric.KubecostPodNetworkIngressBytesTotal,
		[]string{
			source.NamespaceLabel,
			source.PodNameLabel,
			source.UIDLabel,
		},
		aggregator.Increase,
		func(labels map[string]string) bool {
			return labels[source.InternetLabel] == "false" &&
				labels[source.SameZoneLabel] == "false" &&
				labels[source.SameRegionLabel] == "true"
		},
	)
}

//	sum(
//		increase(
//			kubecost_pod_network_ingress_bytes_total{
//				internet="false",
//				same_zone="false",
//				same_region="false",
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (pod_name, namespace, cluster_id) / 1024 / 1024 / 1024

func NewNetRegionIngressGiBMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NetRegionIngressGiBID,
		metric.KubecostPodNetworkIngressBytesTotal,
		[]string{
			source.NamespaceLabel,
			source.PodNameLabel,
			source.UIDLabel,
		},
		aggregator.Increase,
		func(labels map[string]string) bool {
			return labels[source.InternetLabel] == "false" &&
				labels[source.SameZoneLabel] == "false" &&
				labels[source.SameRegionLabel] == "false"
		},
	)
}

//	sum(
//		increase(
//			kubecost_pod_network_ingress_bytes_total{
//				internet="true",
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (pod_name, namespace, cluster_id) / 1024 / 1024 / 1024

func NewNetInternetIngressGiBMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NetInternetIngressGiBID,
		metric.KubecostPodNetworkIngressBytesTotal,
		[]string{
			source.NamespaceLabel,
			source.PodNameLabel,
			source.UIDLabel,
		},
		aggregator.Increase,
		func(labels map[string]string) bool {
			return labels[source.InternetLabel] == "true"
		},
	)
}

//	`sum(
//		increase(
//			kubecost_pod_network_ingress_bytes_total{
//				internet="true",
//				<some_custom_filter>
//			}[1h]
//		)
//	) by (pod_name, namespace, service, cluster_id) / 1024 / 1024 / 1024

func NewNetInternetServiceIngressGiBMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NetInternetServiceIngressGiBID,
		metric.KubecostPodNetworkIngressBytesTotal,
		[]string{
			source.NamespaceLabel,
			source.PodNameLabel,
			source.ServiceLabel,
			source.UIDLabel,
		},
		aggregator.Increase,
		func(labels map[string]string) bool {
			return labels[source.InternetLabel] == "true"
		},
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

func NewNetTransferBytesMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NetTransferBytesID,
		metric.ContainerNetworkTransmitBytesTotal,
		[]string{
			source.NamespaceLabel,
			source.PodLabel,
			source.UIDLabel,
		},
		aggregator.Increase,
		func(labels map[string]string) bool {
			return labels[source.PodLabel] != ""
		},
	)
}

//	avg_over_time(
//		kube_namespace_labels{
//			<some_custom_filter>
//		}[1h]
//	)

func NewNamespaceLabelsMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NamespaceLabelsID,
		metric.KubeNamespaceLabels,
		[]string{
			source.NamespaceLabel,
			source.UIDLabel,
		},
		aggregator.Info,
		nil,
	)
}

//	avg_over_time(
//		kube_namespace_annotations{
//			<some_custom_filter>
//		}[1h]
//	)

func NewNamespaceAnnotationsMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.NamespaceAnnotationsID,
		metric.KubeNamespaceAnnotations,
		[]string{
			source.NamespaceLabel,
			source.UIDLabel,
		},
		aggregator.Info,
		nil,
	)
}

//	avg_over_time(
//		kube_pod_labels{
//			<some_custom_filter>
//		}[1h]
//	)

func NewPodLabelsMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.PodLabelsID,
		metric.KubePodLabels,
		[]string{
			source.NamespaceLabel,
			source.PodLabel,
			source.UIDLabel,
		},
		aggregator.Info,
		nil,
	)
}

//	avg_over_time(
//		kube_pod_annotations{
//			<some_custom_filter>
//		}[1h]
//	)

func NewPodAnnotationsMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.PodAnnotationsID,
		metric.KubePodAnnotations,
		[]string{
			source.NamespaceLabel,
			source.PodLabel,
			source.UIDLabel,
		},
		aggregator.Info,
		nil,
	)
}

//	avg_over_time(
//		service_selector_labels{
//			<some_custom_filter>
//		}[1h]
//	)

func NewServiceLabelsMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.ServiceLabelsID,
		metric.ServiceSelectorLabels,
		[]string{
			source.NamespaceLabel,
			source.ServiceLabel,
			source.UIDLabel,
		},
		aggregator.Info,
		nil,
	)
}

//	avg_over_time(
//		deployment_match_labels{
//			<some_custom_filter>
//		}[1h]
//	)

func NewDeploymentLabelsMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.DeploymentLabelsID,
		metric.DeploymentMatchLabels,
		[]string{
			source.NamespaceLabel,
			source.DeploymentLabel,
			source.UIDLabel,
		},
		aggregator.Info,
		nil,
	)
}

//	avg_over_time(
//		statefulSet_match_labels{
//			<some_custom_filter>
//		}[1h]
//	)

func NewStatefulSetLabelsMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.StatefulSetLabelsID,
		metric.StatefulSetMatchLabels,
		[]string{
			source.NamespaceLabel,
			source.StatefulSetLabel,
			source.UIDLabel,
		},
		aggregator.Info,
		nil,
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

func NewDaemonSetLabelsMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.DaemonSetLabelsID,
		metric.KubePodOwner,
		[]string{
			source.NamespaceLabel,
			source.PodLabel,
			source.UIDLabel,
			source.OwnerNameLabel,
		},
		aggregator.Info,
		func(labels map[string]string) bool {
			return labels[source.OwnerKindLabel] == "DaemonSet"
		},
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

func NewJobLabelsMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.JobLabelsID,
		metric.KubePodOwner,
		[]string{
			source.NamespaceLabel,
			source.PodLabel,
			source.UIDLabel,
			source.OwnerNameLabel,
		},
		aggregator.Info,
		func(labels map[string]string) bool {
			return labels[source.OwnerKindLabel] == "Job"
		},
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

func NewPodsWithReplicaSetOwnerMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.PodsWithReplicaSetOwnerID,
		metric.KubePodOwner,
		[]string{
			source.NamespaceLabel,
			source.PodLabel,
			source.UIDLabel,
			source.OwnerNameLabel,
		},
		aggregator.Info,
		func(labels map[string]string) bool {
			return labels[source.OwnerKindLabel] == "ReplicaSet"
		},
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

func NewReplicaSetsWithoutOwnersMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.ReplicaSetsWithoutOwnersID,
		metric.KubeReplicasetOwner,
		[]string{
			source.NamespaceLabel,
			source.ReplicaSetLabel,
			source.UIDLabel,
		},
		aggregator.Info,
		func(labels map[string]string) bool {
			return labels[source.OwnerKindLabel] == "<none>" && labels[source.OwnerNameLabel] == "<none>"
		},
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

func NewReplicaSetsWithRolloutMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.ReplicaSetsWithRolloutID,
		metric.KubeReplicasetOwner,
		[]string{
			source.NamespaceLabel,
			source.ReplicaSetLabel,
			source.UIDLabel,
			source.OwnerNameLabel,
			source.OwnerKindLabel,
		},
		aggregator.Info,
		func(labels map[string]string) bool {
			return labels[source.OwnerKindLabel] == "Rollout"
		},
	)
}

// avg(
//	avg_over_time(
//		resourcequota_spec_resource_requests{
//			resource="cpu",
//			unit="core",
//			<some_custom_filter>
//		}[1h]
//	)
//) by (resourcequota, namespace, uid, cluster_id)

func NewResourceQuotaSpecCPURequestAverageMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.ResourceQuotaSpecCPURequestAverageID,
		metric.KubeResourceQuotaSpecResourceRequests,
		[]string{
			source.NamespaceLabel,
			source.ResourceQuotaLabel,
			source.UIDLabel,
		},
		aggregator.AverageOverTime,
		func(labels map[string]string) bool {
			return labels[source.ResourceLabel] == "cpu" && labels[source.UnitLabel] == "core"
		},
	)
}

// max(
//	max_over_time(
//		resourcequota_spec_resource_requests{
//			resource="cpu",
//			unit="core",
//			<some_custom_filter>
//		}[1h]
//	)
//) by (resourcequota, namespace, uid, cluster_id)

func NewResourceQuotaSpecCPURequestMaxMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.ResourceQuotaSpecCPURequestMaxID,
		metric.KubeResourceQuotaSpecResourceRequests,
		[]string{
			source.NamespaceLabel,
			source.ResourceQuotaLabel,
			source.UIDLabel,
		},
		aggregator.MaxOverTime,
		func(labels map[string]string) bool {
			return labels[source.ResourceLabel] == "cpu" && labels[source.UnitLabel] == "core"
		},
	)
}

// avg(
//	avg_over_time(
//		resourcequota_spec_resource_requests{
//			resource="memory",
//			unit="byte",
//			<some_custom_filter>
//		}[1h]
//	)
//) by (resourcequota, namespace, uid, cluster_id)

func NewResourceQuotaSpecRAMRequestAverageMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.ResourceQuotaSpecRAMRequestAverageID,
		metric.KubeResourceQuotaSpecResourceRequests,
		[]string{
			source.NamespaceLabel,
			source.ResourceQuotaLabel,
			source.UIDLabel,
		},
		aggregator.AverageOverTime,
		func(labels map[string]string) bool {
			return labels[source.ResourceLabel] == "memory" && labels[source.UnitLabel] == "byte"
		},
	)
}

// max(
//	max_over_time(
//		resourcequota_spec_resource_requests{
//			resource="memory",
//			unit="byte",
//			<some_custom_filter>
//		}[1h]
//	)
//) by (resourcequota, namespace, uid, cluster_id)

func NewResourceQuotaSpecRAMRequestMaxMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.ResourceQuotaSpecRAMRequestMaxID,
		metric.KubeResourceQuotaSpecResourceRequests,
		[]string{
			source.NamespaceLabel,
			source.ResourceQuotaLabel,
			source.UIDLabel,
		},
		aggregator.MaxOverTime,
		func(labels map[string]string) bool {
			return labels[source.ResourceLabel] == "memory" && labels[source.UnitLabel] == "byte"
		},
	)
}

// avg(
//	avg_over_time(
//		resourcequota_spec_resource_limits{
//			resource="cpu",
//			unit="core",
//			<some_custom_filter>
//		}[1h]
//	)
//) by (resourcequota, namespace, uid, cluster_id)

func NewResourceQuotaSpecCPULimitAverageMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.ResourceQuotaSpecCPULimitAverageID,
		metric.KubeResourceQuotaSpecResourceLimits,
		[]string{
			source.NamespaceLabel,
			source.ResourceQuotaLabel,
			source.UIDLabel,
		},
		aggregator.AverageOverTime,
		func(labels map[string]string) bool {
			return labels[source.ResourceLabel] == "cpu" && labels[source.UnitLabel] == "core"
		},
	)
}

// max(
//	max_over_time(
//		resourcequota_spec_resource_limits{
//			resource="cpu",
//			unit="core",
//			<some_custom_filter>
//		}[1h]
//	)
//) by (resourcequota, namespace, uid, cluster_id)

func NewResourceQuotaSpecCPULimitMaxMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.ResourceQuotaSpecCPULimitMaxID,
		metric.KubeResourceQuotaSpecResourceLimits,
		[]string{
			source.NamespaceLabel,
			source.ResourceQuotaLabel,
			source.UIDLabel,
		},
		aggregator.MaxOverTime,
		func(labels map[string]string) bool {
			return labels[source.ResourceLabel] == "cpu" && labels[source.UnitLabel] == "core"
		},
	)
}

// avg(
//	avg_over_time(
//		resourcequota_spec_resource_limits{
//			resource="memory",
//			unit="byte",
//			<some_custom_filter>
//		}[1h]
//	)
//) by (resourcequota, namespace, uid, cluster_id)

func NewResourceQuotaSpecRAMLimitAverageMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.ResourceQuotaSpecRAMLimitAverageID,
		metric.KubeResourceQuotaSpecResourceLimits,
		[]string{
			source.NamespaceLabel,
			source.ResourceQuotaLabel,
			source.UIDLabel,
		},
		aggregator.AverageOverTime,
		func(labels map[string]string) bool {
			return labels[source.ResourceLabel] == "memory" && labels[source.UnitLabel] == "byte"
		},
	)
}

// max(
//	max_over_time(
//		resourcequota_spec_resource_limits{
//			resource="memory",
//			unit="byte",
//			<some_custom_filter>
//		}[1h]
//	)
//) by (resourcequota, namespace, uid, cluster_id)

func NewResourceQuotaSpecRAMLimitMaxMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.ResourceQuotaSpecRAMLimitMaxID,
		metric.KubeResourceQuotaSpecResourceLimits,
		[]string{
			source.NamespaceLabel,
			source.ResourceQuotaLabel,
			source.UIDLabel,
		},
		aggregator.MaxOverTime,
		func(labels map[string]string) bool {
			return labels[source.ResourceLabel] == "memory" && labels[source.UnitLabel] == "byte"
		},
	)
}

// avg(
//	avg_over_time(
//		resourcequota_status_used_resource_requests{
//			resource="cpu",
//			unit="core",
//			<some_custom_filter>
//		}[1h]
//	)
//) by (resourcequota, namespace, uid, cluster_id)

func NewResourceQuotaStatusUsedCPURequestAverageMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.ResourceQuotaStatusUsedCPURequestAverageID,
		metric.KubeResourceQuotaStatusUsedResourceRequests,
		[]string{
			source.NamespaceLabel,
			source.ResourceQuotaLabel,
			source.UIDLabel,
		},
		aggregator.AverageOverTime,
		func(labels map[string]string) bool {
			return labels[source.ResourceLabel] == "cpu" && labels[source.UnitLabel] == "core"
		},
	)
}

// max(
//	max_over_time(
//		resourcequota_status_used_resource_requests{
//			resource="cpu",
//			unit="core",
//			<some_custom_filter>
//		}[1h]
//	)
//) by (resourcequota, namespace, uid, cluster_id)

func NewResourceQuotaStatusUsedCPURequestMaxMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.ResourceQuotaStatusUsedCPURequestMaxID,
		metric.KubeResourceQuotaStatusUsedResourceRequests,
		[]string{
			source.NamespaceLabel,
			source.ResourceQuotaLabel,
			source.UIDLabel,
		},
		aggregator.MaxOverTime,
		func(labels map[string]string) bool {
			return labels[source.ResourceLabel] == "cpu" && labels[source.UnitLabel] == "core"
		},
	)
}

// avg(
//	avg_over_time(
//		resourcequota_status_used_resource_requests{
//			resource="memory",
//			unit="byte",
//			<some_custom_filter>
//		}[1h]
//	)
//) by (resourcequota, namespace, uid, cluster_id)

func NewResourceQuotaStatusUsedRAMRequestAverageMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.ResourceQuotaStatusUsedRAMRequestAverageID,
		metric.KubeResourceQuotaStatusUsedResourceRequests,
		[]string{
			source.NamespaceLabel,
			source.ResourceQuotaLabel,
			source.UIDLabel,
		},
		aggregator.AverageOverTime,
		func(labels map[string]string) bool {
			return labels[source.ResourceLabel] == "memory" && labels[source.UnitLabel] == "byte"
		},
	)
}

// max(
//	max_over_time(
//		resourcequota_status_used_resource_requests{
//			resource="memory",
//			unit="byte",
//			<some_custom_filter>
//		}[1h]
//	)
//) by (resourcequota, namespace, uid, cluster_id)

func NewResourceQuotaStatusUsedRAMRequestMaxMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.ResourceQuotaStatusUsedRAMRequestMaxID,
		metric.KubeResourceQuotaStatusUsedResourceRequests,
		[]string{
			source.NamespaceLabel,
			source.ResourceQuotaLabel,
			source.UIDLabel,
		},
		aggregator.MaxOverTime,
		func(labels map[string]string) bool {
			return labels[source.ResourceLabel] == "memory" && labels[source.UnitLabel] == "byte"
		},
	)
}

// avg(
//	avg_over_time(
//		resourcequota_status_used_resource_limits{
//			resource="cpu",
//			unit="core",
//			<some_custom_filter>
//		}[1h]
//	)
//) by (resourcequota, namespace, uid, cluster_id)

func NewResourceQuotaStatusUsedCPULimitAverageMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.ResourceQuotaStatusUsedCPULimitAverageID,
		metric.KubeResourceQuotaStatusUsedResourceLimits,
		[]string{
			source.NamespaceLabel,
			source.ResourceQuotaLabel,
			source.UIDLabel,
		},
		aggregator.AverageOverTime,
		func(labels map[string]string) bool {
			return labels[source.ResourceLabel] == "cpu" && labels[source.UnitLabel] == "core"
		},
	)
}

// max(
//	max_over_time(
//		resourcequota_status_used_resource_limits{
//			resource="cpu",
//			unit="core",
//			<some_custom_filter>
//		}[1h]
//	)
//) by (resourcequota, namespace, uid, cluster_id)

func NewResourceQuotaStatusUsedCPULimitMaxMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.ResourceQuotaStatusUsedCPULimitMaxID,
		metric.KubeResourceQuotaStatusUsedResourceLimits,
		[]string{
			source.NamespaceLabel,
			source.ResourceQuotaLabel,
			source.UIDLabel,
		},
		aggregator.MaxOverTime,
		func(labels map[string]string) bool {
			return labels[source.ResourceLabel] == "cpu" && labels[source.UnitLabel] == "core"
		},
	)
}

// avg(
//	avg_over_time(
//		resourcequota_status_used_resource_limits{
//			resource="memory",
//			unit="byte",
//			<some_custom_filter>
//		}[1h]
//	)
//) by (resourcequota, namespace, uid, cluster_id)

func NewResourceQuotaStatusUsedRAMLimitAverageMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.ResourceQuotaStatusUsedRAMLimitAverageID,
		metric.KubeResourceQuotaStatusUsedResourceLimits,
		[]string{
			source.NamespaceLabel,
			source.ResourceQuotaLabel,
			source.UIDLabel,
		},
		aggregator.AverageOverTime,
		func(labels map[string]string) bool {
			return labels[source.ResourceLabel] == "memory" && labels[source.UnitLabel] == "byte"
		},
	)
}

// max(
//	max_over_time(
//		resourcequota_status_used_resource_limits{
//			resource="memory",
//			unit="byte",
//			<some_custom_filter>
//		}[1h]
//	)
//) by (resourcequota, namespace, uid, cluster_id)

func NewResourceQuotaStatusUsedRAMLimitMaxMetricCollector() *metric.MetricCollector {
	return metric.NewMetricCollector(
		metric.ResourceQuotaStatusUsedRAMLimitMaxID,
		metric.KubeResourceQuotaStatusUsedResourceLimits,
		[]string{
			source.NamespaceLabel,
			source.ResourceQuotaLabel,
			source.UIDLabel,
		},
		aggregator.MaxOverTime,
		func(labels map[string]string) bool {
			return labels[source.ResourceLabel] == "memory" && labels[source.UnitLabel] == "byte"
		},
	)
}
