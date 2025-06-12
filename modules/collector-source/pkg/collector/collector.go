package collector

import (
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric/aggregator"
	"github.com/opencost/opencost/modules/collector-source/pkg/scrape"
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
	memStore.Register(NewRAMUsageAverageMetricCollector())
	memStore.Register(NewRAMUsageMaxMetricCollector())
	memStore.Register(NewCPUCoresAllocatedMetricCollector())
	memStore.Register(NewCPURequestsMetricCollector())
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
		scrape.PVHourlyCost,
		[]string{
			source.VolumeNameLabel,
			source.PVLabel,
			source.ProviderIDLabel,
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
		scrape.KubeletVolumeStatsUsedBytes,
		[]string{
			source.NamespaceLabel,
			source.PVCLabel,
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
		scrape.KubeletVolumeStatsUsedBytes,
		[]string{
			source.NamespaceLabel,
			source.PVCLabel,
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
		scrape.KubePersistentVolumeClaimInfo,
		[]string{
			source.NamespaceLabel,
			source.VolumeNameLabel,
			source.PVCLabel,
			source.StorageClassLabel,
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
		scrape.KubePersistentVolumeCapacityBytes,
		[]string{
			source.PVLabel,
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
		scrape.ContainerFSUsageBytes,
		[]string{
			source.InstanceLabel,
			source.DeviceLabel,
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
		scrape.ContainerFSUsageBytes,
		[]string{
			source.InstanceLabel,
			source.DeviceLabel,
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
		scrape.ContainerFSUsageBytes,
		[]string{
			source.InstanceLabel,
			source.DeviceLabel,
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
		scrape.NodeFSCapacityBytes,
		[]string{
			source.InstanceLabel,
			source.DeviceLabel,
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
		scrape.NodeTotalHourlyCost,
		[]string{
			source.NodeLabel,
			source.ProviderIDLabel,
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
		scrape.KubeNodeStatusCapacityCPUCores,
		[]string{
			source.NodeLabel,
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
		scrape.KubeNodeStatusAllocatableCPUCores,
		[]string{
			source.NodeLabel,
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
		scrape.KubeNodeStatusCapacityMemoryBytes,
		[]string{
			source.NodeLabel,
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
		scrape.KubeNodeStatusAllocatableMemoryBytes,
		[]string{
			source.NodeLabel,
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
		scrape.NodeGPUCount,
		[]string{
			source.NodeLabel,
			source.ProviderIDLabel,
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
		scrape.KubeNodeLabels,
		[]string{
			source.NodeLabel,
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
		scrape.NodeTotalHourlyCost,
		[]string{
			source.NodeLabel,
			source.ProviderIDLabel,
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
		scrape.NodeCPUSecondsTotal,
		[]string{
			source.KubernetesNodeLabel,
			source.ModeLabel,
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
		scrape.ContainerMemoryWorkingSetBytes,
		[]string{
			source.InstanceLabel,
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
		scrape.ContainerMemoryWorkingSetBytes,
		[]string{
			source.InstanceLabel,
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
		scrape.KubecostLoadBalancerCost,
		[]string{
			source.NamespaceLabel,
			source.ServiceNameLabel,
			source.IngressIPLabel,
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
		scrape.KubecostLoadBalancerCost,
		[]string{
			source.NamespaceLabel,
			source.ServiceNameLabel,
			source.IngressIPLabel,
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
		scrape.KubecostClusterManagementCost,
		[]string{
			source.ProvisionerNameLabel,
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
		scrape.KubecostClusterManagementCost,
		[]string{
			source.ProvisionerNameLabel,
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
		scrape.KubePodContainerStatusRunning,
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
		scrape.ContainerMemoryAllocationBytes,
		[]string{
			source.NodeLabel,
			source.InstanceLabel,
			source.NamespaceLabel,
			source.PodLabel,
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
		scrape.KubePodContainerResourceRequests,
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
		scrape.ContainerMemoryWorkingSetBytes,
		[]string{
			source.NodeLabel,
			source.InstanceLabel,
			source.NamespaceLabel,
			source.PodLabel,
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
		scrape.ContainerMemoryWorkingSetBytes,
		[]string{
			source.NodeLabel,
			source.InstanceLabel,
			source.NamespaceLabel,
			source.PodLabel,
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
		scrape.ContainerCPUAllocation,
		[]string{
			source.NodeLabel,
			source.InstanceLabel,
			source.NamespaceLabel,
			source.PodLabel,
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
		scrape.KubePodContainerResourceRequests,
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
		scrape.ContainerCPUUsageSecondsTotal,
		[]string{
			source.NodeLabel,
			source.InstanceLabel,
			source.NamespaceLabel,
			source.PodLabel,
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
		scrape.ContainerCPUUsageSecondsTotal,
		[]string{
			source.NodeLabel,
			source.InstanceLabel,
			source.NamespaceLabel,
			source.PodLabel,
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
		scrape.KubePodContainerResourceRequests,
		[]string{
			source.NamespaceLabel,
			source.PodLabel,
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
		scrape.DCGMFIPROFGRENGINEACTIVE,
		[]string{
			source.NamespaceLabel,
			source.PodLabel,
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
		scrape.DCGMFIPROFGRENGINEACTIVE,
		[]string{
			source.NamespaceLabel,
			source.PodLabel,
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
		scrape.ContainerGPUAllocation,
		[]string{
			source.NamespaceLabel,
			source.PodLabel,
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
		scrape.KubePodContainerResourceRequests,
		[]string{
			source.NamespaceLabel,
			source.PodLabel,
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
		scrape.DCGMFIDEVDECUTIL,
		[]string{
			source.NamespaceLabel,
			source.PodLabel,
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
		scrape.NodeCPUHourlyCost,
		[]string{
			source.NodeLabel,
			source.InstanceTypeLabel,
			source.ProviderIDLabel,
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
		scrape.NodeRAMHourlyCost,
		[]string{
			source.NodeLabel,
			source.InstanceTypeLabel,
			source.ProviderIDLabel,
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
		scrape.NodeGPUHourlyCost,
		[]string{
			source.NodeLabel,
			source.InstanceTypeLabel,
			source.ProviderIDLabel,
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
		scrape.KubecostNodeIsSpot,
		[]string{
			source.NodeLabel,
			source.ProviderIDLabel,
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
		scrape.PodPVCAllocation,
		[]string{
			source.NamespaceLabel,
			source.PodLabel,
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
		scrape.KubePersistentVolumeClaimResourceRequestsStorageBytes,
		[]string{
			source.NamespaceLabel,
			source.PVCLabel,
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
		scrape.KubePersistentVolumeCapacityBytes,
		[]string{
			source.PVLabel,
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
		scrape.KubecostPVInfo,
		[]string{
			source.PVLabel,
			source.StorageClassLabel,
			source.ProviderIDLabel,
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
		scrape.KubecostPodNetworkEgressBytesTotal,
		[]string{
			source.NamespaceLabel,
			source.PodNameLabel,
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
		scrape.KubecostNetworkZoneEgressCost,
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
		scrape.KubecostPodNetworkEgressBytesTotal,
		[]string{
			source.NamespaceLabel,
			source.PodNameLabel,
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
		scrape.KubecostNetworkRegionEgressCost,
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
		scrape.KubecostPodNetworkEgressBytesTotal,
		[]string{
			source.NamespaceLabel,
			source.PodNameLabel,
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
		scrape.KubecostNetworkInternetEgressCost,
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
		scrape.KubecostPodNetworkEgressBytesTotal,
		[]string{
			source.NamespaceLabel,
			source.PodNameLabel,
			source.ServiceLabel,
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
		scrape.ContainerNetworkReceiveBytesTotal,
		[]string{
			source.NamespaceLabel,
			source.PodLabel,
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
		scrape.KubecostPodNetworkIngressBytesTotal,
		[]string{
			source.NamespaceLabel,
			source.PodNameLabel,
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
		scrape.KubecostPodNetworkIngressBytesTotal,
		[]string{
			source.NamespaceLabel,
			source.PodNameLabel,
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
		scrape.KubecostPodNetworkIngressBytesTotal,
		[]string{
			source.NamespaceLabel,
			source.PodNameLabel,
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
		scrape.KubecostPodNetworkIngressBytesTotal,
		[]string{
			source.NamespaceLabel,
			source.PodNameLabel,
			source.ServiceLabel,
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
		scrape.ContainerNetworkTransmitBytesTotal,
		[]string{
			source.NamespaceLabel,
			source.PodLabel,
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
		scrape.KubeNamespaceLabels,
		[]string{
			source.NamespaceLabel,
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
		scrape.KubeNamespaceAnnotations,
		[]string{
			source.NamespaceLabel,
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
		scrape.KubePodLabels,
		[]string{
			source.NamespaceLabel,
			source.PodLabel,
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
		scrape.KubePodAnnotations,
		[]string{
			source.NamespaceLabel,
			source.PodLabel,
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
		scrape.ServiceSelectorLabels,
		[]string{
			source.NamespaceLabel,
			source.ServiceLabel,
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
		scrape.DeploymentMatchLabels,
		[]string{
			source.NamespaceLabel,
			source.DeploymentLabel,
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
		scrape.StatefulSetMatchLabels,
		[]string{
			source.NamespaceLabel,
			source.StatefulSetLabel,
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
		scrape.KubePodOwner,
		[]string{
			source.NamespaceLabel,
			source.PodLabel,
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
		scrape.KubePodOwner,
		[]string{
			source.NamespaceLabel,
			source.PodLabel,
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
		scrape.KubePodOwner,
		[]string{
			source.NamespaceLabel,
			source.PodLabel,
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
		scrape.KubeReplicasetOwner,
		[]string{
			source.NamespaceLabel,
			source.ReplicaSetLabel,
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
		scrape.KubeReplicasetOwner,
		[]string{
			source.NamespaceLabel,
			source.ReplicaSetLabel,
			source.OwnerNameLabel,
			source.OwnerKindLabel,
		},
		aggregator.Info,
		func(labels map[string]string) bool {
			return labels[source.OwnerKindLabel] == "Rollout"
		},
	)
}
