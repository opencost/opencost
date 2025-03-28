package collector

import (
	"fmt"
	"slices"
	"sync"
	"time"
)

// Metric names
const (
	PVHourlyCost                                          = "pv_hourly_cost"
	KubeletVolumeStatsUsedBytes                           = "kubelet_volume_stats_used_bytes"
	KubePersistenVolumeClaimInfo                          = "kube_persistentvolumeclaim_info"
	KubePersistentVolumeCapacityBytes                     = "kube_persistentvolume_capacity_bytes"
	ContainerFSLimitBytes                                 = "container_fs_limit_bytes"
	ContainerFSUsageBytes                                 = "container_fs_usage_bytes"
	NodeTotalHourlyCost                                   = "node_total_hourly_cost"
	KubeNodeStatusCapacityCPUCores                        = "kube_node_status_capacity_cpu_cores"
	KubeNodeStatusCapacityMemoryBytes                     = "kube_node_status_capacity_memory_bytes"
	KubeNodeStatusAllocatableCPUCores                     = "kube_node_status_allocatable_cpu_cores"
	KubeNodeStatusAllocatableMemoryBytes                  = "kube_node_status_allocatable_memory_bytes"
	NodeGPUCount                                          = "node_gpu_count"
	KubeNodeLabels                                        = "kube_node_labels"
	NodeCPUSecondsTotal                                   = "node_cpu_seconds_total"
	KubecostLoadBalancerCost                              = "kubecost_load_balancer_cost"
	KubecostClusterManagementCost                         = "kubecost_cluster_management_cost"
	KubePodContainerStatusRunning                         = "kube_pod_container_status_running"
	ContainerMemoryAllocationBytes                        = "container_memory_allocation_bytes"
	KubePodContainerResourceRequests                      = "kube_pod_container_resource_requests"
	ContainerMemoryWorkingSetBytes                        = "container_memory_working_set_bytes"
	ContainerCPUAllocation                                = "container_cpu_allocation"
	ContainerCPUUsageSecondsTotal                         = "container_cpu_usage_seconds_total"
	KubecostContainerCPUUsageIrate                        = "kubecost_container_cpu_usage_irate"
	DCGMFIPROFGRENGINEACTIVE                              = "DCGM_FI_PROF_GR_ENGINE_ACTIVE"
	ContainerGPUAllocation                                = "container_gpu_allocation"
	DCGMFIDEVDECUTIL                                      = "DCGM_FI_DEV_DEC_UTIL"
	NodeCPUHourlyCost                                     = "node_cpu_hourly_cost"
	NodeRAMHourlyCost                                     = "node_ram_hourly_cost"
	NodeGPUHourlyCost                                     = "node_gpu_hourly_cost"
	KubecostNodeIsSpot                                    = "kubecost_node_is_spot"
	PodPVCAllocation                                      = "pod_pvc_allocation"
	KubePersistentVolumeClaimResourceRequestsStorageBytes = "kube_persistentvolumeclaim_resource_requests_storage_bytes"
	KubecostPVInfo                                        = "kubecost_pv_info"
	KubecostPodNetworkEgressBytesTotal                    = "kubecost_pod_network_egress_bytes_total"
	KubecostNetworkZoneEgressCost                         = "kubecost_network_zone_egress_cost"
	KubecostNetworkRegionEgressCost                       = "kubecost_network_region_egress_cost"
	KubecostNetworkInternetEgressCost                     = "kubecost_network_internet_egress_cost"
	ContainerNetworkReceiveBytesTotal                     = "container_network_receive_bytes_total"
	ContainerNetworkTransmitBytesTotal                    = "container_network_transmit_bytes_total"
	KubeNamespaceLabels                                   = "kube_namespace_labels"
	KubeNamespaceAnnotations                              = "kube_namespace_annotations"
	KubePodLabels                                         = "kube_pod_labels"
	KubePodAnnotations                                    = "kube_pod_annotations"
	ServiceSelectorLabels                                 = "service_selector_labels"
	DeploymentMatchLabels                                 = "deployment_match_labels"
	StatefulSetMatchLabels                                = "statefulSet_match_labels"
	KubePodOwner                                          = "kube_pod_owner"
	KubeReplicasetOwner                                   = "kube_replicaset_owner"
)

// MetricCollectorID is a unique identifier for a specific metric collector instance. We
// use this identifier to register and unregister metric instances from the metrics collector
// instead of the metric name and aggregation type to allow selectable cardinality (via labels)
// across multiple instances of the same aggregation type and metric name.
type MetricCollectorID string

const (
	PVPricePerGiBHourID             MetricCollectorID = "PVPricePerGiBHour"
	PVUsedAverageID                 MetricCollectorID = "PVUsedAverage"
	PVUsedMaxID                     MetricCollectorID = "PVUsedMax"
	PVCInfoID                       MetricCollectorID = "PVCInfo"
	PVActiveMinutesID               MetricCollectorID = "PVActiveMinutes"
	LocalStorageCostID              MetricCollectorID = "LocalStorageCost"
	LocalStorageUsedCostID          MetricCollectorID = "LocalStorageUsedCost"
	LocalStorageUsedAverageID       MetricCollectorID = "LocalStorageUsedAverage"
	LocalStorageUsedMaxID           MetricCollectorID = "LocalStorageUsedMax"
	LocalStorageBytesID             MetricCollectorID = "LocalStorageBytesID"
	LocalStorageActiveMinutesID     MetricCollectorID = "LocalStorageActiveMinutes"
	NodeCPUCoresCapacityID          MetricCollectorID = "NodeCPUCoresCapacity"
	NodeCPUCoresAllocatableID       MetricCollectorID = "NodeCPUCoresAllocatable"
	NodeRAMBytesCapacityID          MetricCollectorID = "NodeRAMBytesCapacity"
	NodeRAMBytesAllocatableID       MetricCollectorID = "NodeRAMBytesAllocatable"
	NodeGPUCountID                  MetricCollectorID = "NodeGPUCount"
	NodeLabelsID                    MetricCollectorID = "NodeLabels"
	NodeActiveMinutesID             MetricCollectorID = "NodeActiveMinutes"
	NodeCPUModeTotalID              MetricCollectorID = "NodeCPUModeTotal"
	NodeRAMSystemUsageAverageID     MetricCollectorID = "NodeRAMSystemUsageAverage"
	NodeRAMUserUsageAverageID       MetricCollectorID = "NodeRAMUserUsageAverage"
	LBPricePerHourID                MetricCollectorID = "LBPricePerHour"
	LBActiveMinutesID               MetricCollectorID = "LBActiveMinutes"
	ClusterManagementDurationID     MetricCollectorID = "ClusterManagementDuration"
	ClusterManagementPricePerHourID MetricCollectorID = "ClusterManagementPricePerHour"
	PodActiveMinutesID              MetricCollectorID = "PodActiveMinutes"
	RAMBytesAllocatedID             MetricCollectorID = "RAMBytesAllocated"
	RAMRequestsID                   MetricCollectorID = "RAMRequests"
	RAMUsageAverageID               MetricCollectorID = "RAMUsageAverage"
	RAMUsageMaxID                   MetricCollectorID = "RAMUsageMax"
	CPUCoresAllocatedID             MetricCollectorID = "CPUCoresAllocated"
	CPURequestsID                   MetricCollectorID = "CPURequestsID"
	CPUUsageAverageID               MetricCollectorID = "CPUUsageAverage"
	CPUUsageMaxID                   MetricCollectorID = "CPUUsageMax"
	GPUsRequestedID                 MetricCollectorID = "GPUsRequested"
	GPUsUsageAverageID              MetricCollectorID = "GPUsUsageAverage"
	GPUsUsageMaxID                  MetricCollectorID = "GPUsUsageMax"
	GPUsAllocatedID                 MetricCollectorID = "GPUsAllocated"
	IsGPUSharedID                   MetricCollectorID = "IsGPUShared"
	GPUInfoID                       MetricCollectorID = "GPUInfo"
	NodeCPUPricePerHourID           MetricCollectorID = "NodeCPUPricePerHour"
	NodeRAMPricePerGiBHourID        MetricCollectorID = "NodeRAMPricePerGiBHour"
	NodeGPUPricePerHourID           MetricCollectorID = "NodeGPUPricePerHour"
	NodeIsSpotID                    MetricCollectorID = "NodeIsSpot"
	PodPVCAllocationID              MetricCollectorID = "PodPVCAllocation"
	PVCBytesRequestedID             MetricCollectorID = "PVCBytesRequested"
	PVBytesID                       MetricCollectorID = "PVBytesID"
	PVCostPerGiBHourID              MetricCollectorID = "PVCostPerGiBHour"
	PVInfoID                        MetricCollectorID = "PVInfo"
	NetZoneGiBID                    MetricCollectorID = "NetZoneGiB"
	NetZonePricePerGiBID            MetricCollectorID = "NetZonePricePerGiB"
	NetRegionGiBID                  MetricCollectorID = "NetRegionGiB"
	NetRegionPricePerGiBID          MetricCollectorID = "NetRegionPricePerGiB"
	NetInternetGiBID                MetricCollectorID = "NetInternetGiB"
	NetInternetPricePerGiBID        MetricCollectorID = "NetInternetPricePerGiB"
	NetReceiveBytesID               MetricCollectorID = "NetReceiveBytes"
	NetTransferBytesID              MetricCollectorID = "NetTransferBytes"
	NamespaceLabelsID               MetricCollectorID = "NamespaceLabels"
	NamespaceAnnotationsID          MetricCollectorID = "NamespaceAnnotations"
	PodLabelsID                     MetricCollectorID = "PodLabels"
	PodAnnotationsID                MetricCollectorID = "PodAnnotations"
	ServiceLabelsID                 MetricCollectorID = "ServiceLabels"
	DeploymentLabelsID              MetricCollectorID = "DeploymentLabels"
	StatefulSetLabelsID             MetricCollectorID = "StatefulSetLabels"
	DaemonSetLabelsID               MetricCollectorID = "DaemonSetLabels"
	JobLabelsID                     MetricCollectorID = "JobLabels"
	PodsWithReplicaSetOwnerID       MetricCollectorID = "PodsWithReplicaSetOwner"
	ReplicaSetsWithoutOwnersID      MetricCollectorID = "ReplicaSetsWithoutOwners"
	ReplicaSetsWithRolloutID        MetricCollectorID = "ReplicaSetsWithRollout"
)

// MetricsCollector is an interface that defines an implementation capable of managing a collection
// of metric instances, and exposes helper methods for routing metric updates and queries to the
// proper collector instances.
type MetricsCollector interface {
	// Register accepts a `MetricCollector` instance and registers it for routing updates and querying.
	Register(collector *MetricCollector) error

	// Unregister accepts a `MetricCollectorID` and unregisters the metric collector instance from receiving metrics
	// updates and query availability.
	Unregister(collectorID MetricCollectorID) bool

	// Query accepts a `MetricCollectorID` and returns a slice of `MetricResult` instances for that collector.
	Query(collectorID MetricCollectorID) ([]*MetricResult, error)

	// Update accepts the name of a metric, the label set and values to update the metric, the updated value, and a timestamp.
	// This method does not accept a `MetricCollectorID` because it provides updates across many potential metric collector instances
	// which utilize the same metric.
	Update(metricName string, labels map[string]string, value float64, timestamp *time.Time)
}

// InMemoryMetricsCollector is a thread-safe implementation of the `MetricsCollector` interface that stores metric instances
// in memory.
type InMemoryMetricsCollector struct {
	lock          sync.Mutex
	byMetricName  map[string][]*MetricCollector
	byCollectorID map[MetricCollectorID]*MetricCollector
}

func NewInMemoryMetricsCollector() MetricsCollector {
	return &InMemoryMetricsCollector{
		byMetricName:  make(map[string][]*MetricCollector),
		byCollectorID: make(map[MetricCollectorID]*MetricCollector),
	}
}

func (immc *InMemoryMetricsCollector) Register(collector *MetricCollector) error {
	immc.lock.Lock()
	defer immc.lock.Unlock()

	if _, ok := immc.byCollectorID[collector.id]; ok {
		return fmt.Errorf("collector with ID: %s already exists", collector.id)
	}

	immc.byCollectorID[collector.id] = collector
	immc.byMetricName[collector.metricName] = append(immc.byMetricName[collector.metricName], collector)
	return nil
}

func (immc *InMemoryMetricsCollector) Unregister(collectorID MetricCollectorID) bool {
	immc.lock.Lock()
	defer immc.lock.Unlock()

	if _, ok := immc.byCollectorID[collectorID]; !ok {
		return false
	}

	inst := immc.byCollectorID[collectorID]
	immc.byMetricName[inst.metricName] = slices.DeleteFunc(immc.byMetricName[inst.metricName], func(mc *MetricCollector) bool {
		return mc == nil || mc.id == collectorID
	})

	delete(immc.byCollectorID, collectorID)
	return true
}

func (immc *InMemoryMetricsCollector) Query(collectorID MetricCollectorID) ([]*MetricResult, error) {
	immc.lock.Lock()
	defer immc.lock.Unlock()

	if _, ok := immc.byCollectorID[collectorID]; !ok {
		return nil, fmt.Errorf("collector with ID: %s does not exist", collectorID)
	}

	return immc.byCollectorID[collectorID].Get(), nil
}

func (immc *InMemoryMetricsCollector) Update(metricName string, labels map[string]string, value float64, timestamp *time.Time) {
	immc.lock.Lock()
	defer immc.lock.Unlock()

	for _, collector := range immc.byMetricName[metricName] {
		labelValues := make([]string, 0, len(collector.labels))
		for _, label := range collector.labels {
			labelValues = append(labelValues, labels[label])
		}

		collector.Update(labelValues, value, timestamp)
	}
}
