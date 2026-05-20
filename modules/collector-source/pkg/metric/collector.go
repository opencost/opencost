package metric

import (
	"maps"
	"time"

	"github.com/opencost/opencost/modules/collector-source/pkg/metric/aggregator"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
)

// MetricCollectorID is a unique identifier for a specific metric collector instance. We
// use this identifier to register and unregister metric instances from the metrics metric
// instead of the metric name and aggregation type to allow selectable cardinality (via Labels)
// across multiple instances of the same aggregation type and metric name.
type MetricCollectorID string

const (
	PVPricePerGiBHourID                        MetricCollectorID = "PVPricePerGiBHour"
	PVUsedAverageID                            MetricCollectorID = "PVUsedAverage"
	PVUsedMaxID                                MetricCollectorID = "PVUsedMax"
	PVCInfoID                                  MetricCollectorID = "PVCInfo"
	PVActiveMinutesID                          MetricCollectorID = "PVActiveMinutes"
	LocalStorageUsedActiveMinutesID            MetricCollectorID = "LocalStorageUsedCost"
	LocalStorageUsedAverageID                  MetricCollectorID = "LocalStorageUsedAverage"
	LocalStorageUsedMaxID                      MetricCollectorID = "LocalStorageUsedMax"
	LocalStorageBytesID                        MetricCollectorID = "LocalStorageBytesID"
	LocalStorageActiveMinutesID                MetricCollectorID = "LocalStorageActiveMinutes"
	NodeCPUCoresCapacityID                     MetricCollectorID = "NodeCPUCoresCapacity"
	NodeCPUCoresAllocatableID                  MetricCollectorID = "NodeCPUCoresAllocatable"
	NodeRAMBytesCapacityID                     MetricCollectorID = "NodeRAMBytesCapacity"
	NodeRAMBytesAllocatableID                  MetricCollectorID = "NodeRAMBytesAllocatable"
	NodeGPUCountID                             MetricCollectorID = "NodeGPUCount"
	NodeLabelsID                               MetricCollectorID = "NodeLabels"
	NodeActiveMinutesID                        MetricCollectorID = "NodeActiveMinutes"
	NodeCPUModeTotalID                         MetricCollectorID = "NodeCPUModeTotal"
	NodeRAMSystemUsageAverageID                MetricCollectorID = "NodeRAMSystemUsageAverage"
	NodeRAMUserUsageAverageID                  MetricCollectorID = "NodeRAMUserUsageAverage"
	LBPricePerHourID                           MetricCollectorID = "LBPricePerHour"
	LBActiveMinutesID                          MetricCollectorID = "LBActiveMinutes"
	ClusterUptimeID                            MetricCollectorID = "ClusterUptime"
	ClusterManagementDurationID                MetricCollectorID = "ClusterManagementDuration"
	ClusterManagementPricePerHourID            MetricCollectorID = "ClusterManagementPricePerHour"
	PodActiveMinutesID                         MetricCollectorID = "PodActiveMinutes"
	RAMBytesAllocatedID                        MetricCollectorID = "RAMBytesAllocated"
	RAMRequestsID                              MetricCollectorID = "RAMRequests"
	RAMLimitsID                                MetricCollectorID = "RAMLimits"
	RAMUsageAverageID                          MetricCollectorID = "RAMUsageAverage"
	RAMUsageMaxID                              MetricCollectorID = "RAMUsageMax"
	CPUCoresAllocatedID                        MetricCollectorID = "CPUCoresAllocated"
	CPURequestsID                              MetricCollectorID = "CPURequestsID"
	CPULimitsID                                MetricCollectorID = "CPULimitsID"
	CPUUsageAverageID                          MetricCollectorID = "CPUUsageAverage"
	CPUUsageMaxID                              MetricCollectorID = "CPUUsageMax"
	GPUsRequestedID                            MetricCollectorID = "GPUsRequested"
	GPUsUsageAverageID                         MetricCollectorID = "GPUsUsageAverage"
	GPUsUsageMaxID                             MetricCollectorID = "GPUsUsageMax"
	GPUsAllocatedID                            MetricCollectorID = "GPUsAllocated"
	IsGPUSharedID                              MetricCollectorID = "IsGPUShared"
	GPUInfoID                                  MetricCollectorID = "GPUInfo"
	NodeCPUPricePerHourID                      MetricCollectorID = "NodeCPUPricePerHour"
	NodeRAMPricePerGiBHourID                   MetricCollectorID = "NodeRAMPricePerGiBHour"
	NodeGPUPricePerHourID                      MetricCollectorID = "NodeGPUPricePerHour"
	NodeIsSpotID                               MetricCollectorID = "NodeIsSpot"
	PodPVCAllocationID                         MetricCollectorID = "PodPVCAllocation"
	PVCBytesRequestedID                        MetricCollectorID = "PVCBytesRequested"
	PVBytesID                                  MetricCollectorID = "PVBytesID"
	PVInfoID                                   MetricCollectorID = "PVInfo"
	NetZoneGiBID                               MetricCollectorID = "NetZoneGiB"
	NetZonePricePerGiBID                       MetricCollectorID = "NetZonePricePerGiB"
	NetRegionGiBID                             MetricCollectorID = "NetRegionGiB"
	NetRegionPricePerGiBID                     MetricCollectorID = "NetRegionPricePerGiB"
	NetInternetGiBID                           MetricCollectorID = "NetInternetGiB"
	NetInternetPricePerGiBID                   MetricCollectorID = "NetInternetPricePerGiB"
	NetInternetServiceGiBID                    MetricCollectorID = "NetInternetServiceGiB"
	NetNatGatewayPricePerGiBID                 MetricCollectorID = "NetNatGatewayPricePerGiB"
	NetNatGatewayIngressPricePerGiBID          MetricCollectorID = "NetNatGatewayIngressPricePerGiB"
	NetNatGatewayGiBID                         MetricCollectorID = "NetNatGatewayGiB"
	NetTransferBytesID                         MetricCollectorID = "NetTransferBytes"
	NetZoneIngressGiBID                        MetricCollectorID = "NetZoneIngressGiB"
	NetRegionIngressGiBID                      MetricCollectorID = "NetRegionIngressGiB"
	NetInternetIngressGiBID                    MetricCollectorID = "NetInternetIngressGiB"
	NetInternetServiceIngressGiBID             MetricCollectorID = "NetInternetServiceIngressGiB"
	NetNatGatewayIngressGiBID                  MetricCollectorID = "NetNatGatewayIngressGiB"
	NetReceiveBytesID                          MetricCollectorID = "NetReceiveBytes"
	NamespaceUptimeID                          MetricCollectorID = "NamespaceUptime"
	NamespaceLabelsID                          MetricCollectorID = "NamespaceLabels"
	NamespaceAnnotationsID                     MetricCollectorID = "NamespaceAnnotations"
	PodLabelsID                                MetricCollectorID = "PodLabels"
	PodAnnotationsID                           MetricCollectorID = "PodAnnotations"
	ServiceLabelsID                            MetricCollectorID = "ServiceLabels"
	DeploymentLabelsID                         MetricCollectorID = "DeploymentLabels"
	StatefulSetLabelsID                        MetricCollectorID = "StatefulSetLabels"
	DaemonSetLabelsID                          MetricCollectorID = "DaemonSetLabels"
	JobLabelsID                                MetricCollectorID = "JobLabels"
	PodsWithReplicaSetOwnerID                  MetricCollectorID = "PodsWithReplicaSetOwner"
	ReplicaSetsWithoutOwnersID                 MetricCollectorID = "ReplicaSetsWithoutOwners"
	ReplicaSetsWithRolloutID                   MetricCollectorID = "ReplicaSetsWithRollout"
	ResourceQuotaUptimeID                      MetricCollectorID = "ResourceQuotaUptime"
	ResourceQuotaSpecCPURequestAverageID       MetricCollectorID = "ResourceQuotaSpecCPURequestAverage"
	ResourceQuotaSpecCPURequestMaxID           MetricCollectorID = "ResourceQuotaSpecCPURequestMax"
	ResourceQuotaSpecRAMRequestAverageID       MetricCollectorID = "ResourceQuotaSpecRAMRequestAverage"
	ResourceQuotaSpecRAMRequestMaxID           MetricCollectorID = "ResourceQuotaSpecRAMRequestMax"
	ResourceQuotaSpecCPULimitAverageID         MetricCollectorID = "ResourceQuotaSpecCPULimitAverage"
	ResourceQuotaSpecCPULimitMaxID             MetricCollectorID = "ResourceQuotaSpecCPULimitMax"
	ResourceQuotaSpecRAMLimitAverageID         MetricCollectorID = "ResourceQuotaSpecRAMLimitAverage"
	ResourceQuotaSpecRAMLimitMaxID             MetricCollectorID = "ResourceQuotaSpecRAMLimitMax"
	ResourceQuotaStatusUsedCPURequestAverageID MetricCollectorID = "ResourceQuotaStatusUsedCPURequestAverage"
	ResourceQuotaStatusUsedCPURequestMaxID     MetricCollectorID = "ResourceQuotaStatusUsedCPURequestMax"
	ResourceQuotaStatusUsedRAMRequestAverageID MetricCollectorID = "ResourceQuotaStatusUsedRAMRequestAverage"
	ResourceQuotaStatusUsedRAMRequestMaxID     MetricCollectorID = "ResourceQuotaStatusUsedRAMRequestMax"
	ResourceQuotaStatusUsedCPULimitAverageID   MetricCollectorID = "ResourceQuotaStatusUsedCPULimitAverage"
	ResourceQuotaStatusUsedCPULimitMaxID       MetricCollectorID = "ResourceQuotaStatusUsedCPULimitMax"
	ResourceQuotaStatusUsedRAMLimitAverageID   MetricCollectorID = "ResourceQuotaStatusUsedRAMLimitAverage"
	ResourceQuotaStatusUsedRAMLimitMaxID       MetricCollectorID = "ResourceQuotaStatusUsedRAMLimitMax"
)

// MetricCollector is a data structure that represents a specific MetricCollector metric instance that contains its own breakdown
// of stored metrics by a specific label set.
type MetricCollector struct {
	id                MetricCollectorID // ie: RAMUsageAverage
	metricName        string            // ie: container_memory_working_set_bytes
	labels            []string
	aggregatorFactory aggregator.MetricAggregatorFactory
	metrics           map[uint64]aggregator.MetricAggregator // map[Hash(labelValues)] = aggregator
	filter            func(map[string]string) bool
}

// NewMetricCollector creates a new MetricCollector instance with a unique identifier. The metric name is the specific
// name of the collected metric that will be used to query the
func NewMetricCollector(id MetricCollectorID, metricName string, labels []string, aggregatorFactory aggregator.MetricAggregatorFactory, fn func(map[string]string) bool) *MetricCollector {
	return &MetricCollector{
		id:                id,
		metricName:        metricName,
		labels:            labels,
		aggregatorFactory: aggregatorFactory,
		metrics:           make(map[uint64]aggregator.MetricAggregator),
		filter:            fn,
	}
}

func (mi *MetricCollector) Update(labels map[string]string, value float64, timestamp time.Time, additionalInfo map[string]string) {
	if mi.filter != nil && !mi.filter(labels) {
		return
	}

	labelValues := make([]string, len(mi.labels))
	for i, key := range mi.labels {
		labelValues[i] = labels[key]
	}
	key := util.Hash(labelValues)
	if mi.metrics[key] == nil {
		mi.metrics[key] = mi.aggregatorFactory(labelValues)
	}

	mi.metrics[key].Update(value, timestamp, additionalInfo)
}

func (mi *MetricCollector) Get() []*aggregator.MetricResult {
	results := make([]*aggregator.MetricResult, 0, len(mi.metrics))
	for _, metric := range mi.metrics {
		labels := util.ToMap(mi.labels, metric.LabelValues())
		maps.Copy(labels, metric.AdditionInfo())
		mr := &aggregator.MetricResult{
			MetricLabels: labels,
			Values:       metric.Value(),
		}

		results = append(results, mr)
	}

	return results
}

func (mi *MetricCollector) Labels() []string {
	return mi.labels
}
