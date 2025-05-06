package collector

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
)

type collectorMetricsQuerier struct {
	collectorProvider StoreProvider
}

func newCollectorMetricsQuerier(repo *metric.MetricRepository, resoluationConfigs []util.ResolutionConfiguration) *collectorMetricsQuerier {
	return &collectorMetricsQuerier{
		collectorProvider: newRepoStoreProvider(repo, resoluationConfigs),
	}
}

func queryCollector[T any](c *collectorMetricsQuerier, start, end time.Time, id metric.MetricCollectorID, decoder source.ResultDecoder[T]) *source.Future[T] {
	collector := c.collectorProvider.GetStore(start, end)
	queryResults := source.NewQueryResults(string(id))
	if collector == nil {
		queryResults.Error = fmt.Errorf("collector returned nil")
	} else {
		results, err := collector.Query(id)
		queryResults.Error = err
		for _, result := range results {
			queryResults.Results = append(queryResults.Results, result.ToQueryResult())
		}
	}

	ch := make(source.QueryResultsChan)
	go func() {
		ch <- queryResults
	}()
	return source.NewFuture[T](decoder, ch)
}

func (c *collectorMetricsQuerier) QueryPVActiveMinutes(start, end time.Time) *source.Future[source.PVActiveMinutesResult] {
	return queryCollector(c, start, end, metric.PVActiveMinutesID, source.DecodePVActiveMinutesResult)
}

func (c *collectorMetricsQuerier) QueryPVUsedAverage(start, end time.Time) *source.Future[source.PVUsedAvgResult] {
	return queryCollector(c, start, end, metric.PVUsedAverageID, source.DecodePVUsedAvgResult)
}

func (c *collectorMetricsQuerier) QueryPVUsedMax(start, end time.Time) *source.Future[source.PVUsedMaxResult] {
	return queryCollector(c, start, end, metric.PVUsedMaxID, source.DecodePVUsedMaxResult)
}

func (c *collectorMetricsQuerier) QueryLocalStorageActiveMinutes(start, end time.Time) *source.Future[source.LocalStorageActiveMinutesResult] {
	return queryCollector(c, start, end, metric.LocalStorageActiveMinutesID, source.DecodeLocalStorageActiveMinutesResult)
}

func (c *collectorMetricsQuerier) QueryLocalStorageCost(start, end time.Time) *source.Future[source.LocalStorageCostResult] {
	return queryCollector(c, start, end, metric.LocalStorageCostID, source.DecodeLocalStorageCostResult)

}

func (c *collectorMetricsQuerier) QueryLocalStorageUsedCost(start, end time.Time) *source.Future[source.LocalStorageUsedCostResult] {
	return queryCollector(c, start, end, metric.LocalStorageUsedCostID, source.DecodeLocalStorageUsedCostResult)

}

func (c *collectorMetricsQuerier) QueryLocalStorageUsedAvg(start, end time.Time) *source.Future[source.LocalStorageUsedAvgResult] {
	return queryCollector(c, start, end, metric.LocalStorageUsedAverageID, source.DecodeLocalStorageUsedAvgResult)

}

func (c *collectorMetricsQuerier) QueryLocalStorageUsedMax(start, end time.Time) *source.Future[source.LocalStorageUsedMaxResult] {
	return queryCollector(c, start, end, metric.LocalStorageUsedMaxID, source.DecodeLocalStorageUsedMaxResult)

}

func (c *collectorMetricsQuerier) QueryLocalStorageBytes(start, end time.Time) *source.Future[source.LocalStorageBytesResult] {
	return queryCollector(c, start, end, metric.LocalStorageBytesID, source.DecodeLocalStorageBytesResult)

}

func (c *collectorMetricsQuerier) QueryNodeActiveMinutes(start, end time.Time) *source.Future[source.NodeActiveMinutesResult] {
	return queryCollector(c, start, end, metric.NodeActiveMinutesID, source.DecodeNodeActiveMinutesResult)

}

func (c *collectorMetricsQuerier) QueryNodeCPUCoresCapacity(start, end time.Time) *source.Future[source.NodeCPUCoresCapacityResult] {
	return queryCollector(c, start, end, metric.NodeCPUCoresCapacityID, source.DecodeNodeCPUCoresCapacityResult)

}

func (c *collectorMetricsQuerier) QueryNodeCPUCoresAllocatable(start, end time.Time) *source.Future[source.NodeCPUCoresAllocatableResult] {
	return queryCollector(c, start, end, metric.NodeCPUCoresAllocatableID, source.DecodeNodeCPUCoresAllocatableResult)
}

func (c *collectorMetricsQuerier) QueryNodeRAMBytesCapacity(start, end time.Time) *source.Future[source.NodeRAMBytesCapacityResult] {
	return queryCollector(c, start, end, metric.NodeRAMBytesCapacityID, source.DecodeNodeRAMBytesCapacityResult)
}

func (c *collectorMetricsQuerier) QueryNodeRAMBytesAllocatable(start, end time.Time) *source.Future[source.NodeRAMBytesAllocatableResult] {
	return queryCollector(c, start, end, metric.NodeRAMBytesAllocatableID, source.DecodeNodeRAMBytesAllocatableResult)
}

func (c *collectorMetricsQuerier) QueryNodeGPUCount(start, end time.Time) *source.Future[source.NodeGPUCountResult] {
	return queryCollector(c, start, end, metric.NodeGPUCountID, source.DecodeNodeGPUCountResult)
}

func (c *collectorMetricsQuerier) QueryNodeCPUModeTotal(start, end time.Time) *source.Future[source.NodeCPUModeTotalResult] {
	return queryCollector(c, start, end, metric.NodeCPUModeTotalID, source.DecodeNodeCPUModeTotalResult)
}

func (c *collectorMetricsQuerier) QueryNodeIsSpot(start, end time.Time) *source.Future[source.NodeIsSpotResult] {
	return queryCollector(c, start, end, metric.NodeIsSpotID, source.DecodeNodeIsSpotResult)
}

func (c *collectorMetricsQuerier) QueryNodeRAMSystemPercent(start, end time.Time) *source.Future[source.NodeRAMSystemPercentResult] {
	return queryCollector(c, start, end, metric.NodeRAMSystemUsageAverageID, source.DecodeNodeRAMSystemPercentResult)
}

func (c *collectorMetricsQuerier) QueryNodeRAMUserPercent(start, end time.Time) *source.Future[source.NodeRAMUserPercentResult] {
	return queryCollector(c, start, end, metric.NodeRAMUserUsageAverageID, source.DecodeNodeRAMUserPercentResult)
}

func (c *collectorMetricsQuerier) QueryLBActiveMinutes(start, end time.Time) *source.Future[source.LBActiveMinutesResult] {
	return queryCollector(c, start, end, metric.LBActiveMinutesID, source.DecodeLBActiveMinutesResult)
}

func (c *collectorMetricsQuerier) QueryLBPricePerHr(start, end time.Time) *source.Future[source.LBPricePerHrResult] {
	return queryCollector(c, start, end, metric.LBPricePerHourID, source.DecodeLBPricePerHrResult)
}

func (c *collectorMetricsQuerier) QueryClusterManagementDuration(start, end time.Time) *source.Future[source.ClusterManagementDurationResult] {
	return queryCollector(c, start, end, metric.ClusterManagementDurationID, source.DecodeClusterManagementDurationResult)
}

func (c *collectorMetricsQuerier) QueryClusterManagementPricePerHr(start, end time.Time) *source.Future[source.ClusterManagementPricePerHrResult] {
	return queryCollector(c, start, end, metric.ClusterManagementPricePerHourID, source.DecodeClusterManagementPricePerHrResult)
}

func (c *collectorMetricsQuerier) QueryPods(start, end time.Time) *source.Future[source.PodsResult] {
	return queryCollector(c, start, end, metric.PodActiveMinutesID, source.DecodePodsResult)

}

func (c *collectorMetricsQuerier) QueryPodsUID(start, end time.Time) *source.Future[source.PodsResult] {
	return queryCollector(c, start, end, metric.PodActiveMinutesID, source.DecodePodsResult)
}

func (c *collectorMetricsQuerier) QueryRAMBytesAllocated(start, end time.Time) *source.Future[source.RAMBytesAllocatedResult] {
	return queryCollector(c, start, end, metric.RAMBytesAllocatedID, source.DecodeRAMBytesAllocatedResult)
}

func (c *collectorMetricsQuerier) QueryRAMRequests(start, end time.Time) *source.Future[source.RAMRequestsResult] {
	return queryCollector(c, start, end, metric.RAMRequestsID, source.DecodeRAMRequestsResult)
}

func (c *collectorMetricsQuerier) QueryRAMUsageAvg(start, end time.Time) *source.Future[source.RAMUsageAvgResult] {
	return queryCollector(c, start, end, metric.RAMUsageAverageID, source.DecodeRAMUsageAvgResult)
}

func (c *collectorMetricsQuerier) QueryRAMUsageMax(start, end time.Time) *source.Future[source.RAMUsageMaxResult] {
	return queryCollector(c, start, end, metric.RAMUsageMaxID, source.DecodeRAMUsageMaxResult)
}

func (c *collectorMetricsQuerier) QueryNodeRAMPricePerGiBHr(start, end time.Time) *source.Future[source.NodeRAMPricePerGiBHrResult] {
	return queryCollector(c, start, end, metric.NodeRAMPricePerGiBHourID, source.DecodeNodeRAMPricePerGiBHrResult)
}

func (c *collectorMetricsQuerier) QueryCPUCoresAllocated(start, end time.Time) *source.Future[source.CPUCoresAllocatedResult] {
	return queryCollector(c, start, end, metric.CPUCoresAllocatedID, source.DecodeCPUCoresAllocatedResult)
}

func (c *collectorMetricsQuerier) QueryCPURequests(start, end time.Time) *source.Future[source.CPURequestsResult] {
	return queryCollector(c, start, end, metric.CPURequestsID, source.DecodeCPURequestsResult)
}

func (c *collectorMetricsQuerier) QueryCPUUsageAvg(start, end time.Time) *source.Future[source.CPUUsageAvgResult] {
	return queryCollector(c, start, end, metric.CPUUsageAverageID, source.DecodeCPUUsageAvgResult)
}

func (c *collectorMetricsQuerier) QueryCPUUsageMax(start, end time.Time) *source.Future[source.CPUUsageMaxResult] {
	return queryCollector(c, start, end, metric.CPUUsageMaxID, source.DecodeCPUUsageMaxResult)
}

func (c *collectorMetricsQuerier) QueryNodeCPUPricePerHr(start, end time.Time) *source.Future[source.NodeCPUPricePerHrResult] {
	return queryCollector(c, start, end, metric.NodeCPUPricePerHourID, source.DecodeNodeCPUPricePerHrResult)
}

func (c *collectorMetricsQuerier) QueryGPUsAllocated(start, end time.Time) *source.Future[source.GPUsAllocatedResult] {
	return queryCollector(c, start, end, metric.GPUsAllocatedID, source.DecodeGPUsAllocatedResult)
}

func (c *collectorMetricsQuerier) QueryGPUsRequested(start, end time.Time) *source.Future[source.GPUsRequestedResult] {
	return queryCollector(c, start, end, metric.GPUsRequestedID, source.DecodeGPUsRequestedResult)
}

func (c *collectorMetricsQuerier) QueryGPUsUsageAvg(start, end time.Time) *source.Future[source.GPUsUsageAvgResult] {
	return queryCollector(c, start, end, metric.GPUsUsageAverageID, source.DecodeGPUsUsageAvgResult)
}

func (c *collectorMetricsQuerier) QueryGPUsUsageMax(start, end time.Time) *source.Future[source.GPUsUsageMaxResult] {
	return queryCollector(c, start, end, metric.GPUsUsageMaxID, source.DecodeGPUsUsageMaxResult)
}

func (c *collectorMetricsQuerier) QueryNodeGPUPricePerHr(start, end time.Time) *source.Future[source.NodeGPUPricePerHrResult] {
	return queryCollector(c, start, end, metric.NodeGPUPricePerHourID, source.DecodeNodeGPUPricePerHrResult)
}

func (c *collectorMetricsQuerier) QueryGPUInfo(start, end time.Time) *source.Future[source.GPUInfoResult] {
	return queryCollector(c, start, end, metric.GPUInfoID, source.DecodeGPUInfoResult)
}

func (c *collectorMetricsQuerier) QueryIsGPUShared(start, end time.Time) *source.Future[source.IsGPUSharedResult] {
	return queryCollector(c, start, end, metric.IsGPUSharedID, source.DecodeIsGPUSharedResult)
}

func (c *collectorMetricsQuerier) QueryPodPVCAllocation(start, end time.Time) *source.Future[source.PodPVCAllocationResult] {
	return queryCollector(c, start, end, metric.PodPVCAllocationID, source.DecodePodPVCAllocationResult)
}

func (c *collectorMetricsQuerier) QueryPVCBytesRequested(start, end time.Time) *source.Future[source.PVCBytesRequestedResult] {
	return queryCollector(c, start, end, metric.PVCBytesRequestedID, source.DecodePVCBytesRequestedResult)
}

func (c *collectorMetricsQuerier) QueryPVCInfo(start, end time.Time) *source.Future[source.PVCInfoResult] {
	return queryCollector(c, start, end, metric.PVCInfoID, source.DecodePVCInfoResult)
}

func (c *collectorMetricsQuerier) QueryPVBytes(start, end time.Time) *source.Future[source.PVBytesResult] {
	return queryCollector(c, start, end, metric.PVBytesID, source.DecodePVBytesResult)
}

func (c *collectorMetricsQuerier) QueryPVPricePerGiBHour(start, end time.Time) *source.Future[source.PVPricePerGiBHourResult] {
	return queryCollector(c, start, end, metric.PVPricePerGiBHourID, source.DecodePVPricePerGiBHourResult)
}

func (c *collectorMetricsQuerier) QueryPVInfo(start, end time.Time) *source.Future[source.PVInfoResult] {
	return queryCollector(c, start, end, metric.PVInfoID, source.DecodePVInfoResult)
}

func (c *collectorMetricsQuerier) QueryNetZoneGiB(start, end time.Time) *source.Future[source.NetZoneGiBResult] {
	return queryCollector(c, start, end, metric.NetZoneGiBID, source.DecodeNetZoneGiBResult)
}

func (c *collectorMetricsQuerier) QueryNetZonePricePerGiB(start, end time.Time) *source.Future[source.NetZonePricePerGiBResult] {
	return queryCollector(c, start, end, metric.NetZonePricePerGiBID, source.DecodeNetZonePricePerGiBResult)
}

func (c *collectorMetricsQuerier) QueryNetRegionGiB(start, end time.Time) *source.Future[source.NetRegionGiBResult] {
	return queryCollector(c, start, end, metric.NetRegionGiBID, source.DecodeNetRegionGiBResult)
}

func (c *collectorMetricsQuerier) QueryNetRegionPricePerGiB(start, end time.Time) *source.Future[source.NetRegionPricePerGiBResult] {
	return queryCollector(c, start, end, metric.NetRegionPricePerGiBID, source.DecodeNetRegionPricePerGiBResult)
}

func (c *collectorMetricsQuerier) QueryNetInternetGiB(start, end time.Time) *source.Future[source.NetInternetGiBResult] {
	return queryCollector(c, start, end, metric.NetInternetGiBID, source.DecodeNetInternetGiBResult)
}

func (c *collectorMetricsQuerier) QueryNetInternetPricePerGiB(start, end time.Time) *source.Future[source.NetInternetPricePerGiBResult] {
	return queryCollector(c, start, end, metric.NetInternetPricePerGiBID, source.DecodeNetInternetPricePerGiBResult)
}

// TODO figure this out
func (c *collectorMetricsQuerier) QueryNetInternetServiceGiB(start, end time.Time) *source.Future[source.NetInternetServiceGiBResult] {
	return queryCollector(c, start, end, metric.NetInternetServiceGiBID, source.DecodeNetInternetServiceGiBResult)
}

func (c *collectorMetricsQuerier) QueryNetTransferBytes(start, end time.Time) *source.Future[source.NetTransferBytesResult] {
	return queryCollector(c, start, end, metric.NetTransferBytesID, source.DecodeNetTransferBytesResult)
}

// TODO figure this out
func (c *collectorMetricsQuerier) QueryNetZoneIngressGiB(start, end time.Time) *source.Future[source.NetZoneIngressGiBResult] {
	return queryCollector(c, start, end, metric.NetZoneIngressGiBID, source.DecodeNetZoneIngressGiBResult)
}

// TODO figure this out
func (c *collectorMetricsQuerier) QueryNetRegionIngressGiB(start, end time.Time) *source.Future[source.NetRegionIngressGiBResult] {
	return queryCollector(c, start, end, metric.NetRegionIngressGiBID, source.DecodeNetRegionIngressGiBResult)
}

// TODO figure this out
func (c *collectorMetricsQuerier) QueryNetInternetIngressGiB(start, end time.Time) *source.Future[source.NetInternetIngressGiBResult] {
	return queryCollector(c, start, end, metric.NetInternetIngressGiBID, source.DecodeNetInternetIngressGiBResult)
}

// TODO figure this out
func (c *collectorMetricsQuerier) QueryNetInternetServiceIngressGiB(start, end time.Time) *source.Future[source.NetInternetServiceIngressGiBResult] {
	return queryCollector(c, start, end, metric.NetInternetServiceIngressGiBID, source.DecodeNetInternetServiceIngressGiBResult)
}

func (c *collectorMetricsQuerier) QueryNetReceiveBytes(start, end time.Time) *source.Future[source.NetReceiveBytesResult] {
	return queryCollector(c, start, end, metric.NetReceiveBytesID, source.DecodeNetReceiveBytesResult)
}

func (c *collectorMetricsQuerier) QueryNamespaceAnnotations(start, end time.Time) *source.Future[source.NamespaceAnnotationsResult] {
	return queryCollector(c, start, end, metric.NamespaceAnnotationsID, source.DecodeNamespaceAnnotationsResult)
}

func (c *collectorMetricsQuerier) QueryPodAnnotations(start, end time.Time) *source.Future[source.PodAnnotationsResult] {
	return queryCollector(c, start, end, metric.PodAnnotationsID, source.DecodePodAnnotationsResult)
}

func (c *collectorMetricsQuerier) QueryNodeLabels(start, end time.Time) *source.Future[source.NodeLabelsResult] {
	return queryCollector(c, start, end, metric.NodeLabelsID, source.DecodeNodeLabelsResult)
}

func (c *collectorMetricsQuerier) QueryNamespaceLabels(start, end time.Time) *source.Future[source.NamespaceLabelsResult] {
	return queryCollector(c, start, end, metric.NamespaceLabelsID, source.DecodeNamespaceLabelsResult)
}

func (c *collectorMetricsQuerier) QueryPodLabels(start, end time.Time) *source.Future[source.PodLabelsResult] {
	return queryCollector(c, start, end, metric.PodLabelsID, source.DecodePodLabelsResult)
}

func (c *collectorMetricsQuerier) QueryServiceLabels(start, end time.Time) *source.Future[source.ServiceLabelsResult] {
	return queryCollector(c, start, end, metric.ServiceLabelsID, source.DecodeServiceLabelsResult)
}

func (c *collectorMetricsQuerier) QueryDeploymentLabels(start, end time.Time) *source.Future[source.DeploymentLabelsResult] {
	return queryCollector(c, start, end, metric.DeploymentLabelsID, source.DecodeDeploymentLabelsResult)
}

func (c *collectorMetricsQuerier) QueryStatefulSetLabels(start, end time.Time) *source.Future[source.StatefulSetLabelsResult] {
	return queryCollector(c, start, end, metric.StatefulSetLabelsID, source.DecodeStatefulSetLabelsResult)
}

func (c *collectorMetricsQuerier) QueryDaemonSetLabels(start, end time.Time) *source.Future[source.DaemonSetLabelsResult] {
	return queryCollector(c, start, end, metric.DaemonSetLabelsID, source.DecodeDaemonSetLabelsResult)
}

func (c *collectorMetricsQuerier) QueryJobLabels(start, end time.Time) *source.Future[source.JobLabelsResult] {
	return queryCollector(c, start, end, metric.JobLabelsID, source.DecodeJobLabelsResult)
}

func (c *collectorMetricsQuerier) QueryPodsWithReplicaSetOwner(start, end time.Time) *source.Future[source.PodsWithReplicaSetOwnerResult] {
	return queryCollector(c, start, end, metric.PodsWithReplicaSetOwnerID, source.DecodePodsWithReplicaSetOwnerResult)
}

func (c *collectorMetricsQuerier) QueryReplicaSetsWithoutOwners(start, end time.Time) *source.Future[source.ReplicaSetsWithoutOwnersResult] {
	return queryCollector(c, start, end, metric.ReplicaSetsWithoutOwnersID, source.DecodeReplicaSetsWithoutOwnersResult)
}

func (c *collectorMetricsQuerier) QueryReplicaSetsWithRollout(start, end time.Time) *source.Future[source.ReplicaSetsWithRolloutResult] {
	return queryCollector(c, start, end, metric.ReplicaSetsWithRolloutID, source.DecodeReplicaSetsWithRolloutResult)
}

func (c *collectorMetricsQuerier) QueryDataCoverage(limitDays int) (time.Time, time.Time, error) {
	// TODO immplement me
	panic("implement me")
}
