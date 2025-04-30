package collector

import (
	"time"

	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
)

type CollectorMetricsQuerier struct {
	collectorProvider StoreProvider
}

func queryCollector[T any](c *CollectorMetricsQuerier, start, end time.Time, id metric.MetricCollectorID, decoder source.ResultDecoder[T]) *source.Future[T] {
	collector := c.collectorProvider.GetStore(start, end)
	results, err := collector.Query(id)
	queryResults := source.NewQueryResults(string(id))
	queryResults.Error = err
	for _, result := range results {
		queryResults.Results = append(queryResults.Results, result.ToQueryResult())
	}

	ch := make(source.QueryResultsChan)
	go func() {
		ch <- queryResults
	}()
	return source.NewFuture[T](decoder, ch)
}

func (c *CollectorMetricsQuerier) QueryPVActiveMinutes(start, end time.Time) *source.Future[source.PVActiveMinutesResult] {
	return queryCollector(c, start, end, metric.PVActiveMinutesID, source.DecodePVActiveMinutesResult)
}

func (c *CollectorMetricsQuerier) QueryPVUsedAverage(start, end time.Time) *source.Future[source.PVUsedAvgResult] {
	return queryCollector(c, start, end, metric.PVUsedAverageID, source.DecodePVUsedAvgResult)
}

func (c *CollectorMetricsQuerier) QueryPVUsedMax(start, end time.Time) *source.Future[source.PVUsedMaxResult] {
	return queryCollector(c, start, end, metric.PVUsedMaxID, source.DecodePVUsedMaxResult)
}

func (c *CollectorMetricsQuerier) QueryLocalStorageActiveMinutes(start, end time.Time) *source.Future[source.LocalStorageActiveMinutesResult] {
	return queryCollector(c, start, end, metric.LocalStorageActiveMinutesID, source.DecodeLocalStorageActiveMinutesResult)
}

func (c *CollectorMetricsQuerier) QueryLocalStorageCost(start, end time.Time) *source.Future[source.LocalStorageCostResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryLocalStorageUsedCost(start, end time.Time) *source.Future[source.LocalStorageUsedCostResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryLocalStorageUsedAvg(start, end time.Time) *source.Future[source.LocalStorageUsedAvgResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryLocalStorageUsedMax(start, end time.Time) *source.Future[source.LocalStorageUsedMaxResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryLocalStorageBytes(start, end time.Time) *source.Future[source.LocalStorageBytesResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryNodeActiveMinutes(start, end time.Time) *source.Future[source.NodeActiveMinutesResult] {
	collector := c.collectorProvider.GetStore(start, end)
	results, err := collector.Query(metric.NodeActiveMinutesID)
	queryResults := source.NewQueryResults(string(metric.NodeActiveMinutesID))
	queryResults.Error = err
	for _, result := range results {
		queryResults.Results = append(queryResults.Results, result.ToQueryResult())
	}

	ch := make(source.QueryResultsChan)
	go func() {
		ch <- queryResults
	}()
	return source.NewFuture[source.NodeActiveMinutesResult](source.DecodeNodeActiveMinutesResult, ch)
}

func (c *CollectorMetricsQuerier) QueryNodeCPUCoresCapacity(start, end time.Time) *source.Future[source.NodeCPUCoresCapacityResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryNodeCPUCoresAllocatable(start, end time.Time) *source.Future[source.NodeCPUCoresAllocatableResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryNodeRAMBytesCapacity(start, end time.Time) *source.Future[source.NodeRAMBytesCapacityResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryNodeRAMBytesAllocatable(start, end time.Time) *source.Future[source.NodeRAMBytesAllocatableResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryNodeGPUCount(start, end time.Time) *source.Future[source.NodeGPUCountResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryNodeCPUModeTotal(start, end time.Time) *source.Future[source.NodeCPUModeTotalResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryNodeIsSpot(start, end time.Time) *source.Future[source.NodeIsSpotResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryNodeRAMSystemPercent(start, end time.Time) *source.Future[source.NodeRAMSystemPercentResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryNodeRAMUserPercent(start, end time.Time) *source.Future[source.NodeRAMUserPercentResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryLBActiveMinutes(start, end time.Time) *source.Future[source.LBActiveMinutesResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryLBPricePerHr(start, end time.Time) *source.Future[source.LBPricePerHrResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryClusterManagementDuration(start, end time.Time) *source.Future[source.ClusterManagementDurationResult] {
	collector := c.collectorProvider.GetStore(start, end)
	results, err := collector.Query(metric.ClusterManagementDurationID)
	queryResults := source.NewQueryResults(string(metric.ClusterManagementDurationID))
	queryResults.Error = err
	for _, result := range results {
		queryResults.Results = append(queryResults.Results, result.ToQueryResult())
	}

	ch := make(source.QueryResultsChan)
	go func() {
		ch <- queryResults
	}()
	return source.NewFuture[source.ClusterManagementDurationResult](source.DecodeClusterManagementDurationResult, ch)
}

func (c *CollectorMetricsQuerier) QueryClusterManagementPricePerHr(start, end time.Time) *source.Future[source.ClusterManagementPricePerHrResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryPods(start, end time.Time) *source.Future[source.PodsResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryPodsUID(start, end time.Time) *source.Future[source.PodsResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryRAMBytesAllocated(start, end time.Time) *source.Future[source.RAMBytesAllocatedResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryRAMRequests(start, end time.Time) *source.Future[source.RAMRequestsResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryRAMUsageAvg(start, end time.Time) *source.Future[source.RAMUsageAvgResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryRAMUsageMax(start, end time.Time) *source.Future[source.RAMUsageMaxResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryNodeRAMPricePerGiBHr(start, end time.Time) *source.Future[source.NodeRAMPricePerGiBHrResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryCPUCoresAllocated(start, end time.Time) *source.Future[source.CPUCoresAllocatedResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryCPURequests(start, end time.Time) *source.Future[source.CPURequestsResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryCPUUsageAvg(start, end time.Time) *source.Future[source.CPUUsageAvgResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryCPUUsageMax(start, end time.Time) *source.Future[source.CPUUsageMaxResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryNodeCPUPricePerHr(start, end time.Time) *source.Future[source.NodeCPUPricePerHrResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryGPUsAllocated(start, end time.Time) *source.Future[source.GPUsAllocatedResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryGPUsRequested(start, end time.Time) *source.Future[source.GPUsRequestedResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryGPUsUsageAvg(start, end time.Time) *source.Future[source.GPUsUsageAvgResult] {
	return queryCollector(c, start, end, metric.GPUsUsageAverageID, source.DecodeGPUsUsageAvgResult)
}

func (c *CollectorMetricsQuerier) QueryGPUsUsageMax(start, end time.Time) *source.Future[source.GPUsUsageMaxResult] {
	return queryCollector(c, start, end, metric.GPUsUsageMaxID, source.DecodeGPUsUsageMaxResult)
}

func (c *CollectorMetricsQuerier) QueryNodeGPUPricePerHr(start, end time.Time) *source.Future[source.NodeGPUPricePerHrResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryGPUInfo(start, end time.Time) *source.Future[source.GPUInfoResult] {
	return queryCollector(c, start, end, metric.GPUInfoID, source.DecodeGPUInfoResult)
}

func (c *CollectorMetricsQuerier) QueryIsGPUShared(start, end time.Time) *source.Future[source.IsGPUSharedResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryPodPVCAllocation(start, end time.Time) *source.Future[source.PodPVCAllocationResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryPVCBytesRequested(start, end time.Time) *source.Future[source.PVCBytesRequestedResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryPVCInfo(start, end time.Time) *source.Future[source.PVCInfoResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryPVBytes(start, end time.Time) *source.Future[source.PVBytesResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryPVPricePerGiBHour(start, end time.Time) *source.Future[source.PVPricePerGiBHourResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryPVInfo(start, end time.Time) *source.Future[source.PVInfoResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryNetZoneGiB(start, end time.Time) *source.Future[source.NetZoneGiBResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryNetZonePricePerGiB(start, end time.Time) *source.Future[source.NetZonePricePerGiBResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryNetRegionGiB(start, end time.Time) *source.Future[source.NetRegionGiBResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryNetRegionPricePerGiB(start, end time.Time) *source.Future[source.NetRegionPricePerGiBResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryNetInternetGiB(start, end time.Time) *source.Future[source.NetInternetGiBResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryNetInternetPricePerGiB(start, end time.Time) *source.Future[source.NetInternetPricePerGiBResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryNetInternetServiceGiB(start, end time.Time) *source.Future[source.NetInternetServiceGiBResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryNetTransferBytes(start, end time.Time) *source.Future[source.NetTransferBytesResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryNetZoneIngressGiB(start, end time.Time) *source.Future[source.NetZoneIngressGiBResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryNetRegionIngressGiB(start, end time.Time) *source.Future[source.NetRegionIngressGiBResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryNetInternetIngressGiB(start, end time.Time) *source.Future[source.NetInternetIngressGiBResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryNetInternetServiceIngressGiB(start, end time.Time) *source.Future[source.NetInternetServiceIngressGiBResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryNetReceiveBytes(start, end time.Time) *source.Future[source.NetReceiveBytesResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryNamespaceAnnotations(start, end time.Time) *source.Future[source.NamespaceAnnotationsResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryPodAnnotations(start, end time.Time) *source.Future[source.PodAnnotationsResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryNodeLabels(start, end time.Time) *source.Future[source.NodeLabelsResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryNamespaceLabels(start, end time.Time) *source.Future[source.NamespaceLabelsResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryPodLabels(start, end time.Time) *source.Future[source.PodLabelsResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryServiceLabels(start, end time.Time) *source.Future[source.ServiceLabelsResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryDeploymentLabels(start, end time.Time) *source.Future[source.DeploymentLabelsResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryStatefulSetLabels(start, end time.Time) *source.Future[source.StatefulSetLabelsResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryDaemonSetLabels(start, end time.Time) *source.Future[source.DaemonSetLabelsResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryJobLabels(start, end time.Time) *source.Future[source.JobLabelsResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryPodsWithReplicaSetOwner(start, end time.Time) *source.Future[source.PodsWithReplicaSetOwnerResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryReplicaSetsWithoutOwners(start, end time.Time) *source.Future[source.ReplicaSetsWithoutOwnersResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryReplicaSetsWithRollout(start, end time.Time) *source.Future[source.ReplicaSetsWithRolloutResult] {
	//TODO implement me
	panic("implement me")
}

func (c *CollectorMetricsQuerier) QueryDataCoverage(limitDays int) (time.Time, time.Time, error) {
	//TODO implement me
	panic("implement me")
}
