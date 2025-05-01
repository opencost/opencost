package collector

import (
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
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryLocalStorageUsedCost(start, end time.Time) *source.Future[source.LocalStorageUsedCostResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryLocalStorageUsedAvg(start, end time.Time) *source.Future[source.LocalStorageUsedAvgResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryLocalStorageUsedMax(start, end time.Time) *source.Future[source.LocalStorageUsedMaxResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryLocalStorageBytes(start, end time.Time) *source.Future[source.LocalStorageBytesResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryNodeActiveMinutes(start, end time.Time) *source.Future[source.NodeActiveMinutesResult] {
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

func (c *collectorMetricsQuerier) QueryNodeCPUCoresCapacity(start, end time.Time) *source.Future[source.NodeCPUCoresCapacityResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryNodeCPUCoresAllocatable(start, end time.Time) *source.Future[source.NodeCPUCoresAllocatableResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryNodeRAMBytesCapacity(start, end time.Time) *source.Future[source.NodeRAMBytesCapacityResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryNodeRAMBytesAllocatable(start, end time.Time) *source.Future[source.NodeRAMBytesAllocatableResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryNodeGPUCount(start, end time.Time) *source.Future[source.NodeGPUCountResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryNodeCPUModeTotal(start, end time.Time) *source.Future[source.NodeCPUModeTotalResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryNodeIsSpot(start, end time.Time) *source.Future[source.NodeIsSpotResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryNodeRAMSystemPercent(start, end time.Time) *source.Future[source.NodeRAMSystemPercentResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryNodeRAMUserPercent(start, end time.Time) *source.Future[source.NodeRAMUserPercentResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryLBActiveMinutes(start, end time.Time) *source.Future[source.LBActiveMinutesResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryLBPricePerHr(start, end time.Time) *source.Future[source.LBPricePerHrResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryClusterManagementDuration(start, end time.Time) *source.Future[source.ClusterManagementDurationResult] {
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

func (c *collectorMetricsQuerier) QueryClusterManagementPricePerHr(start, end time.Time) *source.Future[source.ClusterManagementPricePerHrResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryPods(start, end time.Time) *source.Future[source.PodsResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryPodsUID(start, end time.Time) *source.Future[source.PodsResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryRAMBytesAllocated(start, end time.Time) *source.Future[source.RAMBytesAllocatedResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryRAMRequests(start, end time.Time) *source.Future[source.RAMRequestsResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryRAMUsageAvg(start, end time.Time) *source.Future[source.RAMUsageAvgResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryRAMUsageMax(start, end time.Time) *source.Future[source.RAMUsageMaxResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryNodeRAMPricePerGiBHr(start, end time.Time) *source.Future[source.NodeRAMPricePerGiBHrResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryCPUCoresAllocated(start, end time.Time) *source.Future[source.CPUCoresAllocatedResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryCPURequests(start, end time.Time) *source.Future[source.CPURequestsResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryCPUUsageAvg(start, end time.Time) *source.Future[source.CPUUsageAvgResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryCPUUsageMax(start, end time.Time) *source.Future[source.CPUUsageMaxResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryNodeCPUPricePerHr(start, end time.Time) *source.Future[source.NodeCPUPricePerHrResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryGPUsAllocated(start, end time.Time) *source.Future[source.GPUsAllocatedResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryGPUsRequested(start, end time.Time) *source.Future[source.GPUsRequestedResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryGPUsUsageAvg(start, end time.Time) *source.Future[source.GPUsUsageAvgResult] {
	return queryCollector(c, start, end, metric.GPUsUsageAverageID, source.DecodeGPUsUsageAvgResult)
}

func (c *collectorMetricsQuerier) QueryGPUsUsageMax(start, end time.Time) *source.Future[source.GPUsUsageMaxResult] {
	return queryCollector(c, start, end, metric.GPUsUsageMaxID, source.DecodeGPUsUsageMaxResult)
}

func (c *collectorMetricsQuerier) QueryNodeGPUPricePerHr(start, end time.Time) *source.Future[source.NodeGPUPricePerHrResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryGPUInfo(start, end time.Time) *source.Future[source.GPUInfoResult] {
	return queryCollector(c, start, end, metric.GPUInfoID, source.DecodeGPUInfoResult)
}

func (c *collectorMetricsQuerier) QueryIsGPUShared(start, end time.Time) *source.Future[source.IsGPUSharedResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryPodPVCAllocation(start, end time.Time) *source.Future[source.PodPVCAllocationResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryPVCBytesRequested(start, end time.Time) *source.Future[source.PVCBytesRequestedResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryPVCInfo(start, end time.Time) *source.Future[source.PVCInfoResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryPVBytes(start, end time.Time) *source.Future[source.PVBytesResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryPVPricePerGiBHour(start, end time.Time) *source.Future[source.PVPricePerGiBHourResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryPVInfo(start, end time.Time) *source.Future[source.PVInfoResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryNetZoneGiB(start, end time.Time) *source.Future[source.NetZoneGiBResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryNetZonePricePerGiB(start, end time.Time) *source.Future[source.NetZonePricePerGiBResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryNetRegionGiB(start, end time.Time) *source.Future[source.NetRegionGiBResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryNetRegionPricePerGiB(start, end time.Time) *source.Future[source.NetRegionPricePerGiBResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryNetInternetGiB(start, end time.Time) *source.Future[source.NetInternetGiBResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryNetInternetPricePerGiB(start, end time.Time) *source.Future[source.NetInternetPricePerGiBResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryNetInternetServiceGiB(start, end time.Time) *source.Future[source.NetInternetServiceGiBResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryNetTransferBytes(start, end time.Time) *source.Future[source.NetTransferBytesResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryNetZoneIngressGiB(start, end time.Time) *source.Future[source.NetZoneIngressGiBResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryNetRegionIngressGiB(start, end time.Time) *source.Future[source.NetRegionIngressGiBResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryNetInternetIngressGiB(start, end time.Time) *source.Future[source.NetInternetIngressGiBResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryNetInternetServiceIngressGiB(start, end time.Time) *source.Future[source.NetInternetServiceIngressGiBResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryNetReceiveBytes(start, end time.Time) *source.Future[source.NetReceiveBytesResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryNamespaceAnnotations(start, end time.Time) *source.Future[source.NamespaceAnnotationsResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryPodAnnotations(start, end time.Time) *source.Future[source.PodAnnotationsResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryNodeLabels(start, end time.Time) *source.Future[source.NodeLabelsResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryNamespaceLabels(start, end time.Time) *source.Future[source.NamespaceLabelsResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryPodLabels(start, end time.Time) *source.Future[source.PodLabelsResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryServiceLabels(start, end time.Time) *source.Future[source.ServiceLabelsResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryDeploymentLabels(start, end time.Time) *source.Future[source.DeploymentLabelsResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryStatefulSetLabels(start, end time.Time) *source.Future[source.StatefulSetLabelsResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryDaemonSetLabels(start, end time.Time) *source.Future[source.DaemonSetLabelsResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryJobLabels(start, end time.Time) *source.Future[source.JobLabelsResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryPodsWithReplicaSetOwner(start, end time.Time) *source.Future[source.PodsWithReplicaSetOwnerResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryReplicaSetsWithoutOwners(start, end time.Time) *source.Future[source.ReplicaSetsWithoutOwnersResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryReplicaSetsWithRollout(start, end time.Time) *source.Future[source.ReplicaSetsWithRolloutResult] {
	//TODO implement me
	panic("implement me")
}

func (c *collectorMetricsQuerier) QueryDataCoverage(limitDays int) (time.Time, time.Time, error) {
	//TODO implement me
	panic("implement me")
}
