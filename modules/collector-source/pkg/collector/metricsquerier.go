package collector

import (
	"time"

	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
)

const GiB = 1024 * 1024 * 1024
const LocalStorageCostPerGiBHr = 0.04 / 730.0

type collectorMetricsQuerier struct {
	collectorProvider StoreProvider
}

func newCollectorMetricsQuerier(repo *metric.MetricRepository, resoluationConfigs []util.ResolutionConfiguration) *collectorMetricsQuerier {
	return &collectorMetricsQuerier{
		collectorProvider: newRepoStoreProvider(repo, resoluationConfigs),
	}
}

func queryCollector[T any](c *collectorMetricsQuerier, start, end time.Time, id metric.MetricCollectorID, decoder source.ResultDecoder[T]) *source.Future[T] {
	queryResults := source.NewQueryResults(string(id))
	collector := c.collectorProvider.GetStore(start, end)
	if collector != nil {
		results, err := collector.Query(id)
		queryResults.Error = err
		for _, result := range results {
			queryResults.Results = append(queryResults.Results, result.ToQueryResult())
		}
	}
	ch := make(source.QueryResultsChan, 1)
	ch <- queryResults
	f := source.NewFuture[T](decoder, ch)
	return f

}

func queryCollectorGiB[T any](c *collectorMetricsQuerier, start, end time.Time, id metric.MetricCollectorID, decoder source.ResultDecoder[T]) *source.Future[T] {
	queryResults := source.NewQueryResults(string(id))
	collector := c.collectorProvider.GetStore(start, end)
	if collector != nil {
		results, err := collector.Query(id)
		queryResults.Error = err
		for _, result := range results {
			for i := range result.Values {
				result.Values[i].Value /= GiB
			}
			queryResults.Results = append(queryResults.Results, result.ToQueryResult())
		}
	}
	ch := make(source.QueryResultsChan, 1)
	ch <- queryResults
	f := source.NewFuture[T](decoder, ch)
	return f

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
	queryResults := source.NewQueryResults("LocalStorageCost")
	collector := c.collectorProvider.GetStore(start, end)
	if collector != nil {
		minutesResults, err := collector.Query(metric.LocalStorageActiveMinutesID)
		if err != nil {
			queryResults.Error = err
		}
		minutesByNode := map[string]float64{}
		for _, result := range minutesResults {
			node := result.MetricLabels[source.NodeLabel]
			if node == "" || len(result.Values) == 0 {
				continue
			}
			nodeStart := result.Values[0].Timestamp
			nodeEnd := result.Values[len(result.Values)-1].Timestamp
			if nodeStart == nil || nodeEnd == nil {
				continue
			}
			minutesByNode[node] = nodeEnd.Sub(*nodeStart).Minutes()

		}
		bytesResults, err := collector.Query(metric.LocalStorageBytesID)
		if err != nil {
			queryResults.Error = err
		}
		for _, result := range bytesResults {
			instance := result.MetricLabels[source.InstanceLabel]
			if instance == "" || len(result.Values) == 0 {
				continue
			}
			mintues, ok := minutesByNode[instance]
			if !ok {
				continue
			}
			queryResult := result.ToQueryResult()
			bytes := queryResult.Values[0].Value
			GiBs := bytes / GiB
			hours := mintues / 60
			queryResult.Values[0].Value = GiBs * hours * LocalStorageCostPerGiBHr
			queryResults.Results = append(queryResults.Results, queryResult)
		}
	}
	ch := make(source.QueryResultsChan, 1)
	ch <- queryResults
	return source.NewFuture(source.DecodeLocalStorageCostResult, ch)
}

func (c *collectorMetricsQuerier) QueryLocalStorageUsedCost(start, end time.Time) *source.Future[source.LocalStorageUsedCostResult] {
	queryResults := source.NewQueryResults("LocalStorageUsedCost")
	collector := c.collectorProvider.GetStore(start, end)
	if collector != nil {
		minutesResults, err := collector.Query(metric.LocalStorageUsedActiveMinutesID)
		if err != nil {
			queryResults.Error = err
		}
		minutesByNode := map[string]float64{}
		for _, result := range minutesResults {
			node := result.MetricLabels[source.InstanceLabel]
			if node == "" || len(result.Values) == 0 {
				continue
			}
			nodeStart := result.Values[0].Timestamp
			nodeEnd := result.Values[len(result.Values)-1].Timestamp
			if nodeStart == nil || nodeEnd == nil {
				continue
			}
			minutesByNode[node] = nodeEnd.Sub(*nodeStart).Minutes()

		}
		bytesResults, err := collector.Query(metric.LocalStorageUsedAverageID)
		if err != nil {
			queryResults.Error = err
		}
		for _, result := range bytesResults {
			instance := result.MetricLabels[source.InstanceLabel]
			if instance == "" || len(result.Values) == 0 {
				continue
			}
			mintues, ok := minutesByNode[instance]
			if !ok {
				continue
			}
			queryResult := result.ToQueryResult()
			bytes := queryResult.Values[0].Value
			GiBs := bytes / GiB
			hours := mintues / 60
			queryResult.Values[0].Value = GiBs * hours * LocalStorageCostPerGiBHr
			queryResults.Results = append(queryResults.Results, queryResult)
		}
	}
	ch := make(source.QueryResultsChan, 1)
	ch <- queryResults
	return source.NewFuture(source.DecodeLocalStorageUsedCostResult, ch)
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
	queryResults := source.NewQueryResults("NodeRAMSystemPercent")
	collector := c.collectorProvider.GetStore(start, end)
	if collector != nil {
		capacityResult, err := collector.Query(metric.NodeRAMBytesCapacityID)
		if err != nil {
			queryResults.Error = err
		}
		nodeCapacities := map[string]float64{}
		for _, result := range capacityResult {
			node := result.MetricLabels[source.NodeLabel]
			if node == "" || len(result.Values) == 0 {
				continue
			}
			nodeCapacities[node] = result.Values[0].Value
		}

		results, err := collector.Query(metric.NodeRAMSystemUsageAverageID)
		if err != nil {
			queryResults.Error = err
		}
		for _, result := range results {
			instance := result.MetricLabels[source.InstanceLabel]

			capacity, ok := nodeCapacities[instance]
			if !ok || len(result.Values) == 0 {
				continue
			}
			result.Values[0].Value /= capacity
			queryResults.Results = append(queryResults.Results, result.ToQueryResult())
		}
	}
	ch := make(source.QueryResultsChan, 1)
	ch <- queryResults
	f := source.NewFuture(source.DecodeNodeRAMSystemPercentResult, ch)
	return f
}

func (c *collectorMetricsQuerier) QueryNodeRAMUserPercent(start, end time.Time) *source.Future[source.NodeRAMUserPercentResult] {
	queryResults := source.NewQueryResults("NodeRAMUserPercent")
	collector := c.collectorProvider.GetStore(start, end)
	if collector != nil {
		capacityResult, err := collector.Query(metric.NodeRAMBytesCapacityID)
		if err != nil {
			queryResults.Error = err
		}
		nodeCapacities := map[string]float64{}
		for _, result := range capacityResult {
			node := result.MetricLabels[source.NodeLabel]
			if node == "" || len(result.Values) == 0 {
				continue
			}
			nodeCapacities[node] = result.Values[0].Value
		}

		results, err := collector.Query(metric.NodeRAMUserUsageAverageID)
		if err != nil {
			queryResults.Error = err
		}
		for _, result := range results {
			instance := result.MetricLabels[source.InstanceLabel]

			capacity, ok := nodeCapacities[instance]
			if !ok || len(result.Values) == 0 {
				continue
			}
			result.Values[0].Value /= capacity
			queryResults.Results = append(queryResults.Results, result.ToQueryResult())
		}
	}
	ch := make(source.QueryResultsChan, 1)
	ch <- queryResults
	f := source.NewFuture(source.DecodeNodeRAMUserPercentResult, ch)
	return f
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

func (c *collectorMetricsQuerier) QueryRAMLimits(start, end time.Time) *source.Future[source.RAMLimitsResult] {
	return queryCollector(c, start, end, metric.RAMLimitsID, source.DecodeRAMLimitsResult)
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

func (c *collectorMetricsQuerier) QueryCPULimits(start, end time.Time) *source.Future[source.CPULimitsResult] {
	return queryCollector(c, start, end, metric.CPULimitsID, source.DecodeCPULimitsResult)
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
	return queryCollectorGiB(c, start, end, metric.NetZoneGiBID, source.DecodeNetZoneGiBResult)
}

func (c *collectorMetricsQuerier) QueryNetZonePricePerGiB(start, end time.Time) *source.Future[source.NetZonePricePerGiBResult] {
	return queryCollector(c, start, end, metric.NetZonePricePerGiBID, source.DecodeNetZonePricePerGiBResult)
}

func (c *collectorMetricsQuerier) QueryNetRegionGiB(start, end time.Time) *source.Future[source.NetRegionGiBResult] {
	return queryCollectorGiB(c, start, end, metric.NetRegionGiBID, source.DecodeNetRegionGiBResult)
}

func (c *collectorMetricsQuerier) QueryNetRegionPricePerGiB(start, end time.Time) *source.Future[source.NetRegionPricePerGiBResult] {
	return queryCollector(c, start, end, metric.NetRegionPricePerGiBID, source.DecodeNetRegionPricePerGiBResult)
}

func (c *collectorMetricsQuerier) QueryNetInternetGiB(start, end time.Time) *source.Future[source.NetInternetGiBResult] {
	return queryCollectorGiB(c, start, end, metric.NetInternetGiBID, source.DecodeNetInternetGiBResult)
}

func (c *collectorMetricsQuerier) QueryNetInternetPricePerGiB(start, end time.Time) *source.Future[source.NetInternetPricePerGiBResult] {
	return queryCollector(c, start, end, metric.NetInternetPricePerGiBID, source.DecodeNetInternetPricePerGiBResult)
}

func (c *collectorMetricsQuerier) QueryNetInternetServiceGiB(start, end time.Time) *source.Future[source.NetInternetServiceGiBResult] {
	return queryCollectorGiB(c, start, end, metric.NetInternetServiceGiBID, source.DecodeNetInternetServiceGiBResult)
}

func (c *collectorMetricsQuerier) QueryNetTransferBytes(start, end time.Time) *source.Future[source.NetTransferBytesResult] {
	return queryCollector(c, start, end, metric.NetTransferBytesID, source.DecodeNetTransferBytesResult)
}

func (c *collectorMetricsQuerier) QueryNetZoneIngressGiB(start, end time.Time) *source.Future[source.NetZoneIngressGiBResult] {
	return queryCollectorGiB(c, start, end, metric.NetZoneIngressGiBID, source.DecodeNetZoneIngressGiBResult)
}

func (c *collectorMetricsQuerier) QueryNetRegionIngressGiB(start, end time.Time) *source.Future[source.NetRegionIngressGiBResult] {
	return queryCollectorGiB(c, start, end, metric.NetRegionIngressGiBID, source.DecodeNetRegionIngressGiBResult)
}

func (c *collectorMetricsQuerier) QueryNetInternetIngressGiB(start, end time.Time) *source.Future[source.NetInternetIngressGiBResult] {
	return queryCollectorGiB(c, start, end, metric.NetInternetIngressGiBID, source.DecodeNetInternetIngressGiBResult)
}

func (c *collectorMetricsQuerier) QueryNetInternetServiceIngressGiB(start, end time.Time) *source.Future[source.NetInternetServiceIngressGiBResult] {
	return queryCollectorGiB(c, start, end, metric.NetInternetServiceIngressGiBID, source.DecodeNetInternetServiceIngressGiBResult)
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

func (c *collectorMetricsQuerier) QueryResourceQuotaSpecCPURequestAverage(start, end time.Time) *source.Future[source.ResourceQuotaSpecCPURequestAvgResult] {
	return queryCollector(c, start, end, metric.ResourceQuotaSpecCPURequestAverageID, source.DecodeResourceQuotaSpecCPURequestAvgResult)
}

func (c *collectorMetricsQuerier) QueryResourceQuotaSpecCPURequestMax(start, end time.Time) *source.Future[source.ResourceQuotaSpecCPURequestMaxResult] {
	return queryCollector(c, start, end, metric.ResourceQuotaSpecCPURequestMaxID, source.DecodeResourceQuotaSpecCPURequestMaxResult)
}

func (c *collectorMetricsQuerier) QueryResourceQuotaSpecRAMRequestAverage(start, end time.Time) *source.Future[source.ResourceQuotaSpecRAMRequestAvgResult] {
	return queryCollector(c, start, end, metric.ResourceQuotaSpecRAMRequestAverageID, source.DecodeResourceQuotaSpecRAMRequestAvgResult)
}

func (c *collectorMetricsQuerier) QueryResourceQuotaSpecRAMRequestMax(start, end time.Time) *source.Future[source.ResourceQuotaSpecRAMRequestMaxResult] {
	return queryCollector(c, start, end, metric.ResourceQuotaSpecRAMRequestMaxID, source.DecodeResourceQuotaSpecRAMRequestMaxResult)
}

func (c *collectorMetricsQuerier) QueryResourceQuotaSpecCPULimitAverage(start, end time.Time) *source.Future[source.ResourceQuotaSpecCPULimitAvgResult] {
	return queryCollector(c, start, end, metric.ResourceQuotaSpecCPULimitAverageID, source.DecodeResourceQuotaSpecCPULimitAvgResult)
}

func (c *collectorMetricsQuerier) QueryResourceQuotaSpecCPULimitMax(start, end time.Time) *source.Future[source.ResourceQuotaSpecCPULimitMaxResult] {
	return queryCollector(c, start, end, metric.ResourceQuotaSpecCPULimitMaxID, source.DecodeResourceQuotaSpecCPULimitMaxResult)
}

func (c *collectorMetricsQuerier) QueryResourceQuotaSpecRAMLimitAverage(start, end time.Time) *source.Future[source.ResourceQuotaSpecRAMLimitAvgResult] {
	return queryCollector(c, start, end, metric.ResourceQuotaSpecRAMLimitAverageID, source.DecodeResourceQuotaSpecRAMLimitAvgResult)
}

func (c *collectorMetricsQuerier) QueryResourceQuotaSpecRAMLimitMax(start, end time.Time) *source.Future[source.ResourceQuotaSpecRAMLimitMaxResult] {
	return queryCollector(c, start, end, metric.ResourceQuotaSpecRAMLimitMaxID, source.DecodeResourceQuotaSpecRAMLimitMaxResult)
}

func (c *collectorMetricsQuerier) QueryResourceQuotaStatusUsedCPURequestAverage(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedCPURequestAvgResult] {
	return queryCollector(c, start, end, metric.ResourceQuotaStatusUsedCPURequestAverageID, source.DecodeResourceQuotaStatusUsedCPURequestAvgResult)
}

func (c *collectorMetricsQuerier) QueryResourceQuotaStatusUsedCPURequestMax(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedCPURequestMaxResult] {
	return queryCollector(c, start, end, metric.ResourceQuotaStatusUsedCPURequestMaxID, source.DecodeResourceQuotaStatusUsedCPURequestMaxResult)
}

func (c *collectorMetricsQuerier) QueryResourceQuotaStatusUsedRAMRequestAverage(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedRAMRequestAvgResult] {
	return queryCollector(c, start, end, metric.ResourceQuotaStatusUsedRAMRequestAverageID, source.DecodeResourceQuotaStatusUsedRAMRequestAvgResult)
}

func (c *collectorMetricsQuerier) QueryResourceQuotaStatusUsedRAMRequestMax(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedRAMRequestMaxResult] {
	return queryCollector(c, start, end, metric.ResourceQuotaStatusUsedRAMRequestMaxID, source.DecodeResourceQuotaStatusUsedRAMRequestMaxResult)
}

func (c *collectorMetricsQuerier) QueryResourceQuotaStatusUsedCPULimitAverage(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedCPULimitAvgResult] {
	return queryCollector(c, start, end, metric.ResourceQuotaStatusUsedCPULimitAverageID, source.DecodeResourceQuotaStatusUsedCPULimitAvgResult)
}

func (c *collectorMetricsQuerier) QueryResourceQuotaStatusUsedCPULimitMax(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedCPULimitMaxResult] {
	return queryCollector(c, start, end, metric.ResourceQuotaStatusUsedCPULimitMaxID, source.DecodeResourceQuotaStatusUsedCPULimitMaxResult)
}

func (c *collectorMetricsQuerier) QueryResourceQuotaStatusUsedRAMLimitAverage(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedRAMLimitAvgResult] {
	return queryCollector(c, start, end, metric.ResourceQuotaStatusUsedRAMLimitAverageID, source.DecodeResourceQuotaStatusUsedRAMLimitAvgResult)
}

func (c *collectorMetricsQuerier) QueryResourceQuotaStatusUsedRAMLimitMax(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedRAMLimitMaxResult] {
	return queryCollector(c, start, end, metric.ResourceQuotaStatusUsedRAMLimitMaxID, source.DecodeResourceQuotaStatusUsedRAMLimitMaxResult)
}

func (c *collectorMetricsQuerier) QueryDataCoverage(limitDays int) (time.Time, time.Time, error) {
	return c.collectorProvider.GetDailyDataCoverage(limitDays)
}
