package source

import (
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/clusters"
)

type ClusterMetricsQuerier interface {
	// Cluster Disks
	QueryPVActiveMinutes(start, end time.Time) *Future[PVActiveMinutesResult]
	QueryPVUsedAverage(start, end time.Time) *Future[PVUsedAvgResult]
	QueryPVUsedMax(start, end time.Time) *Future[PVUsedMaxResult]

	// Local Cluster Disks
	QueryLocalStorageActiveMinutes(start, end time.Time) *Future[LocalStorageActiveMinutesResult]
	QueryLocalStorageCost(start, end time.Time) *Future[LocalStorageCostResult]
	QueryLocalStorageUsedCost(start, end time.Time) *Future[LocalStorageUsedCostResult]
	QueryLocalStorageUsedAvg(start, end time.Time) *Future[LocalStorageUsedAvgResult]
	QueryLocalStorageUsedMax(start, end time.Time) *Future[LocalStorageUsedMaxResult]
	QueryLocalStorageBytes(start, end time.Time) *Future[LocalStorageBytesResult]
	QueryLocalStorageBytesByProvider(provider string, start, end time.Time) *Future[LocalStorageBytesByProviderResult]
	QueryLocalStorageUsedByProvider(provider string, start, end time.Time) *Future[LocalStorageUsedByProviderResult]

	// Nodes
	QueryNodeActiveMinutes(start, end time.Time) *Future[NodeActiveMinutesResult]
	QueryNodeCPUCoresCapacity(start, end time.Time) *Future[NodeCPUCoresCapacityResult]
	QueryNodeCPUCoresAllocatable(start, end time.Time) *Future[NodeCPUCoresAllocatableResult]
	QueryNodeRAMBytesCapacity(start, end time.Time) *Future[NodeRAMBytesCapacityResult]
	QueryNodeRAMBytesAllocatable(start, end time.Time) *Future[NodeRAMBytesAllocatableResult]
	QueryNodeGPUCount(start, end time.Time) *Future[NodeGPUCountResult]
	QueryNodeCPUModeTotal(start, end time.Time) *Future[NodeCPUModeTotalResult]
	QueryNodeIsSpot(start, end time.Time) *Future[NodeIsSpotResult]
	QueryNodeCPUModePercent(start, end time.Time) *Future[NodeCPUModePercentResult]
	QueryNodeRAMSystemPercent(start, end time.Time) *Future[NodeRAMSystemPercentResult]
	QueryNodeRAMUserPercent(start, end time.Time) *Future[NodeRAMUserPercentResult]

	// Load Balancers
	QueryLBActiveMinutes(start, end time.Time) *Future[LBActiveMinutesResult]
	QueryLBPricePerHr(start, end time.Time) *Future[LBPricePerHrResult]

	// Cluster Management
	QueryClusterManagementDuration(start, end time.Time) *Future[ClusterManagementDurationResult]
	QueryClusterManagementPricePerHr(start, end time.Time) *Future[ClusterManagementPricePerHrResult]

	// Cluster Costs
	QueryDataCount(start, end time.Time) *Future[DataCountResult]
	QueryTotalGPU(start, end time.Time) *Future[TotalGPUResult]
	QueryTotalCPU(start, end time.Time) *Future[TotalCPUResult]
	QueryTotalRAM(start, end time.Time) *Future[TotalRAMResult]
	QueryTotalStorage(start, end time.Time) *Future[TotalStorageResult]

	// Cluster Costs
	QueryClusterCores(start, end time.Time, step time.Duration) *Future[ClusterCoresResult]
	QueryClusterRAM(start, end time.Time, step time.Duration) *Future[ClusterRAMResult]
	QueryClusterStorage(start, end time.Time, step time.Duration) *Future[ClusterStorageResult]
	QueryClusterStorageByProvider(provider string, start, end time.Time, step time.Duration) *Future[ClusterStorageResult]
	QueryClusterTotal(start, end time.Time, step time.Duration) *Future[ClusterTotalResult]
	QueryClusterTotalByProvider(provider string, start, end time.Time, step time.Duration) *Future[ClusterTotalResult]
	QueryClusterNodes(start, end time.Time, step time.Duration) *Future[ClusterNodesResult]
	QueryClusterNodesByProvider(provider string, start, end time.Time, step time.Duration) *Future[ClusterNodesResult]
}

type AllocationMetricsQuerier interface {
	QueryPods(start, end time.Time) *Future[PodsResult]
	QueryPodsUID(start, end time.Time) *Future[PodsResult]

	QueryRAMBytesAllocated(start, end time.Time) *Future[RAMBytesAllocatedResult]
	QueryRAMRequests(start, end time.Time) *Future[RAMRequestsResult]
	QueryRAMUsageAvg(start, end time.Time) *Future[RAMUsageAvgResult]
	QueryRAMUsageMax(start, end time.Time) *Future[RAMUsageMaxResult]
	QueryNodeRAMPricePerGiBHr(start, end time.Time) *Future[NodeRAMPricePerGiBHrResult]

	QueryCPUCoresAllocated(start, end time.Time) *Future[CPUCoresAllocatedResult]
	QueryCPURequests(start, end time.Time) *Future[CPURequestsResult]
	QueryCPUUsageAvg(start, end time.Time) *Future[CPUUsageAvgResult]
	QueryCPUUsageMax(start, end time.Time) *Future[CPUUsageMaxResult]
	QueryNodeCPUPricePerHr(start, end time.Time) *Future[NodeCPUPricePerHrResult]

	QueryGPUsAllocated(start, end time.Time) *Future[GPUsAllocatedResult]
	QueryGPUsRequested(start, end time.Time) *Future[GPUsRequestedResult]
	QueryGPUsUsageAvg(start, end time.Time) *Future[GPUsUsageAvgResult]
	QueryGPUsUsageMax(start, end time.Time) *Future[GPUsUsageMaxResult]
	QueryNodeGPUPricePerHr(start, end time.Time) *Future[NodeGPUPricePerHrResult]
	QueryGPUInfo(start, end time.Time) *Future[GPUInfoResult]
	QueryIsGPUShared(start, end time.Time) *Future[IsGPUSharedResult]

	QueryPodPVCAllocation(start, end time.Time) *Future[PodPVCAllocationResult]
	QueryPVCBytesRequested(start, end time.Time) *Future[PVCBytesRequestedResult]
	QueryPVCInfo(start, end time.Time) *Future[PVCInfoResult]

	QueryPVBytes(start, end time.Time) *Future[PVBytesResult]
	QueryPVPricePerGiBHour(start, end time.Time) *Future[PVPricePerGiBHourResult]
	QueryPVInfo(start, end time.Time) *Future[PVInfoResult]

	QueryNetZoneGiB(start, end time.Time) *Future[NetZoneGiBResult]
	QueryNetZonePricePerGiB(start, end time.Time) *Future[NetZonePricePerGiBResult]
	QueryNetRegionGiB(start, end time.Time) *Future[NetRegionGiBResult]
	QueryNetRegionPricePerGiB(start, end time.Time) *Future[NetRegionPricePerGiBResult]
	QueryNetInternetGiB(start, end time.Time) *Future[NetInternetGiBResult]
	QueryNetInternetPricePerGiB(start, end time.Time) *Future[NetInternetPricePerGiBResult]
	QueryNetReceiveBytes(start, end time.Time) *Future[NetReceiveBytesResult]
	QueryNetTransferBytes(start, end time.Time) *Future[NetTransferBytesResult]

	QueryNamespaceAnnotations(start, end time.Time) *Future[NamespaceAnnotationsResult]
	QueryPodAnnotations(start, end time.Time) *Future[PodAnnotationsResult]

	QueryNodeLabels(start, end time.Time) *Future[NodeLabelsResult]
	QueryNamespaceLabels(start, end time.Time) *Future[NamespaceLabelsResult]
	QueryPodLabels(start, end time.Time) *Future[PodLabelsResult]
	QueryServiceLabels(start, end time.Time) *Future[ServiceLabelsResult]
	QueryDeploymentLabels(start, end time.Time) *Future[DeploymentLabelsResult]
	QueryStatefulSetLabels(start, end time.Time) *Future[StatefulSetLabelsResult]
	QueryDaemonSetLabels(start, end time.Time) *Future[DaemonSetLabelsResult]
	QueryJobLabels(start, end time.Time) *Future[JobLabelsResult]

	QueryPodsWithReplicaSetOwner(start, end time.Time) *Future[PodsWithReplicaSetOwnerResult]
	QueryReplicaSetsWithoutOwners(start, end time.Time) *Future[ReplicaSetsWithoutOwnersResult]
	QueryReplicaSetsWithRollout(start, end time.Time) *Future[ReplicaSetsWithRolloutResult]

	QueryDataCoverage(limitDays int) (time.Time, time.Time, error)
}

type OpenCostDataSource interface {
	ClusterMetricsQuerier
	AllocationMetricsQuerier

	NewClusterMap(clusterInfoProvider clusters.ClusterInfoProvider) clusters.ClusterMap

	RegisterEndPoints(router *httprouter.Router)

	BatchDuration() time.Duration
	Resolution() time.Duration
	MetaData() map[string]string
}
