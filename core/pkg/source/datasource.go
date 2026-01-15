package source

import (
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/diagnostics"
)

type MetricsQuerier interface {
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

	// Nodes
	QueryNodeActiveMinutes(start, end time.Time) *Future[NodeActiveMinutesResult]
	QueryNodeCPUCoresCapacity(start, end time.Time) *Future[NodeCPUCoresCapacityResult]
	QueryNodeCPUCoresAllocatable(start, end time.Time) *Future[NodeCPUCoresAllocatableResult]
	QueryNodeRAMBytesCapacity(start, end time.Time) *Future[NodeRAMBytesCapacityResult]
	QueryNodeRAMBytesAllocatable(start, end time.Time) *Future[NodeRAMBytesAllocatableResult]
	QueryNodeGPUCount(start, end time.Time) *Future[NodeGPUCountResult]
	QueryNodeCPUModeTotal(start, end time.Time) *Future[NodeCPUModeTotalResult]
	QueryNodeIsSpot(start, end time.Time) *Future[NodeIsSpotResult]
	QueryNodeRAMSystemPercent(start, end time.Time) *Future[NodeRAMSystemPercentResult]
	QueryNodeRAMUserPercent(start, end time.Time) *Future[NodeRAMUserPercentResult]

	// Load Balancers
	QueryLBActiveMinutes(start, end time.Time) *Future[LBActiveMinutesResult]
	QueryLBPricePerHr(start, end time.Time) *Future[LBPricePerHrResult]

	// Cluster Management
	QueryClusterUptime(start, end time.Time) *Future[UptimeResult]
	QueryClusterManagementDuration(start, end time.Time) *Future[ClusterManagementDurationResult]
	QueryClusterManagementPricePerHr(start, end time.Time) *Future[ClusterManagementPricePerHrResult]

	// Pods
	QueryPods(start, end time.Time) *Future[PodsResult]
	QueryPodsUID(start, end time.Time) *Future[PodsResult]

	// RAM
	QueryRAMBytesAllocated(start, end time.Time) *Future[RAMBytesAllocatedResult]
	QueryRAMRequests(start, end time.Time) *Future[RAMRequestsResult]
	QueryRAMLimits(start, end time.Time) *Future[RAMLimitsResult]
	QueryRAMUsageAvg(start, end time.Time) *Future[RAMUsageAvgResult]
	QueryRAMUsageMax(start, end time.Time) *Future[RAMUsageMaxResult]
	QueryNodeRAMPricePerGiBHr(start, end time.Time) *Future[NodeRAMPricePerGiBHrResult]

	// CPU
	QueryCPUCoresAllocated(start, end time.Time) *Future[CPUCoresAllocatedResult]
	QueryCPURequests(start, end time.Time) *Future[CPURequestsResult]
	QueryCPULimits(start, end time.Time) *Future[CPULimitsResult]
	QueryCPUUsageAvg(start, end time.Time) *Future[CPUUsageAvgResult]
	QueryCPUUsageMax(start, end time.Time) *Future[CPUUsageMaxResult]
	QueryNodeCPUPricePerHr(start, end time.Time) *Future[NodeCPUPricePerHrResult]

	// GPU
	QueryGPUsAllocated(start, end time.Time) *Future[GPUsAllocatedResult]
	QueryGPUsRequested(start, end time.Time) *Future[GPUsRequestedResult]
	QueryGPUsUsageAvg(start, end time.Time) *Future[GPUsUsageAvgResult]
	QueryGPUsUsageMax(start, end time.Time) *Future[GPUsUsageMaxResult]
	QueryNodeGPUPricePerHr(start, end time.Time) *Future[NodeGPUPricePerHrResult]
	QueryGPUInfo(start, end time.Time) *Future[GPUInfoResult]
	QueryIsGPUShared(start, end time.Time) *Future[IsGPUSharedResult]

	// PVC
	QueryPodPVCAllocation(start, end time.Time) *Future[PodPVCAllocationResult]
	QueryPVCBytesRequested(start, end time.Time) *Future[PVCBytesRequestedResult]
	QueryPVCInfo(start, end time.Time) *Future[PVCInfoResult]

	// PV
	QueryPVBytes(start, end time.Time) *Future[PVBytesResult]
	QueryPVPricePerGiBHour(start, end time.Time) *Future[PVPricePerGiBHourResult]
	QueryPVInfo(start, end time.Time) *Future[PVInfoResult]

	// Namespace
	QueryNamespaceUptime(start, end time.Time) *Future[UptimeResult]

	// Network Egress
	QueryNetZoneGiB(start, end time.Time) *Future[NetZoneGiBResult]
	QueryNetZonePricePerGiB(start, end time.Time) *Future[NetZonePricePerGiBResult]
	QueryNetRegionGiB(start, end time.Time) *Future[NetRegionGiBResult]
	QueryNetRegionPricePerGiB(start, end time.Time) *Future[NetRegionPricePerGiBResult]
	QueryNetInternetGiB(start, end time.Time) *Future[NetInternetGiBResult]
	QueryNetInternetPricePerGiB(start, end time.Time) *Future[NetInternetPricePerGiBResult]
	QueryNetInternetServiceGiB(start, end time.Time) *Future[NetInternetServiceGiBResult]
	QueryNetNatGatewayPricePerGiB(start, end time.Time) *Future[NetNatGatewayPricePerGiBResult]
	QueryNetNatGatewayGiB(start, end time.Time) *Future[NetNatGatewayGiBResult]
	QueryNetTransferBytes(start, end time.Time) *Future[NetTransferBytesResult]

	// Network Ingress
	QueryNetZoneIngressGiB(start, end time.Time) *Future[NetZoneIngressGiBResult]
	QueryNetRegionIngressGiB(start, end time.Time) *Future[NetRegionIngressGiBResult]
	QueryNetInternetIngressGiB(start, end time.Time) *Future[NetInternetIngressGiBResult]
	QueryNetInternetServiceIngressGiB(start, end time.Time) *Future[NetInternetServiceIngressGiBResult]
	QueryNetNatGatewayIngressPricePerGiB(start, end time.Time) *Future[NetNatGatewayPricePerGiBResult]
	QueryNetNatGatewayIngressGiB(start, end time.Time) *Future[NetNatGatewayIngressGiBResult]
	QueryNetReceiveBytes(start, end time.Time) *Future[NetReceiveBytesResult]

	// Annotations
	QueryNamespaceAnnotations(start, end time.Time) *Future[NamespaceAnnotationsResult]
	QueryPodAnnotations(start, end time.Time) *Future[PodAnnotationsResult]

	// Labels
	QueryNodeLabels(start, end time.Time) *Future[NodeLabelsResult]
	QueryNamespaceLabels(start, end time.Time) *Future[NamespaceLabelsResult]
	QueryPodLabels(start, end time.Time) *Future[PodLabelsResult]
	QueryServiceLabels(start, end time.Time) *Future[ServiceLabelsResult]
	QueryDeploymentLabels(start, end time.Time) *Future[DeploymentLabelsResult]
	QueryStatefulSetLabels(start, end time.Time) *Future[StatefulSetLabelsResult]
	QueryDaemonSetLabels(start, end time.Time) *Future[DaemonSetLabelsResult]
	QueryJobLabels(start, end time.Time) *Future[JobLabelsResult]

	// ReplicaSet -> Controller mapping
	QueryPodsWithReplicaSetOwner(start, end time.Time) *Future[PodsWithReplicaSetOwnerResult]
	QueryReplicaSetsWithoutOwners(start, end time.Time) *Future[ReplicaSetsWithoutOwnersResult]
	QueryReplicaSetsWithRollout(start, end time.Time) *Future[ReplicaSetsWithRolloutResult]

	// ResourceQuotas
	QueryResourceQuotaUptime(start, end time.Time) *Future[UptimeResult]
	QueryResourceQuotaSpecCPURequestAverage(start, end time.Time) *Future[ResourceQuotaSpecCPURequestAvgResult]
	QueryResourceQuotaSpecCPURequestMax(start, end time.Time) *Future[ResourceQuotaSpecCPURequestMaxResult]
	QueryResourceQuotaSpecRAMRequestAverage(start, end time.Time) *Future[ResourceQuotaSpecRAMRequestAvgResult]
	QueryResourceQuotaSpecRAMRequestMax(start, end time.Time) *Future[ResourceQuotaSpecRAMRequestMaxResult]
	QueryResourceQuotaSpecCPULimitAverage(start, end time.Time) *Future[ResourceQuotaSpecCPULimitAvgResult]
	QueryResourceQuotaSpecCPULimitMax(start, end time.Time) *Future[ResourceQuotaSpecCPULimitMaxResult]
	QueryResourceQuotaSpecRAMLimitAverage(start, end time.Time) *Future[ResourceQuotaSpecRAMLimitAvgResult]
	QueryResourceQuotaSpecRAMLimitMax(start, end time.Time) *Future[ResourceQuotaSpecRAMLimitMaxResult]
	QueryResourceQuotaStatusUsedCPURequestAverage(start, end time.Time) *Future[ResourceQuotaStatusUsedCPURequestAvgResult]
	QueryResourceQuotaStatusUsedCPURequestMax(start, end time.Time) *Future[ResourceQuotaStatusUsedCPURequestMaxResult]
	QueryResourceQuotaStatusUsedRAMRequestAverage(start, end time.Time) *Future[ResourceQuotaStatusUsedRAMRequestAvgResult]
	QueryResourceQuotaStatusUsedRAMRequestMax(start, end time.Time) *Future[ResourceQuotaStatusUsedRAMRequestMaxResult]
	QueryResourceQuotaStatusUsedCPULimitAverage(start, end time.Time) *Future[ResourceQuotaStatusUsedCPULimitAvgResult]
	QueryResourceQuotaStatusUsedCPULimitMax(start, end time.Time) *Future[ResourceQuotaStatusUsedCPULimitMaxResult]
	QueryResourceQuotaStatusUsedRAMLimitAverage(start, end time.Time) *Future[ResourceQuotaStatusUsedRAMLimitAvgResult]
	QueryResourceQuotaStatusUsedRAMLimitMax(start, end time.Time) *Future[ResourceQuotaStatusUsedRAMLimitMaxResult]

	// Data Coverage Query
	QueryDataCoverage(limitDays int) (time.Time, time.Time, error)
}

type OpenCostDataSource interface {
	// RegisterEndPoints registers any custom endpoints that can be used for diagnostics or debug purposes.
	RegisterEndPoints(router *httprouter.Router)

	// RegisterDiagnostics registers any custom data source diagnostics with the `DiagnosticService` that can
	// be used to report externally.
	RegisterDiagnostics(diagService diagnostics.DiagnosticService)

	// Metrics returns a MetricsQuerier that can be used to query historical metrics data from the data source.
	Metrics() MetricsQuerier

	// ClusterMap returns a mapping of cluster identifier to ClusterInfo for all known clusters (local only for
	// single cluster deployments).
	ClusterMap() clusters.ClusterMap

	// ClusterInfo returns the ClusterInfoProvider for the local cluster.
	ClusterInfo() clusters.ClusterInfoProvider

	BatchDuration() time.Duration
	Resolution() time.Duration
}
