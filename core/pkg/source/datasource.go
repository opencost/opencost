package source

import (
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/diagnostics"
)

type MetricsQuerier interface {
	// Cluster Disks

	// Local Cluster Disks
	QueryLocalStorageActiveMinutes(start, end time.Time) *Future[LocalStorageActiveMinutesResult]
	QueryLocalStorageCost(start, end time.Time) *Future[LocalStorageCostResult]
	QueryLocalStorageUsedCost(start, end time.Time) *Future[LocalStorageUsedCostResult]
	QueryLocalStorageUsedAvg(start, end time.Time) *Future[LocalStorageUsedAvgResult]
	QueryLocalStorageUsedMax(start, end time.Time) *Future[LocalStorageUsedMaxResult]
	QueryLocalStorageBytes(start, end time.Time) *Future[LocalStorageBytesResult]

	// Nodes
	QueryNodeInfo(start, end time.Time) *Future[NodeInfoResult]
	QueryNodeUptime(start, end time.Time) *Future[UptimeResult]
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
	QueryNodeResourceCapacities(start, end time.Time) *Future[ResourceResult]
	QueryNodeResourcesAllocatable(start, end time.Time) *Future[ResourceResult]

	// Load Balancers
	QueryLBActiveMinutes(start, end time.Time) *Future[LBActiveMinutesResult]
	QueryLBPricePerHr(start, end time.Time) *Future[LBPricePerHrResult]

	// Cluster Management
	QueryClusterInfo(start, end time.Time) *Future[ClusterInfoResult]
	QueryClusterUptime(start, end time.Time) *Future[UptimeResult]
	QueryClusterManagementDuration(start, end time.Time) *Future[ClusterManagementDurationResult]
	QueryClusterManagementPricePerHr(start, end time.Time) *Future[ClusterManagementPricePerHrResult]

	// Pods
	QueryPods(start, end time.Time) *Future[PodsResult]
	QueryPodsUID(start, end time.Time) *Future[PodsResult]
	QueryPodInfo(start, end time.Time) *Future[PodInfoResult]
	QueryPodUptime(start, end time.Time) *Future[UptimeResult]
	QueryPodOwners(start, end time.Time) *Future[OwnerResult]
	QueryPodPVCVolumes(start, end time.Time) *Future[PodPVCVolumeResult]

	// Container
	QueryContainerUptime(start, end time.Time) *Future[ContainerUptimeResult]
	QueryContainerResourceRequests(start, end time.Time) *Future[ContainerResourceResult]
	QueryContainerResourceLimits(start, end time.Time) *Future[ContainerResourceResult]

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

	// Device
	QueryDCGMDeviceInfo(start, end time.Time) *Future[DCGMDeviceInfoResult]
	QueryDCGMDeviceUptime(start, end time.Time) *Future[DCGMDeviceUptimeResult]
	QueryDCGMContainerUsageAvg(start, end time.Time) *Future[DCGMDeviceContainerUsageResult]
	QueryDCGMContainerUsageMax(start, end time.Time) *Future[DCGMDeviceContainerUsageResult]

	// PVC
	QueryPodPVCAllocation(start, end time.Time) *Future[PodPVCAllocationResult]
	QueryPVCBytesRequested(start, end time.Time) *Future[PVCBytesRequestedResult]
	QueryPVCInfo(start, end time.Time) *Future[PVCInfoResult]
	QueryPVCUptime(start, end time.Time) *Future[UptimeResult]

	// PV
	QueryPVBytes(start, end time.Time) *Future[PVBytesResult]
	QueryPVPricePerGiBHour(start, end time.Time) *Future[PVPricePerGiBHourResult]
	QueryPVInfo(start, end time.Time) *Future[PVInfoResult]
	QueryPVUptime(start, end time.Time) *Future[UptimeResult]
	QueryPVActiveMinutes(start, end time.Time) *Future[PVActiveMinutesResult]
	QueryPVUsedAverage(start, end time.Time) *Future[PVUsedAvgResult]
	QueryPVUsedMax(start, end time.Time) *Future[PVUsedMaxResult]

	// Deployment
	QueryDeploymentInfo(start, end time.Time) *Future[DeploymentInfoResult]
	QueryDeploymentUptime(start, end time.Time) *Future[UptimeResult]
	QueryDeploymentLabels(start, end time.Time) *Future[LabelsResult]
	QueryDeploymentAnnotations(start, end time.Time) *Future[AnnotationsResult]
	QueryDeploymentMatchLabels(start, end time.Time) *Future[DeploymentLabelsResult]

	// StatefulSet
	QueryStatefulSetInfo(start, end time.Time) *Future[StatefulSetInfoResult]
	QueryStatefulSetUptime(start, end time.Time) *Future[UptimeResult]
	QueryStatefulSetLabels(start, end time.Time) *Future[LabelsResult]
	QueryStatefulSetAnnotations(start, end time.Time) *Future[AnnotationsResult]
	QueryStatefulSetMatchLabels(start, end time.Time) *Future[StatefulSetLabelsResult]

	// DaemonSet
	QueryDaemonSetInfo(start, end time.Time) *Future[DaemonSetInfoResult]
	QueryDaemonSetUptime(start, end time.Time) *Future[UptimeResult]
	QueryDaemonSetLabels(start, end time.Time) *Future[LabelsResult]
	QueryDaemonSetAnnotations(start, end time.Time) *Future[AnnotationsResult]

	// Job
	QueryJobInfo(start, end time.Time) *Future[JobInfoResult]
	QueryJobUptime(start, end time.Time) *Future[UptimeResult]
	QueryJobLabels(start, end time.Time) *Future[LabelsResult]
	QueryJobAnnotations(start, end time.Time) *Future[AnnotationsResult]

	// CronJob
	QueryCronJobInfo(start, end time.Time) *Future[CronJobInfoResult]
	QueryCronJobUptime(start, end time.Time) *Future[UptimeResult]
	QueryCronJobLabels(start, end time.Time) *Future[LabelsResult]
	QueryCronJobAnnotations(start, end time.Time) *Future[AnnotationsResult]

	// ReplicaSet
	QueryReplicaSetInfo(start, end time.Time) *Future[ReplicaSetInfoResult]
	QueryReplicaSetUptime(start, end time.Time) *Future[UptimeResult]
	QueryReplicaSetLabels(start, end time.Time) *Future[LabelsResult]
	QueryReplicaSetAnnotations(start, end time.Time) *Future[AnnotationsResult]
	QueryReplicaSetOwners(start, end time.Time) *Future[OwnerResult]

	// Namespace
	QueryNamespaceInfo(start, end time.Time) *Future[NamespaceInfoResult]
	QueryNamespaceUptime(start, end time.Time) *Future[UptimeResult]

	// Service
	QueryServiceInfo(start, end time.Time) *Future[ServiceInfoResult]
	QueryServiceUptime(start, end time.Time) *Future[UptimeResult]
	QueryServiceSelectorLabels(start, end time.Time) *Future[ServiceLabelsResult]

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
	QueryNetNatGatewayZoneGiB(start, end time.Time) *Future[NetNatGatewayZoneGiBResult]
	QueryNetNatGatewayRegionGiB(start, end time.Time) *Future[NetNatGatewayRegionGiBResult]
	QueryNetNatGatewayInternetGiB(start, end time.Time) *Future[NetNatGatewayInternetGiBResult]
	QueryNetTransferBytes(start, end time.Time) *Future[NetTransferBytesResult]

	// Network Ingress
	QueryNetZoneIngressGiB(start, end time.Time) *Future[NetZoneIngressGiBResult]
	QueryNetRegionIngressGiB(start, end time.Time) *Future[NetRegionIngressGiBResult]
	QueryNetInternetIngressGiB(start, end time.Time) *Future[NetInternetIngressGiBResult]
	QueryNetInternetServiceIngressGiB(start, end time.Time) *Future[NetInternetServiceIngressGiBResult]
	QueryNetNatGatewayIngressPricePerGiB(start, end time.Time) *Future[NetNatGatewayPricePerGiBResult]
	QueryNetNatGatewayIngressGiB(start, end time.Time) *Future[NetNatGatewayIngressGiBResult]
	QueryNetNatGatewayZoneIngressGiB(start, end time.Time) *Future[NetNatGatewayZoneIngressGiBResult]
	QueryNetNatGatewayRegionIngressGiB(start, end time.Time) *Future[NetNatGatewayRegionIngressGiBResult]
	QueryNetNatGatewayInternetIngressGiB(start, end time.Time) *Future[NetNatGatewayInternetIngressGiBResult]
	QueryNetReceiveBytes(start, end time.Time) *Future[NetReceiveBytesResult]

	// Annotations
	QueryNamespaceAnnotations(start, end time.Time) *Future[NamespaceAnnotationsResult]
	QueryPodAnnotations(start, end time.Time) *Future[PodAnnotationsResult]

	// Labels
	QueryNodeLabels(start, end time.Time) *Future[NodeLabelsResult]
	QueryNamespaceLabels(start, end time.Time) *Future[NamespaceLabelsResult]
	QueryPodLabels(start, end time.Time) *Future[PodLabelsResult]

	QueryPodsWithDaemonSetOwner(start, end time.Time) *Future[PodsWithDaemonSetOwnerResult]
	QueryPodsWithJobOwner(start, end time.Time) *Future[PodsWithJobOwnerResult]

	// ReplicaSet -> Controller mapping
	QueryPodsWithReplicaSetOwner(start, end time.Time) *Future[PodsWithReplicaSetOwnerResult]
	QueryReplicaSetsWithoutOwners(start, end time.Time) *Future[ReplicaSetsWithoutOwnersResult]
	QueryReplicaSetsWithRollout(start, end time.Time) *Future[ReplicaSetsWithRolloutResult]

	// ResourceQuotas
	QueryResourceQuotaInfo(start, end time.Time) *Future[ResourceQuotaInfoResult]
	QueryResourceQuotaUptime(start, end time.Time) *Future[UptimeResult]
	QueryResourceQuotaSpecCPURequestAverage(start, end time.Time) *Future[ResourceResult]
	QueryResourceQuotaSpecCPURequestMax(start, end time.Time) *Future[ResourceResult]
	QueryResourceQuotaSpecRAMRequestAverage(start, end time.Time) *Future[ResourceResult]
	QueryResourceQuotaSpecRAMRequestMax(start, end time.Time) *Future[ResourceResult]
	QueryResourceQuotaSpecCPULimitAverage(start, end time.Time) *Future[ResourceResult]
	QueryResourceQuotaSpecCPULimitMax(start, end time.Time) *Future[ResourceResult]
	QueryResourceQuotaSpecRAMLimitAverage(start, end time.Time) *Future[ResourceResult]
	QueryResourceQuotaSpecRAMLimitMax(start, end time.Time) *Future[ResourceResult]
	QueryResourceQuotaStatusUsedCPURequestAverage(start, end time.Time) *Future[ResourceResult]
	QueryResourceQuotaStatusUsedCPURequestMax(start, end time.Time) *Future[ResourceResult]
	QueryResourceQuotaStatusUsedRAMRequestAverage(start, end time.Time) *Future[ResourceResult]
	QueryResourceQuotaStatusUsedRAMRequestMax(start, end time.Time) *Future[ResourceResult]
	QueryResourceQuotaStatusUsedCPULimitAverage(start, end time.Time) *Future[ResourceResult]
	QueryResourceQuotaStatusUsedCPULimitMax(start, end time.Time) *Future[ResourceResult]
	QueryResourceQuotaStatusUsedRAMLimitAverage(start, end time.Time) *Future[ResourceResult]
	QueryResourceQuotaStatusUsedRAMLimitMax(start, end time.Time) *Future[ResourceResult]

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
