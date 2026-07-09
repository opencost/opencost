package source

import (
	"time"
)

//--------------------------------------------------------------------------
//  No-Op MetricsQuerier (empty query results)
//--------------------------------------------------------------------------

// NoOpMetricsQuerier is a no-op implementation of the MetricsQuerier interface
// that returns empty results for all queries.
type NoOpMetricsQuerier struct{}

// NewNoOpMetricsQuerier creates a new mock metrics querier
func NewNoOpMetricsQuerier() *NoOpMetricsQuerier {
	return &NoOpMetricsQuerier{}
}

// Local Cluster Disks

func (m *NoOpMetricsQuerier) QueryLocalStorageActiveMinutes(start, end time.Time) *Future[LocalStorageActiveMinutesResult] {
	return newEmptyResult(DecodeLocalStorageActiveMinutesResult)
}

func (m *NoOpMetricsQuerier) QueryLocalStorageUsedAvg(start, end time.Time) *Future[LocalStorageUsedAvgResult] {
	return newEmptyResult(DecodeLocalStorageUsedAvgResult)
}

func (m *NoOpMetricsQuerier) QueryLocalStorageUsedMax(start, end time.Time) *Future[LocalStorageUsedMaxResult] {
	return newEmptyResult(DecodeLocalStorageUsedMaxResult)
}

func (m *NoOpMetricsQuerier) QueryLocalStorageBytes(start, end time.Time) *Future[LocalStorageBytesResult] {
	return newEmptyResult(DecodeLocalStorageBytesResult)
}

func (m *NoOpMetricsQuerier) QueryKMLocalStorageUsedAvg(start, end time.Time) *Future[NodeUIDValueResult] {
	return newEmptyResult(DecodeNodeUIDValueResult)
}

func (m *NoOpMetricsQuerier) QueryKMLocalStorageUsedMax(start, end time.Time) *Future[NodeUIDValueResult] {
	return newEmptyResult(DecodeNodeUIDValueResult)
}

func (m *NoOpMetricsQuerier) QueryKMLocalStorageBytes(start, end time.Time) *Future[UIDValueResult] {
	return newEmptyResult(DecodeUIDValueResult)
}

// Nodes

func (m *NoOpMetricsQuerier) QueryNodeInfo(start, end time.Time) *Future[NodeInfoResult] {
	return newEmptyResult(DecodeNodeInfoResult)
}

func (m *NoOpMetricsQuerier) QueryNodeUptime(start, end time.Time) *Future[UptimeResult] {
	return newEmptyResult(DecodeUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryNodeActiveMinutes(start, end time.Time) *Future[NodeActiveMinutesResult] {
	return newEmptyResult(DecodeNodeActiveMinutesResult)
}

func (m *NoOpMetricsQuerier) QueryNodeCPUCoresCapacity(start, end time.Time) *Future[NodeCPUCoresCapacityResult] {
	return newEmptyResult(DecodeNodeCPUCoresCapacityResult)
}

func (m *NoOpMetricsQuerier) QueryNodeCPUCoresAllocatable(start, end time.Time) *Future[NodeCPUCoresAllocatableResult] {
	return newEmptyResult(DecodeNodeCPUCoresAllocatableResult)
}

func (m *NoOpMetricsQuerier) QueryNodeRAMBytesCapacity(start, end time.Time) *Future[NodeRAMBytesCapacityResult] {
	return newEmptyResult(DecodeNodeRAMBytesCapacityResult)
}

func (m *NoOpMetricsQuerier) QueryNodeRAMBytesAllocatable(start, end time.Time) *Future[NodeRAMBytesAllocatableResult] {
	return newEmptyResult(DecodeNodeRAMBytesAllocatableResult)
}

func (m *NoOpMetricsQuerier) QueryNodeGPUCount(start, end time.Time) *Future[NodeGPUCountResult] {
	return newEmptyResult(DecodeNodeGPUCountResult)
}

func (m *NoOpMetricsQuerier) QueryNodeCPUModeTotal(start, end time.Time) *Future[NodeCPUModeTotalResult] {
	return newEmptyResult(DecodeNodeCPUModeTotalResult)
}

func (m *NoOpMetricsQuerier) QueryNodeIsSpot(start, end time.Time) *Future[NodeIsSpotResult] {
	return newEmptyResult(DecodeNodeIsSpotResult)
}

func (m *NoOpMetricsQuerier) QueryNodeRAMSystemPercent(start, end time.Time) *Future[NodeRAMSystemPercentResult] {
	return newEmptyResult(DecodeNodeRAMSystemPercentResult)
}

func (m *NoOpMetricsQuerier) QueryNodeRAMUserPercent(start, end time.Time) *Future[NodeRAMUserPercentResult] {
	return newEmptyResult(DecodeNodeRAMUserPercentResult)
}

func (m *NoOpMetricsQuerier) QueryNodeResourceCapacities(start, end time.Time) *Future[ResourceResult] {
	return newEmptyResult(DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryNodeResourcesAllocatable(start, end time.Time) *Future[ResourceResult] {
	return newEmptyResult(DecodeResourceResult)
}

// Load Balancers

func (m *NoOpMetricsQuerier) QueryLBActiveMinutes(start, end time.Time) *Future[LBActiveMinutesResult] {
	return newEmptyResult(DecodeLBActiveMinutesResult)
}

func (m *NoOpMetricsQuerier) QueryLBPricePerHr(start, end time.Time) *Future[LBPricePerHrResult] {
	return newEmptyResult(DecodeLBPricePerHrResult)
}

// Cluster Management

func (m *NoOpMetricsQuerier) QueryClusterInfo(start, end time.Time) *Future[ClusterInfoResult] {
	return newEmptyResult(DecodeClusterInfoResult)
}

func (m *NoOpMetricsQuerier) QueryClusterKubeModelVersion(start, end time.Time) *Future[ClusterKubeModelVersionResult] {
	return newEmptyResult(DecodeClusterKubeModelVersionResult)
}

func (m *NoOpMetricsQuerier) QueryClusterUptime(start, end time.Time) *Future[UptimeResult] {
	return newEmptyResult(DecodeUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryClusterManagementDuration(start, end time.Time) *Future[ClusterManagementDurationResult] {
	return newEmptyResult(DecodeClusterManagementDurationResult)
}

func (m *NoOpMetricsQuerier) QueryClusterManagementPricePerHr(start, end time.Time) *Future[ClusterManagementPricePerHrResult] {
	return newEmptyResult(DecodeClusterManagementPricePerHrResult)
}

// Pods

func (m *NoOpMetricsQuerier) QueryPods(start, end time.Time) *Future[PodsResult] {
	return newEmptyResult(DecodePodsResult)
}

func (m *NoOpMetricsQuerier) QueryPodsUID(start, end time.Time) *Future[PodsResult] {
	return newEmptyResult(DecodePodsResult)
}

func (m *NoOpMetricsQuerier) QueryPodInfo(start, end time.Time) *Future[PodInfoResult] {
	return newEmptyResult(DecodePodInfoResult)
}

func (m *NoOpMetricsQuerier) QueryPodUptime(start, end time.Time) *Future[UptimeResult] {
	return newEmptyResult(DecodeUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryPodOwners(start, end time.Time) *Future[OwnerResult] {
	return newEmptyResult(DecodeOwnerResult)
}

func (m *NoOpMetricsQuerier) QueryPodPVCVolumes(start, end time.Time) *Future[PodPVCVolumeResult] {
	return newEmptyResult(DecodePodPVCVolumeResult)
}

func (m *NoOpMetricsQuerier) QueryPodNetworkEgressBytes(start, end time.Time) *Future[PodNetworkBytesResult] {
	return newEmptyResult(DecodePodNetworkBytesResult)
}

func (m *NoOpMetricsQuerier) QueryPodNetworkIngressBytes(start, end time.Time) *Future[PodNetworkBytesResult] {
	return newEmptyResult(DecodePodNetworkBytesResult)
}

// Container

func (m *NoOpMetricsQuerier) QueryContainerUptime(start, end time.Time) *Future[ContainerUptimeResult] {
	return newEmptyResult(DecodeContainerUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryContainerResourceRequests(start, end time.Time) *Future[ContainerResourceResult] {
	return newEmptyResult(DecodeContainerResourceResult)
}

func (m *NoOpMetricsQuerier) QueryContainerResourceLimits(start, end time.Time) *Future[ContainerResourceResult] {
	return newEmptyResult(DecodeContainerResourceResult)
}

// RAM

func (m *NoOpMetricsQuerier) QueryRAMBytesAllocated(start, end time.Time) *Future[RAMBytesAllocatedResult] {
	return newEmptyResult(DecodeRAMBytesAllocatedResult)
}

func (m *NoOpMetricsQuerier) QueryRAMRequests(start, end time.Time) *Future[RAMRequestsResult] {
	return newEmptyResult(DecodeRAMRequestsResult)
}

func (m *NoOpMetricsQuerier) QueryRAMLimits(start, end time.Time) *Future[RAMLimitsResult] {
	return newEmptyResult(DecodeRAMLimitsResult)
}

func (m *NoOpMetricsQuerier) QueryRAMUsageAvg(start, end time.Time) *Future[RAMUsageAvgResult] {
	return newEmptyResult(DecodeRAMUsageAvgResult)
}

func (m *NoOpMetricsQuerier) QueryRAMUsageMax(start, end time.Time) *Future[RAMUsageMaxResult] {
	return newEmptyResult(DecodeRAMUsageMaxResult)
}

func (m *NoOpMetricsQuerier) QueryNodeRAMPricePerGiBHr(start, end time.Time) *Future[NodeRAMPricePerGiBHrResult] {
	return newEmptyResult(DecodeNodeRAMPricePerGiBHrResult)
}

// CPU

func (m *NoOpMetricsQuerier) QueryCPUCoresAllocated(start, end time.Time) *Future[CPUCoresAllocatedResult] {
	return newEmptyResult(DecodeCPUCoresAllocatedResult)
}

func (m *NoOpMetricsQuerier) QueryCPURequests(start, end time.Time) *Future[CPURequestsResult] {
	return newEmptyResult(DecodeCPURequestsResult)
}

func (m *NoOpMetricsQuerier) QueryCPULimits(start, end time.Time) *Future[CPULimitsResult] {
	return newEmptyResult(DecodeCPULimitsResult)
}

func (m *NoOpMetricsQuerier) QueryCPUUsageAvg(start, end time.Time) *Future[CPUUsageAvgResult] {
	return newEmptyResult(DecodeCPUUsageAvgResult)
}

func (m *NoOpMetricsQuerier) QueryCPUUsageMax(start, end time.Time) *Future[CPUUsageMaxResult] {
	return newEmptyResult(DecodeCPUUsageMaxResult)
}

func (m *NoOpMetricsQuerier) QueryNodeCPUPricePerHr(start, end time.Time) *Future[NodeCPUPricePerHrResult] {
	return newEmptyResult(DecodeNodeCPUPricePerHrResult)
}

// GPU

func (m *NoOpMetricsQuerier) QueryGPUsAllocated(start, end time.Time) *Future[GPUsAllocatedResult] {
	return newEmptyResult(DecodeGPUsAllocatedResult)
}

func (m *NoOpMetricsQuerier) QueryGPUsRequested(start, end time.Time) *Future[GPUsRequestedResult] {
	return newEmptyResult(DecodeGPUsRequestedResult)
}

func (m *NoOpMetricsQuerier) QueryGPUsUsageAvg(start, end time.Time) *Future[GPUsUsageAvgResult] {
	return newEmptyResult(DecodeGPUsUsageAvgResult)
}

func (m *NoOpMetricsQuerier) QueryGPUsUsageMax(start, end time.Time) *Future[GPUsUsageMaxResult] {
	return newEmptyResult(DecodeGPUsUsageMaxResult)
}

func (m *NoOpMetricsQuerier) QueryNodeGPUPricePerHr(start, end time.Time) *Future[NodeGPUPricePerHrResult] {
	return newEmptyResult(DecodeNodeGPUPricePerHrResult)
}

func (m *NoOpMetricsQuerier) QueryGPUInfo(start, end time.Time) *Future[GPUInfoResult] {
	return newEmptyResult(DecodeGPUInfoResult)
}

func (m *NoOpMetricsQuerier) QueryIsGPUShared(start, end time.Time) *Future[IsGPUSharedResult] {
	return newEmptyResult(DecodeIsGPUSharedResult)
}

// Device

func (m *NoOpMetricsQuerier) QueryDCGMDeviceInfo(start, end time.Time) *Future[DCGMDeviceInfoResult] {
	return newEmptyResult(DecodeDCGMDeviceInfoResult)
}

func (m *NoOpMetricsQuerier) QueryDCGMDeviceUptime(start, end time.Time) *Future[DCGMDeviceUptimeResult] {
	return newEmptyResult(DecodeDCGMDeviceUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryDCGMContainerUsageAvg(start, end time.Time) *Future[DCGMDeviceContainerUsageResult] {
	return newEmptyResult(DecodeDCGMDeviceContainerUsageResult)
}

func (m *NoOpMetricsQuerier) QueryDCGMContainerUsageMax(start, end time.Time) *Future[DCGMDeviceContainerUsageResult] {
	return newEmptyResult(DecodeDCGMDeviceContainerUsageResult)
}

// PVC

func (m *NoOpMetricsQuerier) QueryPodPVCAllocation(start, end time.Time) *Future[PodPVCAllocationResult] {
	return newEmptyResult(DecodePodPVCAllocationResult)
}

func (m *NoOpMetricsQuerier) QueryPVCBytesRequested(start, end time.Time) *Future[PVCBytesRequestedResult] {
	return newEmptyResult(DecodePVCBytesRequestedResult)
}

func (m *NoOpMetricsQuerier) QueryPVCInfo(start, end time.Time) *Future[PVCInfoResult] {
	return newEmptyResult(DecodePVCInfoResult)
}

func (m *NoOpMetricsQuerier) QueryKMPVCInfo(start, end time.Time) *Future[PVCInfoResult] {
	return newEmptyResult(DecodePVCInfoResult)
}

func (m *NoOpMetricsQuerier) QueryPVCUptime(start, end time.Time) *Future[UptimeResult] {
	return newEmptyResult(DecodeUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryPVCBytesUsedAverage(start, end time.Time) *Future[PVCUIDValueResult] {
	return newEmptyResult(DecodePVCUIDValueResult)
}

func (m *NoOpMetricsQuerier) QueryPVCBytesUsedMax(start, end time.Time) *Future[PVCUIDValueResult] {
	return newEmptyResult(DecodePVCUIDValueResult)
}

// PV

func (m *NoOpMetricsQuerier) QueryPVBytes(start, end time.Time) *Future[PVBytesResult] {
	return newEmptyResult(DecodePVBytesResult)
}

func (m *NoOpMetricsQuerier) QueryPVPricePerGiBHour(start, end time.Time) *Future[PVPricePerGiBHourResult] {
	return newEmptyResult(DecodePVPricePerGiBHourResult)
}

func (m *NoOpMetricsQuerier) QueryPVInfo(start, end time.Time) *Future[PVInfoResult] {
	return newEmptyResult(DecodePVInfoResult)
}

func (m *NoOpMetricsQuerier) QueryPVActiveMinutes(start, end time.Time) *Future[PVActiveMinutesResult] {
	return newEmptyResult(DecodePVActiveMinutesResult)
}

func (m *NoOpMetricsQuerier) QueryPVUsedAverage(start, end time.Time) *Future[PVUsedAvgResult] {
	return newEmptyResult(DecodePVUsedAvgResult)
}

func (m *NoOpMetricsQuerier) QueryPVUsedMax(start, end time.Time) *Future[PVUsedMaxResult] {
	return newEmptyResult(DecodePVUsedMaxResult)
}

func (m *NoOpMetricsQuerier) QueryKMPVInfo(start, end time.Time) *Future[PVInfoResult] {
	return newEmptyResult(DecodePVInfoResult)
}

func (m *NoOpMetricsQuerier) QueryPVUptime(start, end time.Time) *Future[UptimeResult] {
	return newEmptyResult(DecodeUptimeResult)
}

// Deployment

func (m *NoOpMetricsQuerier) QueryDeploymentInfo(start, end time.Time) *Future[DeploymentInfoResult] {
	return newEmptyResult(DecodeDeploymentInfoResult)
}

func (m *NoOpMetricsQuerier) QueryDeploymentUptime(start, end time.Time) *Future[UptimeResult] {
	return newEmptyResult(DecodeUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryDeploymentLabels(start, end time.Time) *Future[LabelsResult] {
	return newEmptyResult(DecodeLabelsResult)
}

func (m *NoOpMetricsQuerier) QueryDeploymentAnnotations(start, end time.Time) *Future[AnnotationsResult] {
	return newEmptyResult(DecodeAnnotationsResult)
}

func (m *NoOpMetricsQuerier) QueryDeploymentMatchLabels(start, end time.Time) *Future[DeploymentLabelsResult] {
	return newEmptyResult(DecodeDeploymentLabelsResult)
}

// StatefulSet

func (m *NoOpMetricsQuerier) QueryStatefulSetInfo(start, end time.Time) *Future[StatefulSetInfoResult] {
	return newEmptyResult(DecodeStatefulSetInfoResult)
}

func (m *NoOpMetricsQuerier) QueryStatefulSetUptime(start, end time.Time) *Future[UptimeResult] {
	return newEmptyResult(DecodeUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryStatefulSetLabels(start, end time.Time) *Future[LabelsResult] {
	return newEmptyResult(DecodeLabelsResult)
}

func (m *NoOpMetricsQuerier) QueryStatefulSetAnnotations(start, end time.Time) *Future[AnnotationsResult] {
	return newEmptyResult(DecodeAnnotationsResult)
}

func (m *NoOpMetricsQuerier) QueryStatefulSetMatchLabels(start, end time.Time) *Future[StatefulSetLabelsResult] {
	return newEmptyResult(DecodeStatefulSetLabelsResult)
}

// DaemonSet

func (m *NoOpMetricsQuerier) QueryDaemonSetInfo(start, end time.Time) *Future[DaemonSetInfoResult] {
	return newEmptyResult(DecodeDaemonSetInfoResult)
}

func (m *NoOpMetricsQuerier) QueryDaemonSetUptime(start, end time.Time) *Future[UptimeResult] {
	return newEmptyResult(DecodeUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryDaemonSetLabels(start, end time.Time) *Future[LabelsResult] {
	return newEmptyResult(DecodeLabelsResult)
}

func (m *NoOpMetricsQuerier) QueryDaemonSetAnnotations(start, end time.Time) *Future[AnnotationsResult] {
	return newEmptyResult(DecodeAnnotationsResult)
}

// Job

func (m *NoOpMetricsQuerier) QueryJobInfo(start, end time.Time) *Future[JobInfoResult] {
	return newEmptyResult(DecodeJobInfoResult)
}

func (m *NoOpMetricsQuerier) QueryJobUptime(start, end time.Time) *Future[UptimeResult] {
	return newEmptyResult(DecodeUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryJobLabels(start, end time.Time) *Future[LabelsResult] {
	return newEmptyResult(DecodeLabelsResult)
}

func (m *NoOpMetricsQuerier) QueryJobAnnotations(start, end time.Time) *Future[AnnotationsResult] {
	return newEmptyResult(DecodeAnnotationsResult)
}

// CronJob

func (m *NoOpMetricsQuerier) QueryCronJobInfo(start, end time.Time) *Future[CronJobInfoResult] {
	return newEmptyResult(DecodeCronJobInfoResult)
}

func (m *NoOpMetricsQuerier) QueryCronJobUptime(start, end time.Time) *Future[UptimeResult] {
	return newEmptyResult(DecodeUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryCronJobLabels(start, end time.Time) *Future[LabelsResult] {
	return newEmptyResult(DecodeLabelsResult)
}

func (m *NoOpMetricsQuerier) QueryCronJobAnnotations(start, end time.Time) *Future[AnnotationsResult] {
	return newEmptyResult(DecodeAnnotationsResult)
}

// ReplicaSet

func (m *NoOpMetricsQuerier) QueryReplicaSetInfo(start, end time.Time) *Future[ReplicaSetInfoResult] {
	return newEmptyResult(DecodeReplicaSetInfoResult)
}

func (m *NoOpMetricsQuerier) QueryReplicaSetUptime(start, end time.Time) *Future[UptimeResult] {
	return newEmptyResult(DecodeUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryReplicaSetLabels(start, end time.Time) *Future[LabelsResult] {
	return newEmptyResult(DecodeLabelsResult)
}

func (m *NoOpMetricsQuerier) QueryReplicaSetAnnotations(start, end time.Time) *Future[AnnotationsResult] {
	return newEmptyResult(DecodeAnnotationsResult)
}

func (m *NoOpMetricsQuerier) QueryReplicaSetOwners(start, end time.Time) *Future[OwnerResult] {
	return newEmptyResult(DecodeOwnerResult)
}

// Namespace

func (m *NoOpMetricsQuerier) QueryNamespaceInfo(start, end time.Time) *Future[NamespaceInfoResult] {
	return newEmptyResult(DecodeNamespaceInfoResult)
}

func (m *NoOpMetricsQuerier) QueryNamespaceUptime(start, end time.Time) *Future[UptimeResult] {
	return newEmptyResult(DecodeUptimeResult)
}

// Service

func (m *NoOpMetricsQuerier) QueryServiceInfo(start, end time.Time) *Future[ServiceInfoResult] {
	return newEmptyResult(DecodeServiceInfoResult)
}

func (m *NoOpMetricsQuerier) QueryServiceUptime(start, end time.Time) *Future[UptimeResult] {
	return newEmptyResult(DecodeUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryServiceSelectorLabels(start, end time.Time) *Future[ServiceLabelsResult] {
	return newEmptyResult(DecodeServiceLabelsResult)
}

// Network Egress

func (m *NoOpMetricsQuerier) QueryNetZoneGiB(start, end time.Time) *Future[NetZoneGiBResult] {
	return newEmptyResult(DecodeNetZoneGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetZonePricePerGiB(start, end time.Time) *Future[NetZonePricePerGiBResult] {
	return newEmptyResult(DecodeNetZonePricePerGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetRegionGiB(start, end time.Time) *Future[NetRegionGiBResult] {
	return newEmptyResult(DecodeNetRegionGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetRegionPricePerGiB(start, end time.Time) *Future[NetRegionPricePerGiBResult] {
	return newEmptyResult(DecodeNetRegionPricePerGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetInternetGiB(start, end time.Time) *Future[NetInternetGiBResult] {
	return newEmptyResult(DecodeNetInternetGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetInternetPricePerGiB(start, end time.Time) *Future[NetInternetPricePerGiBResult] {
	return newEmptyResult(DecodeNetInternetPricePerGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetInternetServiceGiB(start, end time.Time) *Future[NetInternetServiceGiBResult] {
	return newEmptyResult(DecodeNetInternetServiceGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetNatGatewayPricePerGiB(start, end time.Time) *Future[NetNatGatewayPricePerGiBResult] {
	return newEmptyResult(DecodeNetNatGatewayPricePerGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetNatGatewayGiB(start, end time.Time) *Future[NetNatGatewayGiBResult] {
	return newEmptyResult(DecodeNetNatGatewayGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetTransferBytes(start, end time.Time) *Future[NetTransferBytesResult] {
	return newEmptyResult(DecodeNetTransferBytesResult)
}

// Network Ingress

func (m *NoOpMetricsQuerier) QueryNetZoneIngressGiB(start, end time.Time) *Future[NetZoneIngressGiBResult] {
	return newEmptyResult(DecodeNetZoneIngressGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetRegionIngressGiB(start, end time.Time) *Future[NetRegionIngressGiBResult] {
	return newEmptyResult(DecodeNetRegionIngressGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetInternetIngressGiB(start, end time.Time) *Future[NetInternetIngressGiBResult] {
	return newEmptyResult(DecodeNetInternetIngressGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetInternetServiceIngressGiB(start, end time.Time) *Future[NetInternetServiceIngressGiBResult] {
	return newEmptyResult(DecodeNetInternetServiceIngressGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetNatGatewayIngressPricePerGiB(start, end time.Time) *Future[NetNatGatewayPricePerGiBResult] {
	return newEmptyResult(DecodeNetNatGatewayPricePerGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetNatGatewayIngressGiB(start, end time.Time) *Future[NetNatGatewayIngressGiBResult] {
	return newEmptyResult(DecodeNetNatGatewayIngressGiBResult)
}

func (m *NoOpMetricsQuerier) QueryNetReceiveBytes(start, end time.Time) *Future[NetReceiveBytesResult] {
	return newEmptyResult(DecodeNetReceiveBytesResult)
}

// Annotations

func (m *NoOpMetricsQuerier) QueryNamespaceAnnotations(start, end time.Time) *Future[NamespaceAnnotationsResult] {
	return newEmptyResult(DecodeNamespaceAnnotationsResult)
}

func (m *NoOpMetricsQuerier) QueryPodAnnotations(start, end time.Time) *Future[PodAnnotationsResult] {
	return newEmptyResult(DecodePodAnnotationsResult)
}

// Labels

func (m *NoOpMetricsQuerier) QueryNodeLabels(start, end time.Time) *Future[NodeLabelsResult] {
	return newEmptyResult(DecodeNodeLabelsResult)
}

func (m *NoOpMetricsQuerier) QueryNamespaceLabels(start, end time.Time) *Future[NamespaceLabelsResult] {
	return newEmptyResult(DecodeNamespaceLabelsResult)
}

func (m *NoOpMetricsQuerier) QueryPodLabels(start, end time.Time) *Future[PodLabelsResult] {
	return newEmptyResult(DecodePodLabelsResult)
}

func (m *NoOpMetricsQuerier) QueryPodsWithDaemonSetOwner(start, end time.Time) *Future[PodsWithDaemonSetOwnerResult] {
	return newEmptyResult(DecodePodsWithDaemonSetOwnerResult)
}

func (m *NoOpMetricsQuerier) QueryPodsWithJobOwner(start, end time.Time) *Future[PodsWithJobOwnerResult] {
	return newEmptyResult(DecodePodsWithJobOwnerResult)
}

// ReplicaSet -> Controller mapping

func (m *NoOpMetricsQuerier) QueryPodsWithReplicaSetOwner(start, end time.Time) *Future[PodsWithReplicaSetOwnerResult] {
	return newEmptyResult(DecodePodsWithReplicaSetOwnerResult)
}

func (m *NoOpMetricsQuerier) QueryReplicaSetsWithoutOwners(start, end time.Time) *Future[ReplicaSetsWithoutOwnersResult] {
	return newEmptyResult(DecodeReplicaSetsWithoutOwnersResult)
}

func (m *NoOpMetricsQuerier) QueryReplicaSetsWithRollout(start, end time.Time) *Future[ReplicaSetsWithRolloutResult] {
	return newEmptyResult(DecodeReplicaSetsWithRolloutResult)
}

// ResourceQuotas

func (m *NoOpMetricsQuerier) QueryResourceQuotaInfo(start, end time.Time) *Future[ResourceQuotaInfoResult] {
	return newEmptyResult(DecodeResourceQuotaInfoResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaUptime(start, end time.Time) *Future[UptimeResult] {
	return newEmptyResult(DecodeUptimeResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaSpecCPURequestAverage(start, end time.Time) *Future[ResourceResult] {
	return newEmptyResult(DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaSpecCPURequestMax(start, end time.Time) *Future[ResourceResult] {
	return newEmptyResult(DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaSpecRAMRequestAverage(start, end time.Time) *Future[ResourceResult] {
	return newEmptyResult(DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaSpecRAMRequestMax(start, end time.Time) *Future[ResourceResult] {
	return newEmptyResult(DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaSpecCPULimitAverage(start, end time.Time) *Future[ResourceResult] {
	return newEmptyResult(DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaSpecCPULimitMax(start, end time.Time) *Future[ResourceResult] {
	return newEmptyResult(DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaSpecRAMLimitAverage(start, end time.Time) *Future[ResourceResult] {
	return newEmptyResult(DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaSpecRAMLimitMax(start, end time.Time) *Future[ResourceResult] {
	return newEmptyResult(DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaStatusUsedCPURequestAverage(start, end time.Time) *Future[ResourceResult] {
	return newEmptyResult(DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaStatusUsedCPURequestMax(start, end time.Time) *Future[ResourceResult] {
	return newEmptyResult(DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaStatusUsedRAMRequestAverage(start, end time.Time) *Future[ResourceResult] {
	return newEmptyResult(DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaStatusUsedRAMRequestMax(start, end time.Time) *Future[ResourceResult] {
	return newEmptyResult(DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaStatusUsedCPULimitAverage(start, end time.Time) *Future[ResourceResult] {
	return newEmptyResult(DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaStatusUsedCPULimitMax(start, end time.Time) *Future[ResourceResult] {
	return newEmptyResult(DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaStatusUsedRAMLimitAverage(start, end time.Time) *Future[ResourceResult] {
	return newEmptyResult(DecodeResourceResult)
}

func (m *NoOpMetricsQuerier) QueryResourceQuotaStatusUsedRAMLimitMax(start, end time.Time) *Future[ResourceResult] {
	return newEmptyResult(DecodeResourceResult)
}

// Inference Metrics

func (m *NoOpMetricsQuerier) QueryInferencePromptTokens(start, end time.Time) *Future[InferenceTokensResult] {
	return newEmptyResult(DecodeInferenceTokensResult)
}

func (m *NoOpMetricsQuerier) QueryInferenceGenerationTokens(start, end time.Time) *Future[InferenceTokensResult] {
	return newEmptyResult(DecodeInferenceTokensResult)
}

func (m *NoOpMetricsQuerier) QueryInferenceInputProcessingTime(start, end time.Time) *Future[InferenceProcessingTimeResult] {
	return newEmptyResult(DecodeInferenceProcessingTimeResult)
}

func (m *NoOpMetricsQuerier) QueryInferenceOutputProcessingTime(start, end time.Time) *Future[InferenceProcessingTimeResult] {
	return newEmptyResult(DecodeInferenceProcessingTimeResult)
}

func (m *NoOpMetricsQuerier) QueryInferenceCachedTokens(start, end time.Time) *Future[InferenceTokensResult] {
	return newEmptyResult(DecodeInferenceTokensResult)
}

func (m *NoOpMetricsQuerier) QueryInferenceCacheConfig(t time.Time) *Future[InferenceCacheConfigResult] {
	return newEmptyResult(DecodeInferenceCacheConfigResult)
}

func (m *NoOpMetricsQuerier) QueryInferenceKVCacheUsageAvg(start, end time.Time) *Future[InferenceServerMetricResult] {
	return newEmptyResult(DecodeInferenceServerMetricResult)
}

func (m *NoOpMetricsQuerier) QueryInferenceKVCacheUsageMax(start, end time.Time) *Future[InferenceServerMetricResult] {
	return newEmptyResult(DecodeInferenceServerMetricResult)
}

func (m *NoOpMetricsQuerier) QueryInferenceQueueDepthAvg(start, end time.Time) *Future[InferenceServerMetricResult] {
	return newEmptyResult(DecodeInferenceServerMetricResult)
}

func (m *NoOpMetricsQuerier) QueryInferenceQueueDepthMax(start, end time.Time) *Future[InferenceServerMetricResult] {
	return newEmptyResult(DecodeInferenceServerMetricResult)
}

func (m *NoOpMetricsQuerier) QueryInferenceRunningRequestsAvg(start, end time.Time) *Future[InferenceServerMetricResult] {
	return newEmptyResult(DecodeInferenceServerMetricResult)
}

func (m *NoOpMetricsQuerier) QueryInferencePreemptions(start, end time.Time) *Future[InferenceServerMetricResult] {
	return newEmptyResult(DecodeInferenceServerMetricResult)
}

// Data Coverage Query

func (m *NoOpMetricsQuerier) QueryDataCoverage(_ int) (time.Time, time.Time, error) {
	return time.Time{}, time.Time{}, nil
}

// Extra methods not in MetricsQuerier interface

func (m *NoOpMetricsQuerier) QueryLocalStorageCost(start, end time.Time) *Future[LocalStorageCostResult] {
	return newEmptyResult(DecodeLocalStorageCostResult)
}

func (m *NoOpMetricsQuerier) QueryLocalStorageUsedCost(start, end time.Time) *Future[LocalStorageUsedCostResult] {
	return newEmptyResult(DecodeLocalStorageUsedCostResult)
}

func (m *NoOpMetricsQuerier) QueryServiceLabels(start, end time.Time) *Future[ServiceLabelsResult] {
	return newEmptyResult(DecodeServiceLabelsResult)
}

func newEmptyResult[T any](decoder ResultDecoder[T]) *Future[T] {
	ch := make(QueryResultsChan)
	go func() {
		results := NewQueryResults("")
		ch <- results
	}()

	return NewFuture(decoder, ch)
}
