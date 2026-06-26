package source

import (
	"time"
)

var _ MetricsQuerier = (*RecordMetricsQuerier)(nil)

// RecordMetricsQuerier is a wrapper implementation of MetricsQuerier which counts the number of times each function is
// called
type RecordMetricsQuerier struct {
	Calls   map[string]int
	Querier MetricsQuerier
}

// NewRecordMetricsQuerier creates a new mock metrics querier
func NewRecordMetricsQuerier(querier MetricsQuerier) *RecordMetricsQuerier {
	return &RecordMetricsQuerier{
		Calls:   make(map[string]int),
		Querier: querier,
	}
}

// Helper to record method calls
func (m *RecordMetricsQuerier) recordCall(method string) {
	m.Calls[method]++
}

// Local Cluster Disks

func (m *RecordMetricsQuerier) QueryLocalStorageActiveMinutes(start, end time.Time) *Future[LocalStorageActiveMinutesResult] {
	m.recordCall(QueryLocalStorageActiveMinutes)
	return m.Querier.QueryLocalStorageActiveMinutes(start, end)
}

func (m *RecordMetricsQuerier) QueryLocalStorageUsedAvg(start, end time.Time) *Future[LocalStorageUsedAvgResult] {
	m.recordCall(QueryLocalStorageUsedAvg)
	return m.Querier.QueryLocalStorageUsedAvg(start, end)
}

func (m *RecordMetricsQuerier) QueryLocalStorageUsedMax(start, end time.Time) *Future[LocalStorageUsedMaxResult] {
	m.recordCall(QueryLocalStorageUsedMax)
	return m.Querier.QueryLocalStorageUsedMax(start, end)
}

func (m *RecordMetricsQuerier) QueryLocalStorageBytes(start, end time.Time) *Future[LocalStorageBytesResult] {
	m.recordCall(QueryLocalStorageBytes)
	return m.Querier.QueryLocalStorageBytes(start, end)
}

func (m *RecordMetricsQuerier) QueryKMLocalStorageUsedAvg(start, end time.Time) *Future[NodeUIDValueResult] {
	m.recordCall(QueryKMLocalStorageUsedAvg)
	return m.Querier.QueryKMLocalStorageUsedAvg(start, end)
}

func (m *RecordMetricsQuerier) QueryKMLocalStorageUsedMax(start, end time.Time) *Future[NodeUIDValueResult] {
	m.recordCall(QueryKMLocalStorageUsedMax)
	return m.Querier.QueryKMLocalStorageUsedMax(start, end)
}

func (m *RecordMetricsQuerier) QueryKMLocalStorageBytes(start, end time.Time) *Future[UIDValueResult] {
	m.recordCall(QueryKMLocalStorageBytes)
	return m.Querier.QueryKMLocalStorageBytes(start, end)
}

// Nodes

func (m *RecordMetricsQuerier) QueryNodeInfo(start, end time.Time) *Future[NodeInfoResult] {
	m.recordCall(QueryNodeInfo)
	return m.Querier.QueryNodeInfo(start, end)
}

func (m *RecordMetricsQuerier) QueryNodeUptime(start, end time.Time) *Future[UptimeResult] {
	m.recordCall(QueryNodeUptime)
	return m.Querier.QueryNodeUptime(start, end)
}

func (m *RecordMetricsQuerier) QueryNodeActiveMinutes(start, end time.Time) *Future[NodeActiveMinutesResult] {
	m.recordCall(QueryNodeActiveMinutes)
	return m.Querier.QueryNodeActiveMinutes(start, end)
}

func (m *RecordMetricsQuerier) QueryNodeCPUCoresCapacity(start, end time.Time) *Future[NodeCPUCoresCapacityResult] {
	m.recordCall(QueryNodeCPUCoresCapacity)
	return m.Querier.QueryNodeCPUCoresCapacity(start, end)
}

func (m *RecordMetricsQuerier) QueryNodeCPUCoresAllocatable(start, end time.Time) *Future[NodeCPUCoresAllocatableResult] {
	m.recordCall(QueryNodeCPUCoresAllocatable)
	return m.Querier.QueryNodeCPUCoresAllocatable(start, end)
}

func (m *RecordMetricsQuerier) QueryNodeRAMBytesCapacity(start, end time.Time) *Future[NodeRAMBytesCapacityResult] {
	m.recordCall(QueryNodeRAMBytesCapacity)
	return m.Querier.QueryNodeRAMBytesCapacity(start, end)
}

func (m *RecordMetricsQuerier) QueryNodeRAMBytesAllocatable(start, end time.Time) *Future[NodeRAMBytesAllocatableResult] {
	m.recordCall(QueryNodeRAMBytesAllocatable)
	return m.Querier.QueryNodeRAMBytesAllocatable(start, end)
}

func (m *RecordMetricsQuerier) QueryNodeGPUCount(start, end time.Time) *Future[NodeGPUCountResult] {
	m.recordCall(QueryNodeGPUCount)
	return m.Querier.QueryNodeGPUCount(start, end)
}

func (m *RecordMetricsQuerier) QueryNodeCPUModeTotal(start, end time.Time) *Future[NodeCPUModeTotalResult] {
	m.recordCall(QueryNodeCPUModeTotal)
	return m.Querier.QueryNodeCPUModeTotal(start, end)
}

func (m *RecordMetricsQuerier) QueryNodeIsSpot(start, end time.Time) *Future[NodeIsSpotResult] {
	m.recordCall(QueryNodeIsSpot)
	return m.Querier.QueryNodeIsSpot(start, end)
}

func (m *RecordMetricsQuerier) QueryNodeRAMSystemPercent(start, end time.Time) *Future[NodeRAMSystemPercentResult] {
	m.recordCall(QueryNodeRAMSystemPercent)
	return m.Querier.QueryNodeRAMSystemPercent(start, end)
}

func (m *RecordMetricsQuerier) QueryNodeRAMUserPercent(start, end time.Time) *Future[NodeRAMUserPercentResult] {
	m.recordCall(QueryNodeRAMUserPercent)
	return m.Querier.QueryNodeRAMUserPercent(start, end)
}

func (m *RecordMetricsQuerier) QueryNodeResourceCapacities(start, end time.Time) *Future[ResourceResult] {
	m.recordCall(QueryNodeResourceCapacities)
	return m.Querier.QueryNodeResourceCapacities(start, end)
}

func (m *RecordMetricsQuerier) QueryNodeResourcesAllocatable(start, end time.Time) *Future[ResourceResult] {
	m.recordCall(QueryNodeResourcesAllocatable)
	return m.Querier.QueryNodeResourcesAllocatable(start, end)
}

// Load Balancers

func (m *RecordMetricsQuerier) QueryLBActiveMinutes(start, end time.Time) *Future[LBActiveMinutesResult] {
	m.recordCall(QueryLBActiveMinutes)
	return m.Querier.QueryLBActiveMinutes(start, end)
}

func (m *RecordMetricsQuerier) QueryLBPricePerHr(start, end time.Time) *Future[LBPricePerHrResult] {
	m.recordCall(QueryLBPricePerHr)
	return m.Querier.QueryLBPricePerHr(start, end)
}

// Cluster Management

func (m *RecordMetricsQuerier) QueryClusterInfo(start, end time.Time) *Future[ClusterInfoResult] {
	m.recordCall(QueryClusterInfo)
	return m.Querier.QueryClusterInfo(start, end)
}

func (m *RecordMetricsQuerier) QueryClusterUptime(start, end time.Time) *Future[UptimeResult] {
	m.recordCall(QueryClusterUptime)
	return m.Querier.QueryClusterUptime(start, end)
}

func (m *RecordMetricsQuerier) QueryClusterManagementDuration(start, end time.Time) *Future[ClusterManagementDurationResult] {
	m.recordCall(QueryClusterManagementDuration)
	return m.Querier.QueryClusterManagementDuration(start, end)
}

func (m *RecordMetricsQuerier) QueryClusterManagementPricePerHr(start, end time.Time) *Future[ClusterManagementPricePerHrResult] {
	m.recordCall(QueryClusterManagementPricePerHr)
	return m.Querier.QueryClusterManagementPricePerHr(start, end)
}

// Pods

func (m *RecordMetricsQuerier) QueryPods(start, end time.Time) *Future[PodsResult] {
	m.recordCall(QueryPods)
	return m.Querier.QueryPods(start, end)
}

func (m *RecordMetricsQuerier) QueryPodsUID(start, end time.Time) *Future[PodsResult] {
	m.recordCall(QueryPodsUID)
	return m.Querier.QueryPodsUID(start, end)
}

func (m *RecordMetricsQuerier) QueryPodInfo(start, end time.Time) *Future[PodInfoResult] {
	m.recordCall(QueryPodInfo)
	return m.Querier.QueryPodInfo(start, end)
}

func (m *RecordMetricsQuerier) QueryPodUptime(start, end time.Time) *Future[UptimeResult] {
	m.recordCall(QueryPodUptime)
	return m.Querier.QueryPodUptime(start, end)
}

func (m *RecordMetricsQuerier) QueryPodOwners(start, end time.Time) *Future[OwnerResult] {
	m.recordCall(QueryPodOwners)
	return m.Querier.QueryPodOwners(start, end)
}

func (m *RecordMetricsQuerier) QueryPodPVCVolumes(start, end time.Time) *Future[PodPVCVolumeResult] {
	m.recordCall(QueryPodPVCVolumes)
	return m.Querier.QueryPodPVCVolumes(start, end)
}

func (m *RecordMetricsQuerier) QueryPodNetworkEgressBytes(start, end time.Time) *Future[PodNetworkBytesResult] {
	m.recordCall(QueryPodNetworkEgressBytes)
	return m.Querier.QueryPodNetworkEgressBytes(start, end)
}

func (m *RecordMetricsQuerier) QueryPodNetworkIngressBytes(start, end time.Time) *Future[PodNetworkBytesResult] {
	m.recordCall(QueryPodNetworkIngressBytes)
	return m.Querier.QueryPodNetworkIngressBytes(start, end)
}

// Container

func (m *RecordMetricsQuerier) QueryContainerUptime(start, end time.Time) *Future[ContainerUptimeResult] {
	m.recordCall(QueryContainerUptime)
	return m.Querier.QueryContainerUptime(start, end)
}

func (m *RecordMetricsQuerier) QueryContainerResourceRequests(start, end time.Time) *Future[ContainerResourceResult] {
	m.recordCall(QueryContainerResourceRequests)
	return m.Querier.QueryContainerResourceRequests(start, end)
}

func (m *RecordMetricsQuerier) QueryContainerResourceLimits(start, end time.Time) *Future[ContainerResourceResult] {
	m.recordCall(QueryContainerResourceLimits)
	return m.Querier.QueryContainerResourceLimits(start, end)
}

// RAM

func (m *RecordMetricsQuerier) QueryRAMBytesAllocated(start, end time.Time) *Future[RAMBytesAllocatedResult] {
	m.recordCall(QueryRAMBytesAllocated)
	return m.Querier.QueryRAMBytesAllocated(start, end)
}

func (m *RecordMetricsQuerier) QueryRAMRequests(start, end time.Time) *Future[RAMRequestsResult] {
	m.recordCall(QueryRAMRequests)
	return m.Querier.QueryRAMRequests(start, end)
}

func (m *RecordMetricsQuerier) QueryRAMLimits(start, end time.Time) *Future[RAMLimitsResult] {
	m.recordCall(QueryRAMLimits)
	return m.Querier.QueryRAMLimits(start, end)
}

func (m *RecordMetricsQuerier) QueryRAMUsageAvg(start, end time.Time) *Future[RAMUsageAvgResult] {
	m.recordCall(QueryRAMUsageAvg)
	return m.Querier.QueryRAMUsageAvg(start, end)
}

func (m *RecordMetricsQuerier) QueryRAMUsageMax(start, end time.Time) *Future[RAMUsageMaxResult] {
	m.recordCall(QueryRAMUsageMax)
	return m.Querier.QueryRAMUsageMax(start, end)
}

func (m *RecordMetricsQuerier) QueryNodeRAMPricePerGiBHr(start, end time.Time) *Future[NodeRAMPricePerGiBHrResult] {
	m.recordCall(QueryNodeRAMPricePerGiBHr)
	return m.Querier.QueryNodeRAMPricePerGiBHr(start, end)
}

// CPU

func (m *RecordMetricsQuerier) QueryCPUCoresAllocated(start, end time.Time) *Future[CPUCoresAllocatedResult] {
	m.recordCall(QueryCPUCoresAllocated)
	return m.Querier.QueryCPUCoresAllocated(start, end)
}

func (m *RecordMetricsQuerier) QueryCPURequests(start, end time.Time) *Future[CPURequestsResult] {
	m.recordCall(QueryCPURequests)
	return m.Querier.QueryCPURequests(start, end)
}

func (m *RecordMetricsQuerier) QueryCPULimits(start, end time.Time) *Future[CPULimitsResult] {
	m.recordCall(QueryCPULimits)
	return m.Querier.QueryCPULimits(start, end)
}

func (m *RecordMetricsQuerier) QueryCPUUsageAvg(start, end time.Time) *Future[CPUUsageAvgResult] {
	m.recordCall(QueryCPUUsageAvg)
	return m.Querier.QueryCPUUsageAvg(start, end)
}

func (m *RecordMetricsQuerier) QueryCPUUsageMax(start, end time.Time) *Future[CPUUsageMaxResult] {
	m.recordCall(QueryCPUUsageMax)
	return m.Querier.QueryCPUUsageMax(start, end)
}

func (m *RecordMetricsQuerier) QueryNodeCPUPricePerHr(start, end time.Time) *Future[NodeCPUPricePerHrResult] {
	m.recordCall(QueryNodeCPUPricePerHr)
	return m.Querier.QueryNodeCPUPricePerHr(start, end)
}

// GPU

func (m *RecordMetricsQuerier) QueryGPUsAllocated(start, end time.Time) *Future[GPUsAllocatedResult] {
	m.recordCall(QueryGPUsAllocated)
	return m.Querier.QueryGPUsAllocated(start, end)
}

func (m *RecordMetricsQuerier) QueryGPUsRequested(start, end time.Time) *Future[GPUsRequestedResult] {
	m.recordCall(QueryGPUsRequested)
	return m.Querier.QueryGPUsRequested(start, end)
}

func (m *RecordMetricsQuerier) QueryGPUsUsageAvg(start, end time.Time) *Future[GPUsUsageAvgResult] {
	m.recordCall(QueryGPUsUsageAvg)
	return m.Querier.QueryGPUsUsageAvg(start, end)
}

func (m *RecordMetricsQuerier) QueryGPUsUsageMax(start, end time.Time) *Future[GPUsUsageMaxResult] {
	m.recordCall(QueryGPUsUsageMax)
	return m.Querier.QueryGPUsUsageMax(start, end)
}

func (m *RecordMetricsQuerier) QueryNodeGPUPricePerHr(start, end time.Time) *Future[NodeGPUPricePerHrResult] {
	m.recordCall(QueryNodeGPUPricePerHr)
	return m.Querier.QueryNodeGPUPricePerHr(start, end)
}

func (m *RecordMetricsQuerier) QueryGPUInfo(start, end time.Time) *Future[GPUInfoResult] {
	m.recordCall(QueryGPUInfo)
	return m.Querier.QueryGPUInfo(start, end)
}

func (m *RecordMetricsQuerier) QueryIsGPUShared(start, end time.Time) *Future[IsGPUSharedResult] {
	m.recordCall(QueryIsGPUShared)
	return m.Querier.QueryIsGPUShared(start, end)
}

// Device

func (m *RecordMetricsQuerier) QueryDCGMDeviceInfo(start, end time.Time) *Future[DCGMDeviceInfoResult] {
	m.recordCall(QueryDCGMDeviceInfo)
	return m.Querier.QueryDCGMDeviceInfo(start, end)
}

func (m *RecordMetricsQuerier) QueryDCGMDeviceUptime(start, end time.Time) *Future[DCGMDeviceUptimeResult] {
	m.recordCall(QueryDCGMDeviceUptime)
	return m.Querier.QueryDCGMDeviceUptime(start, end)
}

func (m *RecordMetricsQuerier) QueryDCGMContainerUsageAvg(start, end time.Time) *Future[DCGMDeviceContainerUsageResult] {
	m.recordCall(QueryDCGMContainerUsageAvg)
	return m.Querier.QueryDCGMContainerUsageAvg(start, end)
}

func (m *RecordMetricsQuerier) QueryDCGMContainerUsageMax(start, end time.Time) *Future[DCGMDeviceContainerUsageResult] {
	m.recordCall(QueryDCGMContainerUsageMax)
	return m.Querier.QueryDCGMContainerUsageMax(start, end)
}

// PVC

func (m *RecordMetricsQuerier) QueryPodPVCAllocation(start, end time.Time) *Future[PodPVCAllocationResult] {
	m.recordCall(QueryPodPVCAllocation)
	return m.Querier.QueryPodPVCAllocation(start, end)
}

func (m *RecordMetricsQuerier) QueryPVCBytesRequested(start, end time.Time) *Future[PVCBytesRequestedResult] {
	m.recordCall(QueryPVCBytesRequested)
	return m.Querier.QueryPVCBytesRequested(start, end)
}

func (m *RecordMetricsQuerier) QueryPVCInfo(start, end time.Time) *Future[PVCInfoResult] {
	m.recordCall(QueryPVCInfo)
	return m.Querier.QueryPVCInfo(start, end)
}

func (m *RecordMetricsQuerier) QueryKMPVCInfo(start, end time.Time) *Future[PVCInfoResult] {
	m.recordCall(QueryKMPVCInfo)
	return m.Querier.QueryKMPVCInfo(start, end)
}

func (m *RecordMetricsQuerier) QueryPVCUptime(start, end time.Time) *Future[UptimeResult] {
	m.recordCall(QueryPVCUptime)
	return m.Querier.QueryPVCUptime(start, end)
}

func (m *RecordMetricsQuerier) QueryPVCBytesUsedAverage(start, end time.Time) *Future[PVCUIDValueResult] {
	m.recordCall(QueryPVCBytesUsedAverage)
	return m.Querier.QueryPVCBytesUsedAverage(start, end)
}

func (m *RecordMetricsQuerier) QueryPVCBytesUsedMax(start, end time.Time) *Future[PVCUIDValueResult] {
	m.recordCall(QueryPVCBytesUsedMax)
	return m.Querier.QueryPVCBytesUsedMax(start, end)
}

// PV

func (m *RecordMetricsQuerier) QueryPVBytes(start, end time.Time) *Future[PVBytesResult] {
	m.recordCall(QueryPVBytes)
	return m.Querier.QueryPVBytes(start, end)
}

func (m *RecordMetricsQuerier) QueryPVPricePerGiBHour(start, end time.Time) *Future[PVPricePerGiBHourResult] {
	m.recordCall(QueryPVPricePerGiBHour)
	return m.Querier.QueryPVPricePerGiBHour(start, end)
}

func (m *RecordMetricsQuerier) QueryPVInfo(start, end time.Time) *Future[PVInfoResult] {
	m.recordCall(QueryPVInfo)
	return m.Querier.QueryPVInfo(start, end)
}

func (m *RecordMetricsQuerier) QueryPVActiveMinutes(start, end time.Time) *Future[PVActiveMinutesResult] {
	m.recordCall(QueryPVActiveMinutes)
	return m.Querier.QueryPVActiveMinutes(start, end)
}

func (m *RecordMetricsQuerier) QueryPVUsedAverage(start, end time.Time) *Future[PVUsedAvgResult] {
	m.recordCall(QueryPVUsedAverage)
	return m.Querier.QueryPVUsedAverage(start, end)
}

func (m *RecordMetricsQuerier) QueryPVUsedMax(start, end time.Time) *Future[PVUsedMaxResult] {
	m.recordCall(QueryPVUsedMax)
	return m.Querier.QueryPVUsedMax(start, end)
}

func (m *RecordMetricsQuerier) QueryKMPVInfo(start, end time.Time) *Future[PVInfoResult] {
	m.recordCall(QueryKMPVInfo)
	return m.Querier.QueryKMPVInfo(start, end)
}

func (m *RecordMetricsQuerier) QueryPVUptime(start, end time.Time) *Future[UptimeResult] {
	m.recordCall(QueryPVUptime)
	return m.Querier.QueryPVUptime(start, end)
}

// Deployment

func (m *RecordMetricsQuerier) QueryDeploymentInfo(start, end time.Time) *Future[DeploymentInfoResult] {
	m.recordCall(QueryDeploymentInfo)
	return m.Querier.QueryDeploymentInfo(start, end)
}

func (m *RecordMetricsQuerier) QueryDeploymentUptime(start, end time.Time) *Future[UptimeResult] {
	m.recordCall(QueryDeploymentUptime)
	return m.Querier.QueryDeploymentUptime(start, end)
}

func (m *RecordMetricsQuerier) QueryDeploymentLabels(start, end time.Time) *Future[LabelsResult] {
	m.recordCall(QueryDeploymentLabels)
	return m.Querier.QueryDeploymentLabels(start, end)
}

func (m *RecordMetricsQuerier) QueryDeploymentAnnotations(start, end time.Time) *Future[AnnotationsResult] {
	m.recordCall(QueryDeploymentAnnotations)
	return m.Querier.QueryDeploymentAnnotations(start, end)
}

func (m *RecordMetricsQuerier) QueryDeploymentMatchLabels(start, end time.Time) *Future[DeploymentLabelsResult] {
	m.recordCall(QueryDeploymentMatchLabels)
	return m.Querier.QueryDeploymentMatchLabels(start, end)
}

// StatefulSet

func (m *RecordMetricsQuerier) QueryStatefulSetInfo(start, end time.Time) *Future[StatefulSetInfoResult] {
	m.recordCall(QueryStatefulSetInfo)
	return m.Querier.QueryStatefulSetInfo(start, end)
}

func (m *RecordMetricsQuerier) QueryStatefulSetUptime(start, end time.Time) *Future[UptimeResult] {
	m.recordCall(QueryStatefulSetUptime)
	return m.Querier.QueryStatefulSetUptime(start, end)
}

func (m *RecordMetricsQuerier) QueryStatefulSetLabels(start, end time.Time) *Future[LabelsResult] {
	m.recordCall(QueryStatefulSetLabels)
	return m.Querier.QueryStatefulSetLabels(start, end)
}

func (m *RecordMetricsQuerier) QueryStatefulSetAnnotations(start, end time.Time) *Future[AnnotationsResult] {
	m.recordCall(QueryStatefulSetAnnotations)
	return m.Querier.QueryStatefulSetAnnotations(start, end)
}

func (m *RecordMetricsQuerier) QueryStatefulSetMatchLabels(start, end time.Time) *Future[StatefulSetLabelsResult] {
	m.recordCall(QueryStatefulSetMatchLabels)
	return m.Querier.QueryStatefulSetMatchLabels(start, end)
}

// DaemonSet

func (m *RecordMetricsQuerier) QueryDaemonSetInfo(start, end time.Time) *Future[DaemonSetInfoResult] {
	m.recordCall(QueryDaemonSetInfo)
	return m.Querier.QueryDaemonSetInfo(start, end)
}

func (m *RecordMetricsQuerier) QueryDaemonSetUptime(start, end time.Time) *Future[UptimeResult] {
	m.recordCall(QueryDaemonSetUptime)
	return m.Querier.QueryDaemonSetUptime(start, end)
}

func (m *RecordMetricsQuerier) QueryDaemonSetLabels(start, end time.Time) *Future[LabelsResult] {
	m.recordCall(QueryDaemonSetLabels)
	return m.Querier.QueryDaemonSetLabels(start, end)
}

func (m *RecordMetricsQuerier) QueryDaemonSetAnnotations(start, end time.Time) *Future[AnnotationsResult] {
	m.recordCall(QueryDaemonSetAnnotations)
	return m.Querier.QueryDaemonSetAnnotations(start, end)
}

// Job

func (m *RecordMetricsQuerier) QueryJobInfo(start, end time.Time) *Future[JobInfoResult] {
	m.recordCall(QueryJobInfo)
	return m.Querier.QueryJobInfo(start, end)
}

func (m *RecordMetricsQuerier) QueryJobUptime(start, end time.Time) *Future[UptimeResult] {
	m.recordCall(QueryJobUptime)
	return m.Querier.QueryJobUptime(start, end)
}

func (m *RecordMetricsQuerier) QueryJobLabels(start, end time.Time) *Future[LabelsResult] {
	m.recordCall(QueryJobLabels)
	return m.Querier.QueryJobLabels(start, end)
}

func (m *RecordMetricsQuerier) QueryJobAnnotations(start, end time.Time) *Future[AnnotationsResult] {
	m.recordCall(QueryJobAnnotations)
	return m.Querier.QueryJobAnnotations(start, end)
}

// CronJob

func (m *RecordMetricsQuerier) QueryCronJobInfo(start, end time.Time) *Future[CronJobInfoResult] {
	m.recordCall(QueryCronJobInfo)
	return m.Querier.QueryCronJobInfo(start, end)
}

func (m *RecordMetricsQuerier) QueryCronJobUptime(start, end time.Time) *Future[UptimeResult] {
	m.recordCall(QueryCronJobUptime)
	return m.Querier.QueryCronJobUptime(start, end)
}

func (m *RecordMetricsQuerier) QueryCronJobLabels(start, end time.Time) *Future[LabelsResult] {
	m.recordCall(QueryCronJobLabels)
	return m.Querier.QueryCronJobLabels(start, end)
}

func (m *RecordMetricsQuerier) QueryCronJobAnnotations(start, end time.Time) *Future[AnnotationsResult] {
	m.recordCall(QueryCronJobAnnotations)
	return m.Querier.QueryCronJobAnnotations(start, end)
}

// ReplicaSet

func (m *RecordMetricsQuerier) QueryReplicaSetInfo(start, end time.Time) *Future[ReplicaSetInfoResult] {
	m.recordCall(QueryReplicaSetInfo)
	return m.Querier.QueryReplicaSetInfo(start, end)
}

func (m *RecordMetricsQuerier) QueryReplicaSetUptime(start, end time.Time) *Future[UptimeResult] {
	m.recordCall(QueryReplicaSetUptime)
	return m.Querier.QueryReplicaSetUptime(start, end)
}

func (m *RecordMetricsQuerier) QueryReplicaSetLabels(start, end time.Time) *Future[LabelsResult] {
	m.recordCall(QueryReplicaSetLabels)
	return m.Querier.QueryReplicaSetLabels(start, end)
}

func (m *RecordMetricsQuerier) QueryReplicaSetAnnotations(start, end time.Time) *Future[AnnotationsResult] {
	m.recordCall(QueryReplicaSetAnnotations)
	return m.Querier.QueryReplicaSetAnnotations(start, end)
}

func (m *RecordMetricsQuerier) QueryReplicaSetOwners(start, end time.Time) *Future[OwnerResult] {
	m.recordCall(QueryReplicaSetOwners)
	return m.Querier.QueryReplicaSetOwners(start, end)
}

// Namespace

func (m *RecordMetricsQuerier) QueryNamespaceInfo(start, end time.Time) *Future[NamespaceInfoResult] {
	m.recordCall(QueryNamespaceInfo)
	return m.Querier.QueryNamespaceInfo(start, end)
}

func (m *RecordMetricsQuerier) QueryNamespaceUptime(start, end time.Time) *Future[UptimeResult] {
	m.recordCall(QueryNamespaceUptime)
	return m.Querier.QueryNamespaceUptime(start, end)
}

// Service

func (m *RecordMetricsQuerier) QueryServiceInfo(start, end time.Time) *Future[ServiceInfoResult] {
	m.recordCall(QueryServiceInfo)
	return m.Querier.QueryServiceInfo(start, end)
}

func (m *RecordMetricsQuerier) QueryServiceUptime(start, end time.Time) *Future[UptimeResult] {
	m.recordCall(QueryServiceUptime)
	return m.Querier.QueryServiceUptime(start, end)
}

func (m *RecordMetricsQuerier) QueryServiceSelectorLabels(start, end time.Time) *Future[ServiceLabelsResult] {
	m.recordCall(QueryServiceSelectorLabels)
	return m.Querier.QueryServiceSelectorLabels(start, end)
}

// Network Egress

func (m *RecordMetricsQuerier) QueryNetZoneGiB(start, end time.Time) *Future[NetZoneGiBResult] {
	m.recordCall(QueryNetZoneGiB)
	return m.Querier.QueryNetZoneGiB(start, end)
}

func (m *RecordMetricsQuerier) QueryNetZonePricePerGiB(start, end time.Time) *Future[NetZonePricePerGiBResult] {
	m.recordCall(QueryNetZonePricePerGiB)
	return m.Querier.QueryNetZonePricePerGiB(start, end)
}

func (m *RecordMetricsQuerier) QueryNetRegionGiB(start, end time.Time) *Future[NetRegionGiBResult] {
	m.recordCall(QueryNetRegionGiB)
	return m.Querier.QueryNetRegionGiB(start, end)
}

func (m *RecordMetricsQuerier) QueryNetRegionPricePerGiB(start, end time.Time) *Future[NetRegionPricePerGiBResult] {
	m.recordCall(QueryNetRegionPricePerGiB)
	return m.Querier.QueryNetRegionPricePerGiB(start, end)
}

func (m *RecordMetricsQuerier) QueryNetInternetGiB(start, end time.Time) *Future[NetInternetGiBResult] {
	m.recordCall(QueryNetInternetGiB)
	return m.Querier.QueryNetInternetGiB(start, end)
}

func (m *RecordMetricsQuerier) QueryNetInternetPricePerGiB(start, end time.Time) *Future[NetInternetPricePerGiBResult] {
	m.recordCall(QueryNetInternetPricePerGiB)
	return m.Querier.QueryNetInternetPricePerGiB(start, end)
}

func (m *RecordMetricsQuerier) QueryNetInternetServiceGiB(start, end time.Time) *Future[NetInternetServiceGiBResult] {
	m.recordCall(QueryNetInternetServiceGiB)
	return m.Querier.QueryNetInternetServiceGiB(start, end)
}

func (m *RecordMetricsQuerier) QueryNetNatGatewayPricePerGiB(start, end time.Time) *Future[NetNatGatewayPricePerGiBResult] {
	m.recordCall(QueryNetNatGatewayPricePerGiB)
	return m.Querier.QueryNetNatGatewayPricePerGiB(start, end)
}

func (m *RecordMetricsQuerier) QueryNetNatGatewayGiB(start, end time.Time) *Future[NetNatGatewayGiBResult] {
	m.recordCall(QueryNetNatGatewayGiB)
	return m.Querier.QueryNetNatGatewayGiB(start, end)
}

func (m *RecordMetricsQuerier) QueryNetTransferBytes(start, end time.Time) *Future[NetTransferBytesResult] {
	m.recordCall(QueryNetTransferBytes)
	return m.Querier.QueryNetTransferBytes(start, end)
}

// Network Ingress

func (m *RecordMetricsQuerier) QueryNetZoneIngressGiB(start, end time.Time) *Future[NetZoneIngressGiBResult] {
	m.recordCall(QueryNetZoneIngressGiB)
	return m.Querier.QueryNetZoneIngressGiB(start, end)
}

func (m *RecordMetricsQuerier) QueryNetRegionIngressGiB(start, end time.Time) *Future[NetRegionIngressGiBResult] {
	m.recordCall(QueryNetRegionIngressGiB)
	return m.Querier.QueryNetRegionIngressGiB(start, end)
}

func (m *RecordMetricsQuerier) QueryNetInternetIngressGiB(start, end time.Time) *Future[NetInternetIngressGiBResult] {
	m.recordCall(QueryNetInternetIngressGiB)
	return m.Querier.QueryNetInternetIngressGiB(start, end)
}

func (m *RecordMetricsQuerier) QueryNetInternetServiceIngressGiB(start, end time.Time) *Future[NetInternetServiceIngressGiBResult] {
	m.recordCall(QueryNetInternetServiceIngressGiB)
	return m.Querier.QueryNetInternetServiceIngressGiB(start, end)
}

func (m *RecordMetricsQuerier) QueryNetNatGatewayIngressPricePerGiB(start, end time.Time) *Future[NetNatGatewayPricePerGiBResult] {
	m.recordCall(QueryNetNatGatewayIngressPricePerGiB)
	return m.Querier.QueryNetNatGatewayIngressPricePerGiB(start, end)
}

func (m *RecordMetricsQuerier) QueryNetNatGatewayIngressGiB(start, end time.Time) *Future[NetNatGatewayIngressGiBResult] {
	m.recordCall(QueryNetNatGatewayIngressGiB)
	return m.Querier.QueryNetNatGatewayIngressGiB(start, end)
}

func (m *RecordMetricsQuerier) QueryNetReceiveBytes(start, end time.Time) *Future[NetReceiveBytesResult] {
	m.recordCall(QueryNetReceiveBytes)
	return m.Querier.QueryNetReceiveBytes(start, end)
}

// Annotations

func (m *RecordMetricsQuerier) QueryNamespaceAnnotations(start, end time.Time) *Future[NamespaceAnnotationsResult] {
	m.recordCall(QueryNamespaceAnnotations)
	return m.Querier.QueryNamespaceAnnotations(start, end)
}

func (m *RecordMetricsQuerier) QueryPodAnnotations(start, end time.Time) *Future[PodAnnotationsResult] {
	m.recordCall(QueryPodAnnotations)
	return m.Querier.QueryPodAnnotations(start, end)
}

// Labels

func (m *RecordMetricsQuerier) QueryNodeLabels(start, end time.Time) *Future[NodeLabelsResult] {
	m.recordCall(QueryNodeLabels)
	return m.Querier.QueryNodeLabels(start, end)
}

func (m *RecordMetricsQuerier) QueryNamespaceLabels(start, end time.Time) *Future[NamespaceLabelsResult] {
	m.recordCall(QueryNamespaceLabels)
	return m.Querier.QueryNamespaceLabels(start, end)
}

func (m *RecordMetricsQuerier) QueryPodLabels(start, end time.Time) *Future[PodLabelsResult] {
	m.recordCall(QueryPodLabels)
	return m.Querier.QueryPodLabels(start, end)
}

func (m *RecordMetricsQuerier) QueryPodsWithDaemonSetOwner(start, end time.Time) *Future[PodsWithDaemonSetOwnerResult] {
	m.recordCall(QueryPodsWithDaemonSetOwner)
	return m.Querier.QueryPodsWithDaemonSetOwner(start, end)
}

func (m *RecordMetricsQuerier) QueryPodsWithJobOwner(start, end time.Time) *Future[PodsWithJobOwnerResult] {
	m.recordCall(QueryPodsWithJobOwner)
	return m.Querier.QueryPodsWithJobOwner(start, end)
}

// ReplicaSet -> Controller mapping

func (m *RecordMetricsQuerier) QueryPodsWithReplicaSetOwner(start, end time.Time) *Future[PodsWithReplicaSetOwnerResult] {
	m.recordCall(QueryPodsWithReplicaSetOwner)
	return m.Querier.QueryPodsWithReplicaSetOwner(start, end)
}

func (m *RecordMetricsQuerier) QueryReplicaSetsWithoutOwners(start, end time.Time) *Future[ReplicaSetsWithoutOwnersResult] {
	m.recordCall(QueryReplicaSetsWithoutOwners)
	return m.Querier.QueryReplicaSetsWithoutOwners(start, end)
}

func (m *RecordMetricsQuerier) QueryReplicaSetsWithRollout(start, end time.Time) *Future[ReplicaSetsWithRolloutResult] {
	m.recordCall(QueryReplicaSetsWithRollout)
	return m.Querier.QueryReplicaSetsWithRollout(start, end)
}

// ResourceQuotas

func (m *RecordMetricsQuerier) QueryResourceQuotaInfo(start, end time.Time) *Future[ResourceQuotaInfoResult] {
	m.recordCall(QueryResourceQuotaInfo)
	return m.Querier.QueryResourceQuotaInfo(start, end)
}

func (m *RecordMetricsQuerier) QueryResourceQuotaUptime(start, end time.Time) *Future[UptimeResult] {
	m.recordCall(QueryResourceQuotaUptime)
	return m.Querier.QueryResourceQuotaUptime(start, end)
}

func (m *RecordMetricsQuerier) QueryResourceQuotaSpecCPURequestAverage(start, end time.Time) *Future[ResourceResult] {
	m.recordCall(QueryResourceQuotaSpecCPURequestAverage)
	return m.Querier.QueryResourceQuotaSpecCPURequestAverage(start, end)
}

func (m *RecordMetricsQuerier) QueryResourceQuotaSpecCPURequestMax(start, end time.Time) *Future[ResourceResult] {
	m.recordCall(QueryResourceQuotaSpecCPURequestMax)
	return m.Querier.QueryResourceQuotaSpecCPURequestMax(start, end)
}

func (m *RecordMetricsQuerier) QueryResourceQuotaSpecRAMRequestAverage(start, end time.Time) *Future[ResourceResult] {
	m.recordCall(QueryResourceQuotaSpecRAMRequestAverage)
	return m.Querier.QueryResourceQuotaSpecRAMRequestAverage(start, end)
}

func (m *RecordMetricsQuerier) QueryResourceQuotaSpecRAMRequestMax(start, end time.Time) *Future[ResourceResult] {
	m.recordCall(QueryResourceQuotaSpecRAMRequestMax)
	return m.Querier.QueryResourceQuotaSpecRAMRequestMax(start, end)
}

func (m *RecordMetricsQuerier) QueryResourceQuotaSpecCPULimitAverage(start, end time.Time) *Future[ResourceResult] {
	m.recordCall(QueryResourceQuotaSpecCPULimitAverage)
	return m.Querier.QueryResourceQuotaSpecCPULimitAverage(start, end)
}

func (m *RecordMetricsQuerier) QueryResourceQuotaSpecCPULimitMax(start, end time.Time) *Future[ResourceResult] {
	m.recordCall(QueryResourceQuotaSpecCPULimitMax)
	return m.Querier.QueryResourceQuotaSpecCPULimitMax(start, end)
}

func (m *RecordMetricsQuerier) QueryResourceQuotaSpecRAMLimitAverage(start, end time.Time) *Future[ResourceResult] {
	m.recordCall(QueryResourceQuotaSpecRAMLimitAverage)
	return m.Querier.QueryResourceQuotaSpecRAMLimitAverage(start, end)
}

func (m *RecordMetricsQuerier) QueryResourceQuotaSpecRAMLimitMax(start, end time.Time) *Future[ResourceResult] {
	m.recordCall(QueryResourceQuotaSpecRAMLimitMax)
	return m.Querier.QueryResourceQuotaSpecRAMLimitMax(start, end)
}

func (m *RecordMetricsQuerier) QueryResourceQuotaStatusUsedCPURequestAverage(start, end time.Time) *Future[ResourceResult] {
	m.recordCall(QueryResourceQuotaStatusUsedCPURequestAverage)
	return m.Querier.QueryResourceQuotaStatusUsedCPURequestAverage(start, end)
}

func (m *RecordMetricsQuerier) QueryResourceQuotaStatusUsedCPURequestMax(start, end time.Time) *Future[ResourceResult] {
	m.recordCall(QueryResourceQuotaStatusUsedCPURequestMax)
	return m.Querier.QueryResourceQuotaStatusUsedCPURequestMax(start, end)
}

func (m *RecordMetricsQuerier) QueryResourceQuotaStatusUsedRAMRequestAverage(start, end time.Time) *Future[ResourceResult] {
	m.recordCall(QueryResourceQuotaStatusUsedRAMRequestAverage)
	return m.Querier.QueryResourceQuotaStatusUsedRAMRequestAverage(start, end)
}

func (m *RecordMetricsQuerier) QueryResourceQuotaStatusUsedRAMRequestMax(start, end time.Time) *Future[ResourceResult] {
	m.recordCall(QueryResourceQuotaStatusUsedRAMRequestMax)
	return m.Querier.QueryResourceQuotaStatusUsedRAMRequestMax(start, end)
}

func (m *RecordMetricsQuerier) QueryResourceQuotaStatusUsedCPULimitAverage(start, end time.Time) *Future[ResourceResult] {
	m.recordCall(QueryResourceQuotaStatusUsedCPULimitAverage)
	return m.Querier.QueryResourceQuotaStatusUsedCPULimitAverage(start, end)
}

func (m *RecordMetricsQuerier) QueryResourceQuotaStatusUsedCPULimitMax(start, end time.Time) *Future[ResourceResult] {
	m.recordCall(QueryResourceQuotaStatusUsedCPULimitMax)
	return m.Querier.QueryResourceQuotaStatusUsedCPULimitMax(start, end)
}

func (m *RecordMetricsQuerier) QueryResourceQuotaStatusUsedRAMLimitAverage(start, end time.Time) *Future[ResourceResult] {
	m.recordCall(QueryResourceQuotaStatusUsedRAMLimitAverage)
	return m.Querier.QueryResourceQuotaStatusUsedRAMLimitAverage(start, end)
}

func (m *RecordMetricsQuerier) QueryResourceQuotaStatusUsedRAMLimitMax(start, end time.Time) *Future[ResourceResult] {
	m.recordCall(QueryResourceQuotaStatusUsedRAMLimitMax)
	return m.Querier.QueryResourceQuotaStatusUsedRAMLimitMax(start, end)
}

// Data Coverage Query

func (m *RecordMetricsQuerier) QueryDataCoverage(limitDays int) (time.Time, time.Time, error) {
	m.recordCall(QueryDataCoverage)
	return m.Querier.QueryDataCoverage(limitDays)
}
