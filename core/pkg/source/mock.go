package source

import (
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/diagnostics"
)

//--------------------------------------------------------------------------
//  Mock MetricsQuerier (per-method override map, NoOp fallback)
//--------------------------------------------------------------------------

var _ MetricsQuerier = (*MockMetricsQuerier)(nil)

// MockMetricsQuerier is a test double for MetricsQuerier. Set a field to a
// non-nil *Future[T] to override what a specific method returns; unset methods
// fall back to NoOpMetricsQuerier (empty results, no error).
type MockMetricsQuerier struct {
	noop      *NoOpMetricsQuerier
	overrides map[string]any
}

func NewMockMetricsQuerier() *MockMetricsQuerier {
	return &MockMetricsQuerier{
		noop:      NewNoOpMetricsQuerier(),
		overrides: make(map[string]any),
	}
}

// SetOverride registers a *Future[T] to be returned when method is called.
// Pass nil to clear an override.
func (m *MockMetricsQuerier) SetOverride(method string, result any) {
	if result == nil {
		delete(m.overrides, method)
		return
	}
	m.overrides[method] = result
}

// resolve returns the override for method if one is set and type-asserts correctly,
// otherwise calls fallback.
func resolve[T any](overrides map[string]any, method string, fallback func() *Future[T]) *Future[T] {
	if v, ok := overrides[method]; ok {
		if f, ok := v.(*Future[T]); ok {
			return f
		}
	}
	return fallback()
}

// Local Cluster Disks

func (m *MockMetricsQuerier) QueryLocalStorageActiveMinutes(start, end time.Time) *Future[LocalStorageActiveMinutesResult] {
	return resolve(m.overrides, QueryLocalStorageActiveMinutes, func() *Future[LocalStorageActiveMinutesResult] {
		return m.noop.QueryLocalStorageActiveMinutes(start, end)
	})
}

func (m *MockMetricsQuerier) QueryLocalStorageUsedAvg(start, end time.Time) *Future[LocalStorageUsedAvgResult] {
	return resolve(m.overrides, QueryLocalStorageUsedAvg, func() *Future[LocalStorageUsedAvgResult] {
		return m.noop.QueryLocalStorageUsedAvg(start, end)
	})
}

func (m *MockMetricsQuerier) QueryLocalStorageUsedMax(start, end time.Time) *Future[LocalStorageUsedMaxResult] {
	return resolve(m.overrides, QueryLocalStorageUsedMax, func() *Future[LocalStorageUsedMaxResult] {
		return m.noop.QueryLocalStorageUsedMax(start, end)
	})
}

func (m *MockMetricsQuerier) QueryLocalStorageBytes(start, end time.Time) *Future[LocalStorageBytesResult] {
	return resolve(m.overrides, QueryLocalStorageBytes, func() *Future[LocalStorageBytesResult] {
		return m.noop.QueryLocalStorageBytes(start, end)
	})
}

func (m *MockMetricsQuerier) QueryKMLocalStorageUsedAvg(start, end time.Time) *Future[NodeUIDValueResult] {
	return resolve(m.overrides, QueryKMLocalStorageUsedAvg, func() *Future[NodeUIDValueResult] {
		return m.noop.QueryKMLocalStorageUsedAvg(start, end)
	})
}

func (m *MockMetricsQuerier) QueryKMLocalStorageUsedMax(start, end time.Time) *Future[NodeUIDValueResult] {
	return resolve(m.overrides, QueryKMLocalStorageUsedMax, func() *Future[NodeUIDValueResult] {
		return m.noop.QueryKMLocalStorageUsedMax(start, end)
	})
}

func (m *MockMetricsQuerier) QueryKMLocalStorageBytes(start, end time.Time) *Future[UIDValueResult] {
	return resolve(m.overrides, QueryKMLocalStorageBytes, func() *Future[UIDValueResult] {
		return m.noop.QueryKMLocalStorageBytes(start, end)
	})
}

// Nodes

func (m *MockMetricsQuerier) QueryNodeInfo(start, end time.Time) *Future[NodeInfoResult] {
	return resolve(m.overrides, QueryNodeInfo, func() *Future[NodeInfoResult] {
		return m.noop.QueryNodeInfo(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNodeUptime(start, end time.Time) *Future[UptimeResult] {
	return resolve(m.overrides, QueryNodeUptime, func() *Future[UptimeResult] {
		return m.noop.QueryNodeUptime(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNodeActiveMinutes(start, end time.Time) *Future[NodeActiveMinutesResult] {
	return resolve(m.overrides, QueryNodeActiveMinutes, func() *Future[NodeActiveMinutesResult] {
		return m.noop.QueryNodeActiveMinutes(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNodeCPUCoresCapacity(start, end time.Time) *Future[NodeCPUCoresCapacityResult] {
	return resolve(m.overrides, QueryNodeCPUCoresCapacity, func() *Future[NodeCPUCoresCapacityResult] {
		return m.noop.QueryNodeCPUCoresCapacity(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNodeCPUCoresAllocatable(start, end time.Time) *Future[NodeCPUCoresAllocatableResult] {
	return resolve(m.overrides, QueryNodeCPUCoresAllocatable, func() *Future[NodeCPUCoresAllocatableResult] {
		return m.noop.QueryNodeCPUCoresAllocatable(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNodeRAMBytesCapacity(start, end time.Time) *Future[NodeRAMBytesCapacityResult] {
	return resolve(m.overrides, QueryNodeRAMBytesCapacity, func() *Future[NodeRAMBytesCapacityResult] {
		return m.noop.QueryNodeRAMBytesCapacity(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNodeRAMBytesAllocatable(start, end time.Time) *Future[NodeRAMBytesAllocatableResult] {
	return resolve(m.overrides, QueryNodeRAMBytesAllocatable, func() *Future[NodeRAMBytesAllocatableResult] {
		return m.noop.QueryNodeRAMBytesAllocatable(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNodeGPUCount(start, end time.Time) *Future[NodeGPUCountResult] {
	return resolve(m.overrides, QueryNodeGPUCount, func() *Future[NodeGPUCountResult] {
		return m.noop.QueryNodeGPUCount(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNodeCPUModeTotal(start, end time.Time) *Future[NodeCPUModeTotalResult] {
	return resolve(m.overrides, QueryNodeCPUModeTotal, func() *Future[NodeCPUModeTotalResult] {
		return m.noop.QueryNodeCPUModeTotal(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNodeIsSpot(start, end time.Time) *Future[NodeIsSpotResult] {
	return resolve(m.overrides, QueryNodeIsSpot, func() *Future[NodeIsSpotResult] {
		return m.noop.QueryNodeIsSpot(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNodeRAMSystemPercent(start, end time.Time) *Future[NodeRAMSystemPercentResult] {
	return resolve(m.overrides, QueryNodeRAMSystemPercent, func() *Future[NodeRAMSystemPercentResult] {
		return m.noop.QueryNodeRAMSystemPercent(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNodeRAMUserPercent(start, end time.Time) *Future[NodeRAMUserPercentResult] {
	return resolve(m.overrides, QueryNodeRAMUserPercent, func() *Future[NodeRAMUserPercentResult] {
		return m.noop.QueryNodeRAMUserPercent(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNodeResourceCapacities(start, end time.Time) *Future[ResourceResult] {
	return resolve(m.overrides, QueryNodeResourceCapacities, func() *Future[ResourceResult] {
		return m.noop.QueryNodeResourceCapacities(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNodeResourcesAllocatable(start, end time.Time) *Future[ResourceResult] {
	return resolve(m.overrides, QueryNodeResourcesAllocatable, func() *Future[ResourceResult] {
		return m.noop.QueryNodeResourcesAllocatable(start, end)
	})
}

// Load Balancers

func (m *MockMetricsQuerier) QueryLBActiveMinutes(start, end time.Time) *Future[LBActiveMinutesResult] {
	return resolve(m.overrides, QueryLBActiveMinutes, func() *Future[LBActiveMinutesResult] {
		return m.noop.QueryLBActiveMinutes(start, end)
	})
}

func (m *MockMetricsQuerier) QueryLBPricePerHr(start, end time.Time) *Future[LBPricePerHrResult] {
	return resolve(m.overrides, QueryLBPricePerHr, func() *Future[LBPricePerHrResult] {
		return m.noop.QueryLBPricePerHr(start, end)
	})
}

// Cluster Management

func (m *MockMetricsQuerier) QueryClusterInfo(start, end time.Time) *Future[ClusterInfoResult] {
	return resolve(m.overrides, QueryClusterInfo, func() *Future[ClusterInfoResult] {
		return m.noop.QueryClusterInfo(start, end)
	})
}

func (m *MockMetricsQuerier) QueryClusterUptime(start, end time.Time) *Future[UptimeResult] {
	return resolve(m.overrides, QueryClusterUptime, func() *Future[UptimeResult] {
		return m.noop.QueryClusterUptime(start, end)
	})
}

func (m *MockMetricsQuerier) QueryClusterManagementDuration(start, end time.Time) *Future[ClusterManagementDurationResult] {
	return resolve(m.overrides, QueryClusterManagementDuration, func() *Future[ClusterManagementDurationResult] {
		return m.noop.QueryClusterManagementDuration(start, end)
	})
}

func (m *MockMetricsQuerier) QueryClusterManagementPricePerHr(start, end time.Time) *Future[ClusterManagementPricePerHrResult] {
	return resolve(m.overrides, QueryClusterManagementPricePerHr, func() *Future[ClusterManagementPricePerHrResult] {
		return m.noop.QueryClusterManagementPricePerHr(start, end)
	})
}

// Pods

func (m *MockMetricsQuerier) QueryPods(start, end time.Time) *Future[PodsResult] {
	return resolve(m.overrides, QueryPods, func() *Future[PodsResult] {
		return m.noop.QueryPods(start, end)
	})
}

func (m *MockMetricsQuerier) QueryPodsUID(start, end time.Time) *Future[PodsResult] {
	return resolve(m.overrides, QueryPodsUID, func() *Future[PodsResult] {
		return m.noop.QueryPodsUID(start, end)
	})
}

func (m *MockMetricsQuerier) QueryPodInfo(start, end time.Time) *Future[PodInfoResult] {
	return resolve(m.overrides, QueryPodInfo, func() *Future[PodInfoResult] {
		return m.noop.QueryPodInfo(start, end)
	})
}

func (m *MockMetricsQuerier) QueryPodUptime(start, end time.Time) *Future[UptimeResult] {
	return resolve(m.overrides, QueryPodUptime, func() *Future[UptimeResult] {
		return m.noop.QueryPodUptime(start, end)
	})
}

func (m *MockMetricsQuerier) QueryPodOwners(start, end time.Time) *Future[OwnerResult] {
	return resolve(m.overrides, QueryPodOwners, func() *Future[OwnerResult] {
		return m.noop.QueryPodOwners(start, end)
	})
}

func (m *MockMetricsQuerier) QueryPodPVCVolumes(start, end time.Time) *Future[PodPVCVolumeResult] {
	return resolve(m.overrides, QueryPodPVCVolumes, func() *Future[PodPVCVolumeResult] {
		return m.noop.QueryPodPVCVolumes(start, end)
	})
}

func (m *MockMetricsQuerier) QueryPodNetworkEgressBytes(start, end time.Time) *Future[PodNetworkBytesResult] {
	return resolve(m.overrides, QueryPodNetworkEgressBytes, func() *Future[PodNetworkBytesResult] {
		return m.noop.QueryPodNetworkEgressBytes(start, end)
	})
}

func (m *MockMetricsQuerier) QueryPodNetworkIngressBytes(start, end time.Time) *Future[PodNetworkBytesResult] {
	return resolve(m.overrides, QueryPodNetworkIngressBytes, func() *Future[PodNetworkBytesResult] {
		return m.noop.QueryPodNetworkIngressBytes(start, end)
	})
}

// Container

func (m *MockMetricsQuerier) QueryContainerUptime(start, end time.Time) *Future[ContainerUptimeResult] {
	return resolve(m.overrides, QueryContainerUptime, func() *Future[ContainerUptimeResult] {
		return m.noop.QueryContainerUptime(start, end)
	})
}

func (m *MockMetricsQuerier) QueryContainerResourceRequests(start, end time.Time) *Future[ContainerResourceResult] {
	return resolve(m.overrides, QueryContainerResourceRequests, func() *Future[ContainerResourceResult] {
		return m.noop.QueryContainerResourceRequests(start, end)
	})
}

func (m *MockMetricsQuerier) QueryContainerResourceLimits(start, end time.Time) *Future[ContainerResourceResult] {
	return resolve(m.overrides, QueryContainerResourceLimits, func() *Future[ContainerResourceResult] {
		return m.noop.QueryContainerResourceLimits(start, end)
	})
}

// RAM

func (m *MockMetricsQuerier) QueryRAMBytesAllocated(start, end time.Time) *Future[RAMBytesAllocatedResult] {
	return resolve(m.overrides, QueryRAMBytesAllocated, func() *Future[RAMBytesAllocatedResult] {
		return m.noop.QueryRAMBytesAllocated(start, end)
	})
}

func (m *MockMetricsQuerier) QueryRAMRequests(start, end time.Time) *Future[RAMRequestsResult] {
	return resolve(m.overrides, QueryRAMRequests, func() *Future[RAMRequestsResult] {
		return m.noop.QueryRAMRequests(start, end)
	})
}

func (m *MockMetricsQuerier) QueryRAMLimits(start, end time.Time) *Future[RAMLimitsResult] {
	return resolve(m.overrides, QueryRAMLimits, func() *Future[RAMLimitsResult] {
		return m.noop.QueryRAMLimits(start, end)
	})
}

func (m *MockMetricsQuerier) QueryRAMUsageAvg(start, end time.Time) *Future[RAMUsageAvgResult] {
	return resolve(m.overrides, QueryRAMUsageAvg, func() *Future[RAMUsageAvgResult] {
		return m.noop.QueryRAMUsageAvg(start, end)
	})
}

func (m *MockMetricsQuerier) QueryRAMUsageMax(start, end time.Time) *Future[RAMUsageMaxResult] {
	return resolve(m.overrides, QueryRAMUsageMax, func() *Future[RAMUsageMaxResult] {
		return m.noop.QueryRAMUsageMax(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNodeRAMPricePerGiBHr(start, end time.Time) *Future[NodeRAMPricePerGiBHrResult] {
	return resolve(m.overrides, QueryNodeRAMPricePerGiBHr, func() *Future[NodeRAMPricePerGiBHrResult] {
		return m.noop.QueryNodeRAMPricePerGiBHr(start, end)
	})
}

// CPU

func (m *MockMetricsQuerier) QueryCPUCoresAllocated(start, end time.Time) *Future[CPUCoresAllocatedResult] {
	return resolve(m.overrides, QueryCPUCoresAllocated, func() *Future[CPUCoresAllocatedResult] {
		return m.noop.QueryCPUCoresAllocated(start, end)
	})
}

func (m *MockMetricsQuerier) QueryCPURequests(start, end time.Time) *Future[CPURequestsResult] {
	return resolve(m.overrides, QueryCPURequests, func() *Future[CPURequestsResult] {
		return m.noop.QueryCPURequests(start, end)
	})
}

func (m *MockMetricsQuerier) QueryCPULimits(start, end time.Time) *Future[CPULimitsResult] {
	return resolve(m.overrides, QueryCPULimits, func() *Future[CPULimitsResult] {
		return m.noop.QueryCPULimits(start, end)
	})
}

func (m *MockMetricsQuerier) QueryCPUUsageAvg(start, end time.Time) *Future[CPUUsageAvgResult] {
	return resolve(m.overrides, QueryCPUUsageAvg, func() *Future[CPUUsageAvgResult] {
		return m.noop.QueryCPUUsageAvg(start, end)
	})
}

func (m *MockMetricsQuerier) QueryCPUUsageMax(start, end time.Time) *Future[CPUUsageMaxResult] {
	return resolve(m.overrides, QueryCPUUsageMax, func() *Future[CPUUsageMaxResult] {
		return m.noop.QueryCPUUsageMax(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNodeCPUPricePerHr(start, end time.Time) *Future[NodeCPUPricePerHrResult] {
	return resolve(m.overrides, QueryNodeCPUPricePerHr, func() *Future[NodeCPUPricePerHrResult] {
		return m.noop.QueryNodeCPUPricePerHr(start, end)
	})
}

// GPU

func (m *MockMetricsQuerier) QueryGPUsAllocated(start, end time.Time) *Future[GPUsAllocatedResult] {
	return resolve(m.overrides, QueryGPUsAllocated, func() *Future[GPUsAllocatedResult] {
		return m.noop.QueryGPUsAllocated(start, end)
	})
}

func (m *MockMetricsQuerier) QueryGPUsRequested(start, end time.Time) *Future[GPUsRequestedResult] {
	return resolve(m.overrides, QueryGPUsRequested, func() *Future[GPUsRequestedResult] {
		return m.noop.QueryGPUsRequested(start, end)
	})
}

func (m *MockMetricsQuerier) QueryGPUsUsageAvg(start, end time.Time) *Future[GPUsUsageAvgResult] {
	return resolve(m.overrides, QueryGPUsUsageAvg, func() *Future[GPUsUsageAvgResult] {
		return m.noop.QueryGPUsUsageAvg(start, end)
	})
}

func (m *MockMetricsQuerier) QueryGPUsUsageMax(start, end time.Time) *Future[GPUsUsageMaxResult] {
	return resolve(m.overrides, QueryGPUsUsageMax, func() *Future[GPUsUsageMaxResult] {
		return m.noop.QueryGPUsUsageMax(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNodeGPUPricePerHr(start, end time.Time) *Future[NodeGPUPricePerHrResult] {
	return resolve(m.overrides, QueryNodeGPUPricePerHr, func() *Future[NodeGPUPricePerHrResult] {
		return m.noop.QueryNodeGPUPricePerHr(start, end)
	})
}

func (m *MockMetricsQuerier) QueryGPUInfo(start, end time.Time) *Future[GPUInfoResult] {
	return resolve(m.overrides, QueryGPUInfo, func() *Future[GPUInfoResult] {
		return m.noop.QueryGPUInfo(start, end)
	})
}

func (m *MockMetricsQuerier) QueryIsGPUShared(start, end time.Time) *Future[IsGPUSharedResult] {
	return resolve(m.overrides, QueryIsGPUShared, func() *Future[IsGPUSharedResult] {
		return m.noop.QueryIsGPUShared(start, end)
	})
}

// Device

func (m *MockMetricsQuerier) QueryDCGMDeviceInfo(start, end time.Time) *Future[DCGMDeviceInfoResult] {
	return resolve(m.overrides, QueryDCGMDeviceInfo, func() *Future[DCGMDeviceInfoResult] {
		return m.noop.QueryDCGMDeviceInfo(start, end)
	})
}

func (m *MockMetricsQuerier) QueryDCGMDeviceUptime(start, end time.Time) *Future[DCGMDeviceUptimeResult] {
	return resolve(m.overrides, QueryDCGMDeviceUptime, func() *Future[DCGMDeviceUptimeResult] {
		return m.noop.QueryDCGMDeviceUptime(start, end)
	})
}

func (m *MockMetricsQuerier) QueryDCGMContainerUsageAvg(start, end time.Time) *Future[DCGMDeviceContainerUsageResult] {
	return resolve(m.overrides, QueryDCGMContainerUsageAvg, func() *Future[DCGMDeviceContainerUsageResult] {
		return m.noop.QueryDCGMContainerUsageAvg(start, end)
	})
}

func (m *MockMetricsQuerier) QueryDCGMContainerUsageMax(start, end time.Time) *Future[DCGMDeviceContainerUsageResult] {
	return resolve(m.overrides, QueryDCGMContainerUsageMax, func() *Future[DCGMDeviceContainerUsageResult] {
		return m.noop.QueryDCGMContainerUsageMax(start, end)
	})
}

// PVC

func (m *MockMetricsQuerier) QueryPodPVCAllocation(start, end time.Time) *Future[PodPVCAllocationResult] {
	return resolve(m.overrides, QueryPodPVCAllocation, func() *Future[PodPVCAllocationResult] {
		return m.noop.QueryPodPVCAllocation(start, end)
	})
}

func (m *MockMetricsQuerier) QueryPVCBytesRequested(start, end time.Time) *Future[PVCBytesRequestedResult] {
	return resolve(m.overrides, QueryPVCBytesRequested, func() *Future[PVCBytesRequestedResult] {
		return m.noop.QueryPVCBytesRequested(start, end)
	})
}

func (m *MockMetricsQuerier) QueryPVCInfo(start, end time.Time) *Future[PVCInfoResult] {
	return resolve(m.overrides, QueryPVCInfo, func() *Future[PVCInfoResult] {
		return m.noop.QueryPVCInfo(start, end)
	})
}

func (m *MockMetricsQuerier) QueryKMPVCInfo(start, end time.Time) *Future[PVCInfoResult] {
	return resolve(m.overrides, QueryKMPVCInfo, func() *Future[PVCInfoResult] {
		return m.noop.QueryKMPVCInfo(start, end)
	})
}

func (m *MockMetricsQuerier) QueryPVCUptime(start, end time.Time) *Future[UptimeResult] {
	return resolve(m.overrides, QueryPVCUptime, func() *Future[UptimeResult] {
		return m.noop.QueryPVCUptime(start, end)
	})
}

func (m *MockMetricsQuerier) QueryPVCBytesUsedAverage(start, end time.Time) *Future[PVCUIDValueResult] {
	return resolve(m.overrides, QueryPVCBytesUsedAverage, func() *Future[PVCUIDValueResult] {
		return m.noop.QueryPVCBytesUsedAverage(start, end)
	})
}

func (m *MockMetricsQuerier) QueryPVCBytesUsedMax(start, end time.Time) *Future[PVCUIDValueResult] {
	return resolve(m.overrides, QueryPVCBytesUsedMax, func() *Future[PVCUIDValueResult] {
		return m.noop.QueryPVCBytesUsedMax(start, end)
	})
}

// PV

func (m *MockMetricsQuerier) QueryPVBytes(start, end time.Time) *Future[PVBytesResult] {
	return resolve(m.overrides, QueryPVBytes, func() *Future[PVBytesResult] {
		return m.noop.QueryPVBytes(start, end)
	})
}

func (m *MockMetricsQuerier) QueryPVPricePerGiBHour(start, end time.Time) *Future[PVPricePerGiBHourResult] {
	return resolve(m.overrides, QueryPVPricePerGiBHour, func() *Future[PVPricePerGiBHourResult] {
		return m.noop.QueryPVPricePerGiBHour(start, end)
	})
}

func (m *MockMetricsQuerier) QueryPVInfo(start, end time.Time) *Future[PVInfoResult] {
	return resolve(m.overrides, QueryPVInfo, func() *Future[PVInfoResult] {
		return m.noop.QueryPVInfo(start, end)
	})
}

func (m *MockMetricsQuerier) QueryPVActiveMinutes(start, end time.Time) *Future[PVActiveMinutesResult] {
	return resolve(m.overrides, QueryPVActiveMinutes, func() *Future[PVActiveMinutesResult] {
		return m.noop.QueryPVActiveMinutes(start, end)
	})
}

func (m *MockMetricsQuerier) QueryPVUsedAverage(start, end time.Time) *Future[PVUsedAvgResult] {
	return resolve(m.overrides, QueryPVUsedAverage, func() *Future[PVUsedAvgResult] {
		return m.noop.QueryPVUsedAverage(start, end)
	})
}

func (m *MockMetricsQuerier) QueryPVUsedMax(start, end time.Time) *Future[PVUsedMaxResult] {
	return resolve(m.overrides, QueryPVUsedMax, func() *Future[PVUsedMaxResult] {
		return m.noop.QueryPVUsedMax(start, end)
	})
}

func (m *MockMetricsQuerier) QueryKMPVInfo(start, end time.Time) *Future[PVInfoResult] {
	return resolve(m.overrides, QueryKMPVInfo, func() *Future[PVInfoResult] {
		return m.noop.QueryKMPVInfo(start, end)
	})
}

func (m *MockMetricsQuerier) QueryPVUptime(start, end time.Time) *Future[UptimeResult] {
	return resolve(m.overrides, QueryPVUptime, func() *Future[UptimeResult] {
		return m.noop.QueryPVUptime(start, end)
	})
}

// Deployment

func (m *MockMetricsQuerier) QueryDeploymentInfo(start, end time.Time) *Future[DeploymentInfoResult] {
	return resolve(m.overrides, QueryDeploymentInfo, func() *Future[DeploymentInfoResult] {
		return m.noop.QueryDeploymentInfo(start, end)
	})
}

func (m *MockMetricsQuerier) QueryDeploymentUptime(start, end time.Time) *Future[UptimeResult] {
	return resolve(m.overrides, QueryDeploymentUptime, func() *Future[UptimeResult] {
		return m.noop.QueryDeploymentUptime(start, end)
	})
}

func (m *MockMetricsQuerier) QueryDeploymentLabels(start, end time.Time) *Future[LabelsResult] {
	return resolve(m.overrides, QueryDeploymentLabels, func() *Future[LabelsResult] {
		return m.noop.QueryDeploymentLabels(start, end)
	})
}

func (m *MockMetricsQuerier) QueryDeploymentAnnotations(start, end time.Time) *Future[AnnotationsResult] {
	return resolve(m.overrides, QueryDeploymentAnnotations, func() *Future[AnnotationsResult] {
		return m.noop.QueryDeploymentAnnotations(start, end)
	})
}

func (m *MockMetricsQuerier) QueryDeploymentMatchLabels(start, end time.Time) *Future[DeploymentLabelsResult] {
	return resolve(m.overrides, QueryDeploymentMatchLabels, func() *Future[DeploymentLabelsResult] {
		return m.noop.QueryDeploymentMatchLabels(start, end)
	})
}

// StatefulSet

func (m *MockMetricsQuerier) QueryStatefulSetInfo(start, end time.Time) *Future[StatefulSetInfoResult] {
	return resolve(m.overrides, QueryStatefulSetInfo, func() *Future[StatefulSetInfoResult] {
		return m.noop.QueryStatefulSetInfo(start, end)
	})
}

func (m *MockMetricsQuerier) QueryStatefulSetUptime(start, end time.Time) *Future[UptimeResult] {
	return resolve(m.overrides, QueryStatefulSetUptime, func() *Future[UptimeResult] {
		return m.noop.QueryStatefulSetUptime(start, end)
	})
}

func (m *MockMetricsQuerier) QueryStatefulSetLabels(start, end time.Time) *Future[LabelsResult] {
	return resolve(m.overrides, QueryStatefulSetLabels, func() *Future[LabelsResult] {
		return m.noop.QueryStatefulSetLabels(start, end)
	})
}

func (m *MockMetricsQuerier) QueryStatefulSetAnnotations(start, end time.Time) *Future[AnnotationsResult] {
	return resolve(m.overrides, QueryStatefulSetAnnotations, func() *Future[AnnotationsResult] {
		return m.noop.QueryStatefulSetAnnotations(start, end)
	})
}

func (m *MockMetricsQuerier) QueryStatefulSetMatchLabels(start, end time.Time) *Future[StatefulSetLabelsResult] {
	return resolve(m.overrides, QueryStatefulSetMatchLabels, func() *Future[StatefulSetLabelsResult] {
		return m.noop.QueryStatefulSetMatchLabels(start, end)
	})
}

// DaemonSet

func (m *MockMetricsQuerier) QueryDaemonSetInfo(start, end time.Time) *Future[DaemonSetInfoResult] {
	return resolve(m.overrides, QueryDaemonSetInfo, func() *Future[DaemonSetInfoResult] {
		return m.noop.QueryDaemonSetInfo(start, end)
	})
}

func (m *MockMetricsQuerier) QueryDaemonSetUptime(start, end time.Time) *Future[UptimeResult] {
	return resolve(m.overrides, QueryDaemonSetUptime, func() *Future[UptimeResult] {
		return m.noop.QueryDaemonSetUptime(start, end)
	})
}

func (m *MockMetricsQuerier) QueryDaemonSetLabels(start, end time.Time) *Future[LabelsResult] {
	return resolve(m.overrides, QueryDaemonSetLabels, func() *Future[LabelsResult] {
		return m.noop.QueryDaemonSetLabels(start, end)
	})
}

func (m *MockMetricsQuerier) QueryDaemonSetAnnotations(start, end time.Time) *Future[AnnotationsResult] {
	return resolve(m.overrides, QueryDaemonSetAnnotations, func() *Future[AnnotationsResult] {
		return m.noop.QueryDaemonSetAnnotations(start, end)
	})
}

// Job

func (m *MockMetricsQuerier) QueryJobInfo(start, end time.Time) *Future[JobInfoResult] {
	return resolve(m.overrides, QueryJobInfo, func() *Future[JobInfoResult] {
		return m.noop.QueryJobInfo(start, end)
	})
}

func (m *MockMetricsQuerier) QueryJobUptime(start, end time.Time) *Future[UptimeResult] {
	return resolve(m.overrides, QueryJobUptime, func() *Future[UptimeResult] {
		return m.noop.QueryJobUptime(start, end)
	})
}

func (m *MockMetricsQuerier) QueryJobLabels(start, end time.Time) *Future[LabelsResult] {
	return resolve(m.overrides, QueryJobLabels, func() *Future[LabelsResult] {
		return m.noop.QueryJobLabels(start, end)
	})
}

func (m *MockMetricsQuerier) QueryJobAnnotations(start, end time.Time) *Future[AnnotationsResult] {
	return resolve(m.overrides, QueryJobAnnotations, func() *Future[AnnotationsResult] {
		return m.noop.QueryJobAnnotations(start, end)
	})
}

// CronJob

func (m *MockMetricsQuerier) QueryCronJobInfo(start, end time.Time) *Future[CronJobInfoResult] {
	return resolve(m.overrides, QueryCronJobInfo, func() *Future[CronJobInfoResult] {
		return m.noop.QueryCronJobInfo(start, end)
	})
}

func (m *MockMetricsQuerier) QueryCronJobUptime(start, end time.Time) *Future[UptimeResult] {
	return resolve(m.overrides, QueryCronJobUptime, func() *Future[UptimeResult] {
		return m.noop.QueryCronJobUptime(start, end)
	})
}

func (m *MockMetricsQuerier) QueryCronJobLabels(start, end time.Time) *Future[LabelsResult] {
	return resolve(m.overrides, QueryCronJobLabels, func() *Future[LabelsResult] {
		return m.noop.QueryCronJobLabels(start, end)
	})
}

func (m *MockMetricsQuerier) QueryCronJobAnnotations(start, end time.Time) *Future[AnnotationsResult] {
	return resolve(m.overrides, QueryCronJobAnnotations, func() *Future[AnnotationsResult] {
		return m.noop.QueryCronJobAnnotations(start, end)
	})
}

// ReplicaSet

func (m *MockMetricsQuerier) QueryReplicaSetInfo(start, end time.Time) *Future[ReplicaSetInfoResult] {
	return resolve(m.overrides, QueryReplicaSetInfo, func() *Future[ReplicaSetInfoResult] {
		return m.noop.QueryReplicaSetInfo(start, end)
	})
}

func (m *MockMetricsQuerier) QueryReplicaSetUptime(start, end time.Time) *Future[UptimeResult] {
	return resolve(m.overrides, QueryReplicaSetUptime, func() *Future[UptimeResult] {
		return m.noop.QueryReplicaSetUptime(start, end)
	})
}

func (m *MockMetricsQuerier) QueryReplicaSetLabels(start, end time.Time) *Future[LabelsResult] {
	return resolve(m.overrides, QueryReplicaSetLabels, func() *Future[LabelsResult] {
		return m.noop.QueryReplicaSetLabels(start, end)
	})
}

func (m *MockMetricsQuerier) QueryReplicaSetAnnotations(start, end time.Time) *Future[AnnotationsResult] {
	return resolve(m.overrides, QueryReplicaSetAnnotations, func() *Future[AnnotationsResult] {
		return m.noop.QueryReplicaSetAnnotations(start, end)
	})
}

func (m *MockMetricsQuerier) QueryReplicaSetOwners(start, end time.Time) *Future[OwnerResult] {
	return resolve(m.overrides, QueryReplicaSetOwners, func() *Future[OwnerResult] {
		return m.noop.QueryReplicaSetOwners(start, end)
	})
}

// Namespace

func (m *MockMetricsQuerier) QueryNamespaceInfo(start, end time.Time) *Future[NamespaceInfoResult] {
	return resolve(m.overrides, QueryNamespaceInfo, func() *Future[NamespaceInfoResult] {
		return m.noop.QueryNamespaceInfo(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNamespaceUptime(start, end time.Time) *Future[UptimeResult] {
	return resolve(m.overrides, QueryNamespaceUptime, func() *Future[UptimeResult] {
		return m.noop.QueryNamespaceUptime(start, end)
	})
}

// Service

func (m *MockMetricsQuerier) QueryServiceInfo(start, end time.Time) *Future[ServiceInfoResult] {
	return resolve(m.overrides, QueryServiceInfo, func() *Future[ServiceInfoResult] {
		return m.noop.QueryServiceInfo(start, end)
	})
}

func (m *MockMetricsQuerier) QueryServiceUptime(start, end time.Time) *Future[UptimeResult] {
	return resolve(m.overrides, QueryServiceUptime, func() *Future[UptimeResult] {
		return m.noop.QueryServiceUptime(start, end)
	})
}

func (m *MockMetricsQuerier) QueryServiceSelectorLabels(start, end time.Time) *Future[ServiceLabelsResult] {
	return resolve(m.overrides, QueryServiceSelectorLabels, func() *Future[ServiceLabelsResult] {
		return m.noop.QueryServiceSelectorLabels(start, end)
	})
}

// Network Egress

func (m *MockMetricsQuerier) QueryNetZoneGiB(start, end time.Time) *Future[NetZoneGiBResult] {
	return resolve(m.overrides, QueryNetZoneGiB, func() *Future[NetZoneGiBResult] {
		return m.noop.QueryNetZoneGiB(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNetZonePricePerGiB(start, end time.Time) *Future[NetZonePricePerGiBResult] {
	return resolve(m.overrides, QueryNetZonePricePerGiB, func() *Future[NetZonePricePerGiBResult] {
		return m.noop.QueryNetZonePricePerGiB(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNetRegionGiB(start, end time.Time) *Future[NetRegionGiBResult] {
	return resolve(m.overrides, QueryNetRegionGiB, func() *Future[NetRegionGiBResult] {
		return m.noop.QueryNetRegionGiB(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNetRegionPricePerGiB(start, end time.Time) *Future[NetRegionPricePerGiBResult] {
	return resolve(m.overrides, QueryNetRegionPricePerGiB, func() *Future[NetRegionPricePerGiBResult] {
		return m.noop.QueryNetRegionPricePerGiB(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNetInternetGiB(start, end time.Time) *Future[NetInternetGiBResult] {
	return resolve(m.overrides, QueryNetInternetGiB, func() *Future[NetInternetGiBResult] {
		return m.noop.QueryNetInternetGiB(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNetInternetPricePerGiB(start, end time.Time) *Future[NetInternetPricePerGiBResult] {
	return resolve(m.overrides, QueryNetInternetPricePerGiB, func() *Future[NetInternetPricePerGiBResult] {
		return m.noop.QueryNetInternetPricePerGiB(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNetInternetServiceGiB(start, end time.Time) *Future[NetInternetServiceGiBResult] {
	return resolve(m.overrides, QueryNetInternetServiceGiB, func() *Future[NetInternetServiceGiBResult] {
		return m.noop.QueryNetInternetServiceGiB(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNetNatGatewayPricePerGiB(start, end time.Time) *Future[NetNatGatewayPricePerGiBResult] {
	return resolve(m.overrides, QueryNetNatGatewayPricePerGiB, func() *Future[NetNatGatewayPricePerGiBResult] {
		return m.noop.QueryNetNatGatewayPricePerGiB(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNetNatGatewayGiB(start, end time.Time) *Future[NetNatGatewayGiBResult] {
	return resolve(m.overrides, QueryNetNatGatewayGiB, func() *Future[NetNatGatewayGiBResult] {
		return m.noop.QueryNetNatGatewayGiB(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNetTransferBytes(start, end time.Time) *Future[NetTransferBytesResult] {
	return resolve(m.overrides, QueryNetTransferBytes, func() *Future[NetTransferBytesResult] {
		return m.noop.QueryNetTransferBytes(start, end)
	})
}

// Network Ingress

func (m *MockMetricsQuerier) QueryNetZoneIngressGiB(start, end time.Time) *Future[NetZoneIngressGiBResult] {
	return resolve(m.overrides, QueryNetZoneIngressGiB, func() *Future[NetZoneIngressGiBResult] {
		return m.noop.QueryNetZoneIngressGiB(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNetRegionIngressGiB(start, end time.Time) *Future[NetRegionIngressGiBResult] {
	return resolve(m.overrides, QueryNetRegionIngressGiB, func() *Future[NetRegionIngressGiBResult] {
		return m.noop.QueryNetRegionIngressGiB(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNetInternetIngressGiB(start, end time.Time) *Future[NetInternetIngressGiBResult] {
	return resolve(m.overrides, QueryNetInternetIngressGiB, func() *Future[NetInternetIngressGiBResult] {
		return m.noop.QueryNetInternetIngressGiB(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNetInternetServiceIngressGiB(start, end time.Time) *Future[NetInternetServiceIngressGiBResult] {
	return resolve(m.overrides, QueryNetInternetServiceIngressGiB, func() *Future[NetInternetServiceIngressGiBResult] {
		return m.noop.QueryNetInternetServiceIngressGiB(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNetNatGatewayIngressPricePerGiB(start, end time.Time) *Future[NetNatGatewayPricePerGiBResult] {
	return resolve(m.overrides, QueryNetNatGatewayIngressPricePerGiB, func() *Future[NetNatGatewayPricePerGiBResult] {
		return m.noop.QueryNetNatGatewayIngressPricePerGiB(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNetNatGatewayIngressGiB(start, end time.Time) *Future[NetNatGatewayIngressGiBResult] {
	return resolve(m.overrides, QueryNetNatGatewayIngressGiB, func() *Future[NetNatGatewayIngressGiBResult] {
		return m.noop.QueryNetNatGatewayIngressGiB(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNetReceiveBytes(start, end time.Time) *Future[NetReceiveBytesResult] {
	return resolve(m.overrides, QueryNetReceiveBytes, func() *Future[NetReceiveBytesResult] {
		return m.noop.QueryNetReceiveBytes(start, end)
	})
}

// Annotations

func (m *MockMetricsQuerier) QueryNamespaceAnnotations(start, end time.Time) *Future[NamespaceAnnotationsResult] {
	return resolve(m.overrides, QueryNamespaceAnnotations, func() *Future[NamespaceAnnotationsResult] {
		return m.noop.QueryNamespaceAnnotations(start, end)
	})
}

func (m *MockMetricsQuerier) QueryPodAnnotations(start, end time.Time) *Future[PodAnnotationsResult] {
	return resolve(m.overrides, QueryPodAnnotations, func() *Future[PodAnnotationsResult] {
		return m.noop.QueryPodAnnotations(start, end)
	})
}

// Labels

func (m *MockMetricsQuerier) QueryNodeLabels(start, end time.Time) *Future[NodeLabelsResult] {
	return resolve(m.overrides, QueryNodeLabels, func() *Future[NodeLabelsResult] {
		return m.noop.QueryNodeLabels(start, end)
	})
}

func (m *MockMetricsQuerier) QueryNamespaceLabels(start, end time.Time) *Future[NamespaceLabelsResult] {
	return resolve(m.overrides, QueryNamespaceLabels, func() *Future[NamespaceLabelsResult] {
		return m.noop.QueryNamespaceLabels(start, end)
	})
}

func (m *MockMetricsQuerier) QueryPodLabels(start, end time.Time) *Future[PodLabelsResult] {
	return resolve(m.overrides, QueryPodLabels, func() *Future[PodLabelsResult] {
		return m.noop.QueryPodLabels(start, end)
	})
}

func (m *MockMetricsQuerier) QueryPodsWithDaemonSetOwner(start, end time.Time) *Future[PodsWithDaemonSetOwnerResult] {
	return resolve(m.overrides, QueryPodsWithDaemonSetOwner, func() *Future[PodsWithDaemonSetOwnerResult] {
		return m.noop.QueryPodsWithDaemonSetOwner(start, end)
	})
}

func (m *MockMetricsQuerier) QueryPodsWithJobOwner(start, end time.Time) *Future[PodsWithJobOwnerResult] {
	return resolve(m.overrides, QueryPodsWithJobOwner, func() *Future[PodsWithJobOwnerResult] {
		return m.noop.QueryPodsWithJobOwner(start, end)
	})
}

// ReplicaSet -> Controller mapping

func (m *MockMetricsQuerier) QueryPodsWithReplicaSetOwner(start, end time.Time) *Future[PodsWithReplicaSetOwnerResult] {
	return resolve(m.overrides, QueryPodsWithReplicaSetOwner, func() *Future[PodsWithReplicaSetOwnerResult] {
		return m.noop.QueryPodsWithReplicaSetOwner(start, end)
	})
}

func (m *MockMetricsQuerier) QueryReplicaSetsWithoutOwners(start, end time.Time) *Future[ReplicaSetsWithoutOwnersResult] {
	return resolve(m.overrides, QueryReplicaSetsWithoutOwners, func() *Future[ReplicaSetsWithoutOwnersResult] {
		return m.noop.QueryReplicaSetsWithoutOwners(start, end)
	})
}

func (m *MockMetricsQuerier) QueryReplicaSetsWithRollout(start, end time.Time) *Future[ReplicaSetsWithRolloutResult] {
	return resolve(m.overrides, QueryReplicaSetsWithRollout, func() *Future[ReplicaSetsWithRolloutResult] {
		return m.noop.QueryReplicaSetsWithRollout(start, end)
	})
}

// ResourceQuotas

func (m *MockMetricsQuerier) QueryResourceQuotaInfo(start, end time.Time) *Future[ResourceQuotaInfoResult] {
	return resolve(m.overrides, QueryResourceQuotaInfo, func() *Future[ResourceQuotaInfoResult] {
		return m.noop.QueryResourceQuotaInfo(start, end)
	})
}

func (m *MockMetricsQuerier) QueryResourceQuotaUptime(start, end time.Time) *Future[UptimeResult] {
	return resolve(m.overrides, QueryResourceQuotaUptime, func() *Future[UptimeResult] {
		return m.noop.QueryResourceQuotaUptime(start, end)
	})
}

func (m *MockMetricsQuerier) QueryResourceQuotaSpecCPURequestAverage(start, end time.Time) *Future[ResourceResult] {
	return resolve(m.overrides, QueryResourceQuotaSpecCPURequestAverage, func() *Future[ResourceResult] {
		return m.noop.QueryResourceQuotaSpecCPURequestAverage(start, end)
	})
}

func (m *MockMetricsQuerier) QueryResourceQuotaSpecCPURequestMax(start, end time.Time) *Future[ResourceResult] {
	return resolve(m.overrides, QueryResourceQuotaSpecCPURequestMax, func() *Future[ResourceResult] {
		return m.noop.QueryResourceQuotaSpecCPURequestMax(start, end)
	})
}

func (m *MockMetricsQuerier) QueryResourceQuotaSpecRAMRequestAverage(start, end time.Time) *Future[ResourceResult] {
	return resolve(m.overrides, QueryResourceQuotaSpecRAMRequestAverage, func() *Future[ResourceResult] {
		return m.noop.QueryResourceQuotaSpecRAMRequestAverage(start, end)
	})
}

func (m *MockMetricsQuerier) QueryResourceQuotaSpecRAMRequestMax(start, end time.Time) *Future[ResourceResult] {
	return resolve(m.overrides, QueryResourceQuotaSpecRAMRequestMax, func() *Future[ResourceResult] {
		return m.noop.QueryResourceQuotaSpecRAMRequestMax(start, end)
	})
}

func (m *MockMetricsQuerier) QueryResourceQuotaSpecCPULimitAverage(start, end time.Time) *Future[ResourceResult] {
	return resolve(m.overrides, QueryResourceQuotaSpecCPULimitAverage, func() *Future[ResourceResult] {
		return m.noop.QueryResourceQuotaSpecCPULimitAverage(start, end)
	})
}

func (m *MockMetricsQuerier) QueryResourceQuotaSpecCPULimitMax(start, end time.Time) *Future[ResourceResult] {
	return resolve(m.overrides, QueryResourceQuotaSpecCPULimitMax, func() *Future[ResourceResult] {
		return m.noop.QueryResourceQuotaSpecCPULimitMax(start, end)
	})
}

func (m *MockMetricsQuerier) QueryResourceQuotaSpecRAMLimitAverage(start, end time.Time) *Future[ResourceResult] {
	return resolve(m.overrides, QueryResourceQuotaSpecRAMLimitAverage, func() *Future[ResourceResult] {
		return m.noop.QueryResourceQuotaSpecRAMLimitAverage(start, end)
	})
}

func (m *MockMetricsQuerier) QueryResourceQuotaSpecRAMLimitMax(start, end time.Time) *Future[ResourceResult] {
	return resolve(m.overrides, QueryResourceQuotaSpecRAMLimitMax, func() *Future[ResourceResult] {
		return m.noop.QueryResourceQuotaSpecRAMLimitMax(start, end)
	})
}

func (m *MockMetricsQuerier) QueryResourceQuotaStatusUsedCPURequestAverage(start, end time.Time) *Future[ResourceResult] {
	return resolve(m.overrides, QueryResourceQuotaStatusUsedCPURequestAverage, func() *Future[ResourceResult] {
		return m.noop.QueryResourceQuotaStatusUsedCPURequestAverage(start, end)
	})
}

func (m *MockMetricsQuerier) QueryResourceQuotaStatusUsedCPURequestMax(start, end time.Time) *Future[ResourceResult] {
	return resolve(m.overrides, QueryResourceQuotaStatusUsedCPURequestMax, func() *Future[ResourceResult] {
		return m.noop.QueryResourceQuotaStatusUsedCPURequestMax(start, end)
	})
}

func (m *MockMetricsQuerier) QueryResourceQuotaStatusUsedRAMRequestAverage(start, end time.Time) *Future[ResourceResult] {
	return resolve(m.overrides, QueryResourceQuotaStatusUsedRAMRequestAverage, func() *Future[ResourceResult] {
		return m.noop.QueryResourceQuotaStatusUsedRAMRequestAverage(start, end)
	})
}

func (m *MockMetricsQuerier) QueryResourceQuotaStatusUsedRAMRequestMax(start, end time.Time) *Future[ResourceResult] {
	return resolve(m.overrides, QueryResourceQuotaStatusUsedRAMRequestMax, func() *Future[ResourceResult] {
		return m.noop.QueryResourceQuotaStatusUsedRAMRequestMax(start, end)
	})
}

func (m *MockMetricsQuerier) QueryResourceQuotaStatusUsedCPULimitAverage(start, end time.Time) *Future[ResourceResult] {
	return resolve(m.overrides, QueryResourceQuotaStatusUsedCPULimitAverage, func() *Future[ResourceResult] {
		return m.noop.QueryResourceQuotaStatusUsedCPULimitAverage(start, end)
	})
}

func (m *MockMetricsQuerier) QueryResourceQuotaStatusUsedCPULimitMax(start, end time.Time) *Future[ResourceResult] {
	return resolve(m.overrides, QueryResourceQuotaStatusUsedCPULimitMax, func() *Future[ResourceResult] {
		return m.noop.QueryResourceQuotaStatusUsedCPULimitMax(start, end)
	})
}

func (m *MockMetricsQuerier) QueryResourceQuotaStatusUsedRAMLimitAverage(start, end time.Time) *Future[ResourceResult] {
	return resolve(m.overrides, QueryResourceQuotaStatusUsedRAMLimitAverage, func() *Future[ResourceResult] {
		return m.noop.QueryResourceQuotaStatusUsedRAMLimitAverage(start, end)
	})
}

func (m *MockMetricsQuerier) QueryResourceQuotaStatusUsedRAMLimitMax(start, end time.Time) *Future[ResourceResult] {
	return resolve(m.overrides, QueryResourceQuotaStatusUsedRAMLimitMax, func() *Future[ResourceResult] {
		return m.noop.QueryResourceQuotaStatusUsedRAMLimitMax(start, end)
	})
}

// Data Coverage Query

func (m *MockMetricsQuerier) QueryDataCoverage(limitDays int) (time.Time, time.Time, error) {
	if v, ok := m.overrides[QueryDataCoverage]; ok {
		if f, ok := v.(func(int) (time.Time, time.Time, error)); ok {
			return f(limitDays)
		}
	}
	return m.noop.QueryDataCoverage(limitDays)
}

//--------------------------------------------------------------------------
//  Mock OpenCostDataSource
//--------------------------------------------------------------------------

var _ OpenCostDataSource = (*MockOpenCostDataSource)(nil)

// MockOpenCostDataSource is a minimal OpenCostDataSource for tests. Set fields
// directly to configure what the data source returns.
type MockOpenCostDataSource struct {
	Querier            *MockMetricsQuerier
	ClusterMapValue    clusters.ClusterMap
	ClusterInfoValue   clusters.ClusterInfoProvider
	BatchDurationValue time.Duration
	ResolutionValue    time.Duration
}

func NewMockOpenCostDataSource() *MockOpenCostDataSource {
	return &MockOpenCostDataSource{
		Querier: NewMockMetricsQuerier(),
	}
}

func (m *MockOpenCostDataSource) RegisterEndPoints(_ *httprouter.Router) {}

func (m *MockOpenCostDataSource) RegisterDiagnostics(_ diagnostics.DiagnosticService) {}

func (m *MockOpenCostDataSource) Metrics() MetricsQuerier {
	return m.Querier
}

func (m *MockOpenCostDataSource) ClusterMap() clusters.ClusterMap {
	return m.ClusterMapValue
}

func (m *MockOpenCostDataSource) ClusterInfo() clusters.ClusterInfoProvider {
	return m.ClusterInfoValue
}

func (m *MockOpenCostDataSource) BatchDuration() time.Duration {
	return m.BatchDurationValue
}

func (m *MockOpenCostDataSource) Resolution() time.Duration {
	return m.ResolutionValue
}
