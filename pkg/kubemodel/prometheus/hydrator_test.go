package prometheus

import (
	"context"
	"testing"
	"time"

	kubepb "github.com/opencost/opencost/core/pkg/model/pb/kubemodel"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/pkg/kubemodel"
	"github.com/julienschmidt/httprouter"
)

func TestBasicHydratorRequiresClusterID(t *testing.T) {
	// NewBasicHydrator should accept empty clusterID (hydrator creation doesn't validate)
	hydrator := NewBasicHydrator("")
	if hydrator == nil {
		t.Fatalf("expected hydrator to be created even with empty clusterID")
	}
}

func TestBasicHydrator(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	metrics := &fakeMetrics{
		nodeLabels: []*source.NodeLabelsResult{
			{
				UID:     "node-uid",
				Cluster: "",
				Node:    "node-a",
				Labels: map[string]string{
					"kubernetes.io/hostname": "node-a",
					"node-role":              "worker",
				},
			},
		},
		namespaceLabels: []*source.NamespaceLabelsResult{
			{
				UID:       "ns-uid",
				Cluster:   "",
				Namespace: "ns-a",
				Labels: map[string]string{
					"team": "platform",
				},
			},
		},
		podLabels: []*source.PodLabelsResult{
			{
				UID:       "pod-uid",
				Cluster:   "",
				Namespace: "ns-a",
				Pod:       "pod-a",
				Labels: map[string]string{
					"app": "demo",
				},
			},
		},
		podAnnotations: []*source.PodAnnotationsResult{
			{
				UID:         "pod-uid",
				Cluster:     "",
				Namespace:   "ns-a",
				Pod:         "pod-a",
				Annotations: map[string]string{"annotation": "value"},
			},
		},
	}

	ds := &fakeDataSource{metrics: metrics}
	hydrator := NewBasicHydrator("cluster-1")

	model := &kubemodel.Model{
		Nodes:      make(map[string]*kubepb.Node),
		Namespaces: make(map[string]*kubepb.Namespace),
		Pods:       make(map[string]*kubepb.Pod),
		Containers: make(map[string]*kubepb.Container),
	}

	err := hydrator(context.Background(), model, ds, start, end)
	if err != nil {
		t.Fatalf("unexpected hydrator error: %v", err)
	}

	// Verify node data
	node, ok := model.Nodes["node-uid"]
	if !ok {
		t.Fatalf("expected node data to be populated")
	}
	if node.ClusterID != "cluster-1" {
		t.Fatalf("expected node cluster ID 'cluster-1', got %q", node.ClusterID)
	}
	if node.Name != "node-a" {
		t.Fatalf("expected node name 'node-a', got %q", node.Name)
	}
	if len(node.Labels) != 2 {
		t.Fatalf("expected 2 node labels, got %d", len(node.Labels))
	}

	// Verify namespace data
	namespace, ok := model.Namespaces["ns-uid"]
	if !ok {
		t.Fatalf("expected namespace to be populated")
	}
	if namespace.ClusterID != "cluster-1" {
		t.Fatalf("expected namespace cluster ID 'cluster-1', got %q", namespace.ClusterID)
	}
	if namespace.Name != "ns-a" {
		t.Fatalf("expected namespace name 'ns-a', got %q", namespace.Name)
	}

	// Verify pod data
	pod, ok := model.Pods["pod-uid"]
	if !ok {
		t.Fatalf("expected pod to be populated")
	}
	if pod.NamespaceID != "ns-uid" {
		t.Fatalf("expected pod namespace ID to map to 'ns-uid', got %q", pod.NamespaceID)
	}
	if pod.Name != "pod-a" {
		t.Fatalf("expected pod name 'pod-a', got %q", pod.Name)
	}
	if got := pod.Labels["app"]; got != "demo" {
		t.Fatalf("expected pod label 'app=demo', got %q", got)
	}
	if got := pod.Annotations["annotation"]; got != "value" {
		t.Fatalf("expected pod annotation 'annotation=value', got %q", got)
	}

	// Verify containers not populated (not implemented yet)
	if len(model.Containers) != 0 {
		t.Fatalf("containers should not be populated yet")
	}

	// Verify metrics were queried with correct time range
	if len(metrics.starts) == 0 || !metrics.starts[0].Equal(start) {
		t.Fatalf("expected metrics queries to use start time %v, got %v", start, metrics.starts)
	}
	if len(metrics.ends) == 0 || !metrics.ends[0].Equal(end) {
		t.Fatalf("expected metrics queries to use end time %v, got %v", end, metrics.ends)
	}
}

func TestBasicHydratorNamespaceCreation(t *testing.T) {
	// Test that namespaces are created if they don't exist when pods reference them
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	metrics := &fakeMetrics{
		nodeLabels:      []*source.NodeLabelsResult{},
		namespaceLabels: []*source.NamespaceLabelsResult{},
		podLabels: []*source.PodLabelsResult{
			{
				UID:       "pod-uid",
				Cluster:   "cluster-1",
				Namespace: "orphan-ns",
				Pod:       "orphan-pod",
				Labels:    map[string]string{},
			},
		},
		podAnnotations: []*source.PodAnnotationsResult{},
	}

	ds := &fakeDataSource{metrics: metrics}
	hydrator := NewBasicHydrator("cluster-1")

	model := &kubemodel.Model{
		Nodes:      make(map[string]*kubepb.Node),
		Namespaces: make(map[string]*kubepb.Namespace),
		Pods:       make(map[string]*kubepb.Pod),
		Containers: make(map[string]*kubepb.Container),
	}

	err := hydrator(context.Background(), model, ds, start, end)
	if err != nil {
		t.Fatalf("unexpected hydrator error: %v", err)
	}

	// Verify namespace was auto-created
	nsKey := "cluster-1/orphan-ns"
	if _, ok := model.Namespaces[nsKey]; !ok {
		t.Fatalf("expected namespace %q to be auto-created", nsKey)
	}

	// Verify pod references the auto-created namespace
	pod, ok := model.Pods["pod-uid"]
	if !ok {
		t.Fatal("expected pod to be populated")
	}
	if pod.NamespaceID != nsKey {
		t.Fatalf("expected pod namespace ID %q, got %q", nsKey, pod.NamespaceID)
	}
}

// fakeMetrics implements source.MetricsQuerier for testing
type fakeMetrics struct {
	nodeLabels      []*source.NodeLabelsResult
	namespaceLabels []*source.NamespaceLabelsResult
	podLabels       []*source.PodLabelsResult
	podAnnotations  []*source.PodAnnotationsResult

	starts []time.Time
	ends   []time.Time
}

func (f *fakeMetrics) record(start, end time.Time) {
	f.starts = append(f.starts, start)
	f.ends = append(f.ends, end)
}

func (f *fakeMetrics) QueryNodeLabels(start, end time.Time) *source.Future[source.NodeLabelsResult] {
	f.record(start, end)
	return source.NewFutureFrom(f.nodeLabels)
}

func (f *fakeMetrics) QueryNamespaceLabels(start, end time.Time) *source.Future[source.NamespaceLabelsResult] {
	f.record(start, end)
	return source.NewFutureFrom(f.namespaceLabels)
}

func (f *fakeMetrics) QueryPodLabels(start, end time.Time) *source.Future[source.PodLabelsResult] {
	f.record(start, end)
	return source.NewFutureFrom(f.podLabels)
}

func (f *fakeMetrics) QueryPodAnnotations(start, end time.Time) *source.Future[source.PodAnnotationsResult] {
	f.record(start, end)
	return source.NewFutureFrom(f.podAnnotations)
}

// Stub implementations for the remaining MetricsQuerier interface methods
func (f *fakeMetrics) QueryPVActiveMinutes(start, end time.Time) *source.Future[source.PVActiveMinutesResult] {
	return source.NewFutureFrom([]*source.PVActiveMinutesResult{})
}
func (f *fakeMetrics) QueryPVUsedAverage(start, end time.Time) *source.Future[source.PVUsedAvgResult] {
	return source.NewFutureFrom([]*source.PVUsedAvgResult{})
}
func (f *fakeMetrics) QueryPVUsedMax(start, end time.Time) *source.Future[source.PVUsedMaxResult] {
	return source.NewFutureFrom([]*source.PVUsedMaxResult{})
}
func (f *fakeMetrics) QueryLocalStorageActiveMinutes(start, end time.Time) *source.Future[source.LocalStorageActiveMinutesResult] {
	return source.NewFutureFrom([]*source.LocalStorageActiveMinutesResult{})
}
func (f *fakeMetrics) QueryLocalStorageCost(start, end time.Time) *source.Future[source.LocalStorageCostResult] {
	return source.NewFutureFrom([]*source.LocalStorageCostResult{})
}
func (f *fakeMetrics) QueryLocalStorageUsedCost(start, end time.Time) *source.Future[source.LocalStorageUsedCostResult] {
	return source.NewFutureFrom([]*source.LocalStorageUsedCostResult{})
}
func (f *fakeMetrics) QueryLocalStorageUsedAvg(start, end time.Time) *source.Future[source.LocalStorageUsedAvgResult] {
	return source.NewFutureFrom([]*source.LocalStorageUsedAvgResult{})
}
func (f *fakeMetrics) QueryLocalStorageUsedMax(start, end time.Time) *source.Future[source.LocalStorageUsedMaxResult] {
	return source.NewFutureFrom([]*source.LocalStorageUsedMaxResult{})
}
func (f *fakeMetrics) QueryLocalStorageBytes(start, end time.Time) *source.Future[source.LocalStorageBytesResult] {
	return source.NewFutureFrom([]*source.LocalStorageBytesResult{})
}
func (f *fakeMetrics) QueryNodeActiveMinutes(start, end time.Time) *source.Future[source.NodeActiveMinutesResult] {
	return source.NewFutureFrom([]*source.NodeActiveMinutesResult{})
}
func (f *fakeMetrics) QueryNodeCPUCoresCapacity(start, end time.Time) *source.Future[source.NodeCPUCoresCapacityResult] {
	return source.NewFutureFrom([]*source.NodeCPUCoresCapacityResult{})
}
func (f *fakeMetrics) QueryNodeCPUCoresAllocatable(start, end time.Time) *source.Future[source.NodeCPUCoresAllocatableResult] {
	return source.NewFutureFrom([]*source.NodeCPUCoresAllocatableResult{})
}
func (f *fakeMetrics) QueryNodeRAMBytesCapacity(start, end time.Time) *source.Future[source.NodeRAMBytesCapacityResult] {
	return source.NewFutureFrom([]*source.NodeRAMBytesCapacityResult{})
}
func (f *fakeMetrics) QueryNodeRAMBytesAllocatable(start, end time.Time) *source.Future[source.NodeRAMBytesAllocatableResult] {
	return source.NewFutureFrom([]*source.NodeRAMBytesAllocatableResult{})
}
func (f *fakeMetrics) QueryNodeGPUCount(start, end time.Time) *source.Future[source.NodeGPUCountResult] {
	return source.NewFutureFrom([]*source.NodeGPUCountResult{})
}
func (f *fakeMetrics) QueryNodeCPUModeTotal(start, end time.Time) *source.Future[source.NodeCPUModeTotalResult] {
	return source.NewFutureFrom([]*source.NodeCPUModeTotalResult{})
}
func (f *fakeMetrics) QueryNodeIsSpot(start, end time.Time) *source.Future[source.NodeIsSpotResult] {
	return source.NewFutureFrom([]*source.NodeIsSpotResult{})
}
func (f *fakeMetrics) QueryNodeRAMSystemPercent(start, end time.Time) *source.Future[source.NodeRAMSystemPercentResult] {
	return source.NewFutureFrom([]*source.NodeRAMSystemPercentResult{})
}
func (f *fakeMetrics) QueryNodeRAMUserPercent(start, end time.Time) *source.Future[source.NodeRAMUserPercentResult] {
	return source.NewFutureFrom([]*source.NodeRAMUserPercentResult{})
}
func (f *fakeMetrics) QueryLBActiveMinutes(start, end time.Time) *source.Future[source.LBActiveMinutesResult] {
	return source.NewFutureFrom([]*source.LBActiveMinutesResult{})
}
func (f *fakeMetrics) QueryLBPricePerHr(start, end time.Time) *source.Future[source.LBPricePerHrResult] {
	return source.NewFutureFrom([]*source.LBPricePerHrResult{})
}
func (f *fakeMetrics) QueryClusterManagementDuration(start, end time.Time) *source.Future[source.ClusterManagementDurationResult] {
	return source.NewFutureFrom([]*source.ClusterManagementDurationResult{})
}
func (f *fakeMetrics) QueryClusterManagementPricePerHr(start, end time.Time) *source.Future[source.ClusterManagementPricePerHrResult] {
	return source.NewFutureFrom([]*source.ClusterManagementPricePerHrResult{})
}
func (f *fakeMetrics) QueryPods(start, end time.Time) *source.Future[source.PodsResult] {
	return source.NewFutureFrom([]*source.PodsResult{})
}
func (f *fakeMetrics) QueryPodsUID(start, end time.Time) *source.Future[source.PodsResult] {
	return source.NewFutureFrom([]*source.PodsResult{})
}
func (f *fakeMetrics) QueryRAMBytesAllocated(start, end time.Time) *source.Future[source.RAMBytesAllocatedResult] {
	return source.NewFutureFrom([]*source.RAMBytesAllocatedResult{})
}
func (f *fakeMetrics) QueryRAMRequests(start, end time.Time) *source.Future[source.RAMRequestsResult] {
	return source.NewFutureFrom([]*source.RAMRequestsResult{})
}
func (f *fakeMetrics) QueryRAMUsageAvg(start, end time.Time) *source.Future[source.RAMUsageAvgResult] {
	return source.NewFutureFrom([]*source.RAMUsageAvgResult{})
}
func (f *fakeMetrics) QueryRAMUsageMax(start, end time.Time) *source.Future[source.RAMUsageMaxResult] {
	return source.NewFutureFrom([]*source.RAMUsageMaxResult{})
}
func (f *fakeMetrics) QueryNodeRAMPricePerGiBHr(start, end time.Time) *source.Future[source.NodeRAMPricePerGiBHrResult] {
	return source.NewFutureFrom([]*source.NodeRAMPricePerGiBHrResult{})
}
func (f *fakeMetrics) QueryCPUCoresAllocated(start, end time.Time) *source.Future[source.CPUCoresAllocatedResult] {
	return source.NewFutureFrom([]*source.CPUCoresAllocatedResult{})
}
func (f *fakeMetrics) QueryCPURequests(start, end time.Time) *source.Future[source.CPURequestsResult] {
	return source.NewFutureFrom([]*source.CPURequestsResult{})
}
func (f *fakeMetrics) QueryCPUUsageAvg(start, end time.Time) *source.Future[source.CPUUsageAvgResult] {
	return source.NewFutureFrom([]*source.CPUUsageAvgResult{})
}
func (f *fakeMetrics) QueryCPUUsageMax(start, end time.Time) *source.Future[source.CPUUsageMaxResult] {
	return source.NewFutureFrom([]*source.CPUUsageMaxResult{})
}
func (f *fakeMetrics) QueryNodeCPUPricePerHr(start, end time.Time) *source.Future[source.NodeCPUPricePerHrResult] {
	return source.NewFutureFrom([]*source.NodeCPUPricePerHrResult{})
}
func (f *fakeMetrics) QueryGPUsAllocated(start, end time.Time) *source.Future[source.GPUsAllocatedResult] {
	return source.NewFutureFrom([]*source.GPUsAllocatedResult{})
}
func (f *fakeMetrics) QueryGPUsRequested(start, end time.Time) *source.Future[source.GPUsRequestedResult] {
	return source.NewFutureFrom([]*source.GPUsRequestedResult{})
}
func (f *fakeMetrics) QueryGPUsUsageAvg(start, end time.Time) *source.Future[source.GPUsUsageAvgResult] {
	return source.NewFutureFrom([]*source.GPUsUsageAvgResult{})
}
func (f *fakeMetrics) QueryGPUsUsageMax(start, end time.Time) *source.Future[source.GPUsUsageMaxResult] {
	return source.NewFutureFrom([]*source.GPUsUsageMaxResult{})
}
func (f *fakeMetrics) QueryNodeGPUPricePerHr(start, end time.Time) *source.Future[source.NodeGPUPricePerHrResult] {
	return source.NewFutureFrom([]*source.NodeGPUPricePerHrResult{})
}
func (f *fakeMetrics) QueryGPUInfo(start, end time.Time) *source.Future[source.GPUInfoResult] {
	return source.NewFutureFrom([]*source.GPUInfoResult{})
}
func (f *fakeMetrics) QueryIsGPUShared(start, end time.Time) *source.Future[source.IsGPUSharedResult] {
	return source.NewFutureFrom([]*source.IsGPUSharedResult{})
}
func (f *fakeMetrics) QueryPodPVCAllocation(start, end time.Time) *source.Future[source.PodPVCAllocationResult] {
	return source.NewFutureFrom([]*source.PodPVCAllocationResult{})
}
func (f *fakeMetrics) QueryPVCBytesRequested(start, end time.Time) *source.Future[source.PVCBytesRequestedResult] {
	return source.NewFutureFrom([]*source.PVCBytesRequestedResult{})
}
func (f *fakeMetrics) QueryPVCInfo(start, end time.Time) *source.Future[source.PVCInfoResult] {
	return source.NewFutureFrom([]*source.PVCInfoResult{})
}
func (f *fakeMetrics) QueryPVBytes(start, end time.Time) *source.Future[source.PVBytesResult] {
	return source.NewFutureFrom([]*source.PVBytesResult{})
}
func (f *fakeMetrics) QueryPVPricePerGiBHour(start, end time.Time) *source.Future[source.PVPricePerGiBHourResult] {
	return source.NewFutureFrom([]*source.PVPricePerGiBHourResult{})
}
func (f *fakeMetrics) QueryPVInfo(start, end time.Time) *source.Future[source.PVInfoResult] {
	return source.NewFutureFrom([]*source.PVInfoResult{})
}
func (f *fakeMetrics) QueryNetZoneGiB(start, end time.Time) *source.Future[source.NetZoneGiBResult] {
	return source.NewFutureFrom([]*source.NetZoneGiBResult{})
}
func (f *fakeMetrics) QueryNetZonePricePerGiB(start, end time.Time) *source.Future[source.NetZonePricePerGiBResult] {
	return source.NewFutureFrom([]*source.NetZonePricePerGiBResult{})
}
func (f *fakeMetrics) QueryNetRegionGiB(start, end time.Time) *source.Future[source.NetRegionGiBResult] {
	return source.NewFutureFrom([]*source.NetRegionGiBResult{})
}
func (f *fakeMetrics) QueryNetRegionPricePerGiB(start, end time.Time) *source.Future[source.NetRegionPricePerGiBResult] {
	return source.NewFutureFrom([]*source.NetRegionPricePerGiBResult{})
}
func (f *fakeMetrics) QueryNetInternetGiB(start, end time.Time) *source.Future[source.NetInternetGiBResult] {
	return source.NewFutureFrom([]*source.NetInternetGiBResult{})
}
func (f *fakeMetrics) QueryNetInternetPricePerGiB(start, end time.Time) *source.Future[source.NetInternetPricePerGiBResult] {
	return source.NewFutureFrom([]*source.NetInternetPricePerGiBResult{})
}
func (f *fakeMetrics) QueryNetInternetServiceGiB(start, end time.Time) *source.Future[source.NetInternetServiceGiBResult] {
	return source.NewFutureFrom([]*source.NetInternetServiceGiBResult{})
}
func (f *fakeMetrics) QueryNetTransferBytes(start, end time.Time) *source.Future[source.NetTransferBytesResult] {
	return source.NewFutureFrom([]*source.NetTransferBytesResult{})
}
func (f *fakeMetrics) QueryNetZoneIngressGiB(start, end time.Time) *source.Future[source.NetZoneIngressGiBResult] {
	return source.NewFutureFrom([]*source.NetZoneIngressGiBResult{})
}
func (f *fakeMetrics) QueryNetRegionIngressGiB(start, end time.Time) *source.Future[source.NetRegionIngressGiBResult] {
	return source.NewFutureFrom([]*source.NetRegionIngressGiBResult{})
}
func (f *fakeMetrics) QueryNetInternetIngressGiB(start, end time.Time) *source.Future[source.NetInternetIngressGiBResult] {
	return source.NewFutureFrom([]*source.NetInternetIngressGiBResult{})
}
func (f *fakeMetrics) QueryNetInternetServiceIngressGiB(start, end time.Time) *source.Future[source.NetInternetServiceIngressGiBResult] {
	return source.NewFutureFrom([]*source.NetInternetServiceIngressGiBResult{})
}
func (f *fakeMetrics) QueryNetReceiveBytes(start, end time.Time) *source.Future[source.NetReceiveBytesResult] {
	return source.NewFutureFrom([]*source.NetReceiveBytesResult{})
}
func (f *fakeMetrics) QueryNamespaceAnnotations(start, end time.Time) *source.Future[source.NamespaceAnnotationsResult] {
	return source.NewFutureFrom([]*source.NamespaceAnnotationsResult{})
}
func (f *fakeMetrics) QueryServiceLabels(start, end time.Time) *source.Future[source.ServiceLabelsResult] {
	return source.NewFutureFrom([]*source.ServiceLabelsResult{})
}
func (f *fakeMetrics) QueryDeploymentLabels(start, end time.Time) *source.Future[source.DeploymentLabelsResult] {
	return source.NewFutureFrom([]*source.DeploymentLabelsResult{})
}
func (f *fakeMetrics) QueryStatefulSetLabels(start, end time.Time) *source.Future[source.StatefulSetLabelsResult] {
	return source.NewFutureFrom([]*source.StatefulSetLabelsResult{})
}
func (f *fakeMetrics) QueryDaemonSetLabels(start, end time.Time) *source.Future[source.DaemonSetLabelsResult] {
	return source.NewFutureFrom([]*source.DaemonSetLabelsResult{})
}
func (f *fakeMetrics) QueryJobLabels(start, end time.Time) *source.Future[source.JobLabelsResult] {
	return source.NewFutureFrom([]*source.JobLabelsResult{})
}
func (f *fakeMetrics) QueryPodsWithReplicaSetOwner(start, end time.Time) *source.Future[source.PodsWithReplicaSetOwnerResult] {
	return source.NewFutureFrom([]*source.PodsWithReplicaSetOwnerResult{})
}
func (f *fakeMetrics) QueryReplicaSetsWithoutOwners(start, end time.Time) *source.Future[source.ReplicaSetsWithoutOwnersResult] {
	return source.NewFutureFrom([]*source.ReplicaSetsWithoutOwnersResult{})
}
func (f *fakeMetrics) QueryReplicaSetsWithRollout(start, end time.Time) *source.Future[source.ReplicaSetsWithRolloutResult] {
	return source.NewFutureFrom([]*source.ReplicaSetsWithRolloutResult{})
}
func (f *fakeMetrics) QueryDataCoverage(limitDays int) (time.Time, time.Time, error) {
	return time.Time{}, time.Time{}, nil
}

// Ensure fakeMetrics implements source.MetricsQuerier
var _ source.MetricsQuerier = (*fakeMetrics)(nil)

// fakeClusterInfo implements clusters.ClusterInfoProvider
type fakeClusterInfo struct{}

func (f *fakeClusterInfo) GetClusterInfo() map[string]string {
	return make(map[string]string)
}

// fakeClusterMap implements clusters.ClusterMap
type fakeClusterMap struct{}

func (f *fakeClusterMap) GetClusterIDs() []string {
	return []string{}
}

func (f *fakeClusterMap) AsMap() map[string]*clusters.ClusterInfo {
	return make(map[string]*clusters.ClusterInfo)
}

func (f *fakeClusterMap) InfoFor(clusterID string) *clusters.ClusterInfo {
	return nil
}

func (f *fakeClusterMap) NameFor(clusterID string) string {
	return ""
}

func (f *fakeClusterMap) NameIDFor(clusterID string) string {
	return clusterID
}

// fakeDataSource wraps fakeMetrics to implement source.OpenCostDataSource
type fakeDataSource struct {
	metrics *fakeMetrics
}

func (f *fakeDataSource) RegisterEndPoints(router *httprouter.Router)           {}
func (f *fakeDataSource) RegisterDiagnostics(diagService diagnostics.DiagnosticService) {}
func (f *fakeDataSource) Metrics() source.MetricsQuerier                        { return f.metrics }
func (f *fakeDataSource) ClusterMap() clusters.ClusterMap                       { return &fakeClusterMap{} }
func (f *fakeDataSource) ClusterInfo() clusters.ClusterInfoProvider             { return &fakeClusterInfo{} }
func (f *fakeDataSource) BatchDuration() time.Duration                          { return time.Hour }
func (f *fakeDataSource) Resolution() time.Duration                             { return time.Minute }

// Ensure fakeDataSource implements source.OpenCostDataSource
var _ source.OpenCostDataSource = (*fakeDataSource)(nil)