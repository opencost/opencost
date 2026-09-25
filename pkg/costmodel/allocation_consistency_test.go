package costmodel

// F-19u / OC-04 repro (invariant E3: one consistent data view per computation).
//
// computeAllocation queries the data source in two phases: buildPodMap runs
// QueryPods/QueryPodsUID in its own QueryGroup, then computeAllocation calls
// DataSource.Metrics() again and issues the resource queries. A data source
// that swaps in-memory snapshots between (or during) those phases yields a pod
// map from one state and resource series from another; series for pods not in
// the map are silently dropped.
//
// swapQuerier models such a source: it holds two immutable states (A and B)
// and serves query number < flipAfter from A and every later query from B,
// counting across all Metrics() calls. It implements source.PinnableMetricsQuerier:
// a pinned view serves every query from the state current at pin time, as an
// adapter over immutable snapshots would.

import (
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/pkg/cloud/provider"
	"github.com/opencost/opencost/pkg/config"
	"github.com/stretchr/testify/require"
)

const (
	swapStateA = 0
	swapStateB = 1
)

var swapStateNames = [2]string{"A", "B"}

type servedQuery struct {
	method string
	state  int
}

// swapQuerier is a MetricsQuerier over two snapshots that flips from state A
// to state B once flipAfter queries have been served.
type swapQuerier struct {
	states    [2]source.MetricsQuerier
	flipAfter int

	// parent and pinnedState are set on a pinned view; queries are recorded on the parent
	parent      *swapQuerier
	pinnedState int

	mu       sync.Mutex
	served   []servedQuery
	pins     int
	releases int
}

var _ source.PinnableMetricsQuerier = (*swapQuerier)(nil)

// currentState returns the state an unpinned query would be served from; q.mu must be held
func (q *swapQuerier) currentState() int {
	if len(q.served) >= q.flipAfter {
		return swapStateB
	}
	return swapStateA
}

func (q *swapQuerier) serve(method string) source.MetricsQuerier {
	root := q
	if q.parent != nil {
		root = q.parent
	}
	root.mu.Lock()
	defer root.mu.Unlock()

	state := q.pinnedState
	if q.parent == nil {
		state = root.currentState()
	}
	root.served = append(root.served, servedQuery{method: method, state: state})
	return q.states[state]
}

func (q *swapQuerier) Pin() (source.MetricsQuerier, func()) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pins++
	pinned := &swapQuerier{states: q.states, parent: q, pinnedState: q.currentState()}
	return pinned, func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		q.releases++
	}
}

func (q *swapQuerier) servedQueries() []servedQuery {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]servedQuery(nil), q.served...)
}

// swapDataSource returns the same swapQuerier from every Metrics() call, so
// the query count (and hence the flip) spans all Metrics() calls.
type swapDataSource struct {
	querier       *swapQuerier
	resolution    time.Duration
	metricsCallsN int
	mu            sync.Mutex
}

var _ source.OpenCostDataSource = (*swapDataSource)(nil)

func (d *swapDataSource) RegisterEndPoints(_ *httprouter.Router)              {}
func (d *swapDataSource) RegisterDiagnostics(_ diagnostics.DiagnosticService) {}
func (d *swapDataSource) ClusterMap() clusters.ClusterMap                     { return nil }
func (d *swapDataSource) ClusterInfo() clusters.ClusterInfoProvider           { return nil }
func (d *swapDataSource) BatchDuration() time.Duration                        { return 0 }
func (d *swapDataSource) Resolution() time.Duration                           { return d.resolution }
func (d *swapDataSource) Metrics() source.MetricsQuerier {
	d.mu.Lock()
	d.metricsCallsN++
	d.mu.Unlock()
	return d.querier
}

func swapTestSnapshot(cluster, node string, pods []string, start, end time.Time) *source.MockMetricsQuerier {
	q := source.NewMockMetricsQuerier()

	var running []*util.Vector
	for ts := start; !ts.After(end); ts = ts.Add(10 * time.Minute) {
		running = append(running, &util.Vector{Timestamp: float64(ts.Unix()), Value: 1})
	}

	var podsRes []*source.PodsResult
	var ram, cpu []*source.ContainerMetricResult
	for _, p := range pods {
		podsRes = append(podsRes, &source.PodsResult{
			UID: "uid-" + p, Cluster: cluster, Namespace: "ns", Pod: p, Data: running,
		})
		ram = append(ram, &source.ContainerMetricResult{
			Cluster: cluster, Node: node, Namespace: "ns", Pod: p, Container: "app",
			Data: []*util.Vector{{Timestamp: float64(end.Unix()), Value: 1024 * 1024 * 1024}},
		})
		cpu = append(cpu, &source.ContainerMetricResult{
			Cluster: cluster, Node: node, Namespace: "ns", Pod: p, Container: "app",
			Data: []*util.Vector{{Timestamp: float64(end.Unix()), Value: 1}},
		})
	}

	q.SetOverride(source.QueryPods, podsRes)
	q.SetOverride(source.QueryPodsUID, podsRes)
	q.SetOverride(source.QueryRAMBytesAllocated, ram)
	q.SetOverride(source.QueryRAMRequests, ram)
	q.SetOverride(source.QueryCPUCoresAllocated, cpu)
	q.SetOverride(source.QueryCPURequests, cpu)
	return q
}

func TestComputeAllocation_ConsistentDataView(t *testing.T) {
	// the data source swaps its snapshot after the first query (the pod query): computeAllocation must
	// still see only state A, so pod-b is absent and none of its data is used
	t.Run("swap after pod query", func(t *testing.T) { checkConsistentDataView(t, 1, false) })
	// the swap happens before the computation starts: everything comes from state B, including pod-b
	t.Run("swap before computation", func(t *testing.T) { checkConsistentDataView(t, 0, true) })
}

func checkConsistentDataView(t *testing.T, flipAfter int, wantPodB bool) {
	const cluster, node = "cluster-one", "node-1"
	end := time.Now().UTC().Truncate(time.Hour)
	start := end.Add(-time.Hour)

	stateA := swapTestSnapshot(cluster, node, []string{"pod-a"}, start, end)
	stateB := swapTestSnapshot(cluster, node, []string{"pod-a", "pod-b"}, start, end)

	// Flip after the first query: the pod query (buildPodMap's first call)
	// is served from A, every subsequent query from B, i.e. the adapter
	// swapped its snapshot between the pod phase and the resource phase.
	querier := &swapQuerier{states: [2]source.MetricsQuerier{stateA, stateB}, flipAfter: flipAfter}
	ds := &swapDataSource{querier: querier, resolution: time.Minute}

	confMan := config.NewConfigFileManager(storage.NewFileStorage("../../"))
	customProvider := &provider.CSVProvider{
		CSVLocation: "../../configs/pricing_schema_pv.csv",
		CustomProvider: &provider.CustomProvider{
			Config: provider.NewProviderConfig(confMan, "../../configs/default.json"),
		},
	}
	require.NoError(t, customProvider.DownloadPricingData())

	cm := &CostModel{DataSource: ds, Provider: customProvider}

	allocSet, _, err := cm.computeAllocation(start, end)
	require.NoError(t, err)
	require.NotNil(t, allocSet)

	served := querier.servedQueries()
	require.NotEmpty(t, served, "fake served no queries; computeAllocation did not use the data source")

	byState := map[int][]string{}
	for _, s := range served {
		byState[s.state] = append(byState[s.state], s.method)
	}

	podPresent := map[string]bool{}
	var podBRAM, podBCPU float64
	for _, alloc := range allocSet.Allocations {
		p := alloc.Properties.Pod
		for _, name := range []string{"pod-a", "pod-b"} {
			if p == name || strings.HasPrefix(p, name+" ") {
				podPresent[name] = true
				if name == "pod-b" {
					podBRAM += alloc.RAMByteHours
					podBCPU += alloc.CPUCoreHours
				}
			}
		}
	}

	// E3 (direct): every query in one computeAllocation call must be served
	// from the same data-source state.
	if len(byState) > 1 {
		for _, st := range []int{swapStateA, swapStateB} {
			methods := append([]string(nil), byState[st]...)
			sort.Strings(methods)
			t.Logf("state %s served %d queries, e.g. %v", swapStateNames[st], len(methods), head(methods, 6))
		}
		t.Errorf("E3 violated: one computeAllocation call was served from %d data-source states (A=%d queries, B=%d queries, %d Metrics() calls); the pod map and the resource series come from different snapshots",
			len(byState), len(byState[swapStateA]), len(byState[swapStateB]), ds.metricsCallsN)
	}

	// Diagnostic consequence: state B carried RAM/CPU series for pod-b, but
	// pod-b was not in state A's pod map, so those series were dropped.
	fromB := len(byState[swapStateB]) > 0
	if fromB && !podPresent["pod-b"] {
		t.Logf("consequence: pod-b RAM/CPU series were fetched from state B but pod-b is absent from the AllocationSet (silently dropped); pods present: %v", podPresent)
	}

	// E3 (outcome): pod-b is either absent with none of its data used, or
	// fully present with its allocation.
	if podPresent["pod-b"] {
		if podBRAM <= 0 || podBCPU <= 0 {
			t.Errorf("E3 violated: pod-b present but incomplete (RAMByteHours=%v CPUCoreHours=%v)", podBRAM, podBCPU)
		}
	}
	if !podPresent["pod-a"] {
		t.Errorf("pod-a missing from AllocationSet; test setup is not driving computeAllocation end to end")
	}
	if podPresent["pod-b"] != wantPodB {
		t.Errorf("pod-b present = %v, want %v", podPresent["pod-b"], wantPodB)
	}

	querier.mu.Lock()
	pins, releases := querier.pins, querier.releases
	querier.mu.Unlock()
	if pins != 1 || releases != 1 {
		t.Errorf("expected the querier to be pinned and released once per computation, got pins=%d releases=%d", pins, releases)
	}
}

func head(s []string, n int) []string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// ---- swapQuerier: MetricsQuerier forwarding (generated from source.RecordMetricsQuerier) ----

func (q *swapQuerier) QueryLocalStorageActiveMinutes(start, end time.Time) *source.Future[source.LocalStorageActiveMinutesResult] {
	return q.serve(source.QueryLocalStorageActiveMinutes).QueryLocalStorageActiveMinutes(start, end)
}

func (q *swapQuerier) QueryLocalStorageUsedAvg(start, end time.Time) *source.Future[source.LocalStorageUsedAvgResult] {
	return q.serve(source.QueryLocalStorageUsedAvg).QueryLocalStorageUsedAvg(start, end)
}

func (q *swapQuerier) QueryLocalStorageUsedMax(start, end time.Time) *source.Future[source.LocalStorageUsedMaxResult] {
	return q.serve(source.QueryLocalStorageUsedMax).QueryLocalStorageUsedMax(start, end)
}

func (q *swapQuerier) QueryLocalStorageBytes(start, end time.Time) *source.Future[source.LocalStorageBytesResult] {
	return q.serve(source.QueryLocalStorageBytes).QueryLocalStorageBytes(start, end)
}

func (q *swapQuerier) QueryKMLocalStorageUsedAvg(start, end time.Time) *source.Future[source.NodeUIDValueResult] {
	return q.serve(source.QueryKMLocalStorageUsedAvg).QueryKMLocalStorageUsedAvg(start, end)
}

func (q *swapQuerier) QueryKMLocalStorageUsedMax(start, end time.Time) *source.Future[source.NodeUIDValueResult] {
	return q.serve(source.QueryKMLocalStorageUsedMax).QueryKMLocalStorageUsedMax(start, end)
}

func (q *swapQuerier) QueryKMLocalStorageBytes(start, end time.Time) *source.Future[source.UIDValueResult] {
	return q.serve(source.QueryKMLocalStorageBytes).QueryKMLocalStorageBytes(start, end)
}

func (q *swapQuerier) QueryNodeInfo(start, end time.Time) *source.Future[source.NodeInfoResult] {
	return q.serve(source.QueryNodeInfo).QueryNodeInfo(start, end)
}

func (q *swapQuerier) QueryNodeUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	return q.serve(source.QueryNodeUptime).QueryNodeUptime(start, end)
}

func (q *swapQuerier) QueryNodeActiveMinutes(start, end time.Time) *source.Future[source.NodeActiveMinutesResult] {
	return q.serve(source.QueryNodeActiveMinutes).QueryNodeActiveMinutes(start, end)
}

func (q *swapQuerier) QueryNodeCPUCoresCapacity(start, end time.Time) *source.Future[source.NodeCPUCoresCapacityResult] {
	return q.serve(source.QueryNodeCPUCoresCapacity).QueryNodeCPUCoresCapacity(start, end)
}

func (q *swapQuerier) QueryNodeCPUCoresAllocatable(start, end time.Time) *source.Future[source.NodeCPUCoresAllocatableResult] {
	return q.serve(source.QueryNodeCPUCoresAllocatable).QueryNodeCPUCoresAllocatable(start, end)
}

func (q *swapQuerier) QueryNodeRAMBytesCapacity(start, end time.Time) *source.Future[source.NodeRAMBytesCapacityResult] {
	return q.serve(source.QueryNodeRAMBytesCapacity).QueryNodeRAMBytesCapacity(start, end)
}

func (q *swapQuerier) QueryNodeRAMBytesAllocatable(start, end time.Time) *source.Future[source.NodeRAMBytesAllocatableResult] {
	return q.serve(source.QueryNodeRAMBytesAllocatable).QueryNodeRAMBytesAllocatable(start, end)
}

func (q *swapQuerier) QueryNodeGPUCount(start, end time.Time) *source.Future[source.NodeGPUCountResult] {
	return q.serve(source.QueryNodeGPUCount).QueryNodeGPUCount(start, end)
}

func (q *swapQuerier) QueryNodeCPUModeTotal(start, end time.Time) *source.Future[source.NodeCPUModeTotalResult] {
	return q.serve(source.QueryNodeCPUModeTotal).QueryNodeCPUModeTotal(start, end)
}

func (q *swapQuerier) QueryNodeIsSpot(start, end time.Time) *source.Future[source.NodeIsSpotResult] {
	return q.serve(source.QueryNodeIsSpot).QueryNodeIsSpot(start, end)
}

func (q *swapQuerier) QueryNodeRAMSystemPercent(start, end time.Time) *source.Future[source.NodeRAMSystemPercentResult] {
	return q.serve(source.QueryNodeRAMSystemPercent).QueryNodeRAMSystemPercent(start, end)
}

func (q *swapQuerier) QueryNodeRAMUserPercent(start, end time.Time) *source.Future[source.NodeRAMUserPercentResult] {
	return q.serve(source.QueryNodeRAMUserPercent).QueryNodeRAMUserPercent(start, end)
}

func (q *swapQuerier) QueryNodeResourceCapacities(start, end time.Time) *source.Future[source.ResourceResult] {
	return q.serve(source.QueryNodeResourceCapacities).QueryNodeResourceCapacities(start, end)
}

func (q *swapQuerier) QueryNodeResourcesAllocatable(start, end time.Time) *source.Future[source.ResourceResult] {
	return q.serve(source.QueryNodeResourcesAllocatable).QueryNodeResourcesAllocatable(start, end)
}

func (q *swapQuerier) QueryLBActiveMinutes(start, end time.Time) *source.Future[source.LBActiveMinutesResult] {
	return q.serve(source.QueryLBActiveMinutes).QueryLBActiveMinutes(start, end)
}

func (q *swapQuerier) QueryLBPricePerHr(start, end time.Time) *source.Future[source.LBPricePerHrResult] {
	return q.serve(source.QueryLBPricePerHr).QueryLBPricePerHr(start, end)
}

func (q *swapQuerier) QueryClusterInfo(start, end time.Time) *source.Future[source.ClusterInfoResult] {
	return q.serve(source.QueryClusterInfo).QueryClusterInfo(start, end)
}

func (q *swapQuerier) QueryClusterKubeModelVersion(start, end time.Time) *source.Future[source.ClusterKubeModelVersionResult] {
	return q.serve(source.QueryClusterKubeModelVersion).QueryClusterKubeModelVersion(start, end)
}

func (q *swapQuerier) QueryClusterUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	return q.serve(source.QueryClusterUptime).QueryClusterUptime(start, end)
}

func (q *swapQuerier) QueryClusterManagementDuration(start, end time.Time) *source.Future[source.ClusterManagementDurationResult] {
	return q.serve(source.QueryClusterManagementDuration).QueryClusterManagementDuration(start, end)
}

func (q *swapQuerier) QueryClusterManagementPricePerHr(start, end time.Time) *source.Future[source.ClusterManagementPricePerHrResult] {
	return q.serve(source.QueryClusterManagementPricePerHr).QueryClusterManagementPricePerHr(start, end)
}

func (q *swapQuerier) QueryPods(start, end time.Time) *source.Future[source.PodsResult] {
	return q.serve(source.QueryPods).QueryPods(start, end)
}

func (q *swapQuerier) QueryPodsUID(start, end time.Time) *source.Future[source.PodsResult] {
	return q.serve(source.QueryPodsUID).QueryPodsUID(start, end)
}

func (q *swapQuerier) QueryPodInfo(start, end time.Time) *source.Future[source.PodInfoResult] {
	return q.serve(source.QueryPodInfo).QueryPodInfo(start, end)
}

func (q *swapQuerier) QueryPodUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	return q.serve(source.QueryPodUptime).QueryPodUptime(start, end)
}

func (q *swapQuerier) QueryPodOwners(start, end time.Time) *source.Future[source.OwnerResult] {
	return q.serve(source.QueryPodOwners).QueryPodOwners(start, end)
}

func (q *swapQuerier) QueryPodPVCVolumes(start, end time.Time) *source.Future[source.PodPVCVolumeResult] {
	return q.serve(source.QueryPodPVCVolumes).QueryPodPVCVolumes(start, end)
}

func (q *swapQuerier) QueryPodNetworkEgressBytes(start, end time.Time) *source.Future[source.PodNetworkBytesResult] {
	return q.serve(source.QueryPodNetworkEgressBytes).QueryPodNetworkEgressBytes(start, end)
}

func (q *swapQuerier) QueryPodNetworkIngressBytes(start, end time.Time) *source.Future[source.PodNetworkBytesResult] {
	return q.serve(source.QueryPodNetworkIngressBytes).QueryPodNetworkIngressBytes(start, end)
}

func (q *swapQuerier) QueryContainerUptime(start, end time.Time) *source.Future[source.ContainerUptimeResult] {
	return q.serve(source.QueryContainerUptime).QueryContainerUptime(start, end)
}

func (q *swapQuerier) QueryContainerResourceRequests(start, end time.Time) *source.Future[source.ContainerResourceResult] {
	return q.serve(source.QueryContainerResourceRequests).QueryContainerResourceRequests(start, end)
}

func (q *swapQuerier) QueryContainerResourceLimits(start, end time.Time) *source.Future[source.ContainerResourceResult] {
	return q.serve(source.QueryContainerResourceLimits).QueryContainerResourceLimits(start, end)
}

func (q *swapQuerier) QueryRAMBytesAllocated(start, end time.Time) *source.Future[source.RAMBytesAllocatedResult] {
	return q.serve(source.QueryRAMBytesAllocated).QueryRAMBytesAllocated(start, end)
}

func (q *swapQuerier) QueryRAMRequests(start, end time.Time) *source.Future[source.RAMRequestsResult] {
	return q.serve(source.QueryRAMRequests).QueryRAMRequests(start, end)
}

func (q *swapQuerier) QueryRAMLimits(start, end time.Time) *source.Future[source.RAMLimitsResult] {
	return q.serve(source.QueryRAMLimits).QueryRAMLimits(start, end)
}

func (q *swapQuerier) QueryRAMUsageAvg(start, end time.Time) *source.Future[source.RAMUsageAvgResult] {
	return q.serve(source.QueryRAMUsageAvg).QueryRAMUsageAvg(start, end)
}

func (q *swapQuerier) QueryRAMUsageMax(start, end time.Time) *source.Future[source.RAMUsageMaxResult] {
	return q.serve(source.QueryRAMUsageMax).QueryRAMUsageMax(start, end)
}

func (q *swapQuerier) QueryNodeRAMPricePerGiBHr(start, end time.Time) *source.Future[source.NodeRAMPricePerGiBHrResult] {
	return q.serve(source.QueryNodeRAMPricePerGiBHr).QueryNodeRAMPricePerGiBHr(start, end)
}

func (q *swapQuerier) QueryCPUCoresAllocated(start, end time.Time) *source.Future[source.CPUCoresAllocatedResult] {
	return q.serve(source.QueryCPUCoresAllocated).QueryCPUCoresAllocated(start, end)
}

func (q *swapQuerier) QueryCPURequests(start, end time.Time) *source.Future[source.CPURequestsResult] {
	return q.serve(source.QueryCPURequests).QueryCPURequests(start, end)
}

func (q *swapQuerier) QueryCPULimits(start, end time.Time) *source.Future[source.CPULimitsResult] {
	return q.serve(source.QueryCPULimits).QueryCPULimits(start, end)
}

func (q *swapQuerier) QueryCPUUsageAvg(start, end time.Time) *source.Future[source.CPUUsageAvgResult] {
	return q.serve(source.QueryCPUUsageAvg).QueryCPUUsageAvg(start, end)
}

func (q *swapQuerier) QueryCPUUsageMax(start, end time.Time) *source.Future[source.CPUUsageMaxResult] {
	return q.serve(source.QueryCPUUsageMax).QueryCPUUsageMax(start, end)
}

func (q *swapQuerier) QueryNodeCPUPricePerHr(start, end time.Time) *source.Future[source.NodeCPUPricePerHrResult] {
	return q.serve(source.QueryNodeCPUPricePerHr).QueryNodeCPUPricePerHr(start, end)
}

func (q *swapQuerier) QueryGPUsAllocated(start, end time.Time) *source.Future[source.GPUsAllocatedResult] {
	return q.serve(source.QueryGPUsAllocated).QueryGPUsAllocated(start, end)
}

func (q *swapQuerier) QueryGPUsRequested(start, end time.Time) *source.Future[source.GPUsRequestedResult] {
	return q.serve(source.QueryGPUsRequested).QueryGPUsRequested(start, end)
}

func (q *swapQuerier) QueryGPUsUsageAvg(start, end time.Time) *source.Future[source.GPUsUsageAvgResult] {
	return q.serve(source.QueryGPUsUsageAvg).QueryGPUsUsageAvg(start, end)
}

func (q *swapQuerier) QueryGPUsUsageMax(start, end time.Time) *source.Future[source.GPUsUsageMaxResult] {
	return q.serve(source.QueryGPUsUsageMax).QueryGPUsUsageMax(start, end)
}

func (q *swapQuerier) QueryNodeGPUPricePerHr(start, end time.Time) *source.Future[source.NodeGPUPricePerHrResult] {
	return q.serve(source.QueryNodeGPUPricePerHr).QueryNodeGPUPricePerHr(start, end)
}

func (q *swapQuerier) QueryGPUInfo(start, end time.Time) *source.Future[source.GPUInfoResult] {
	return q.serve(source.QueryGPUInfo).QueryGPUInfo(start, end)
}

func (q *swapQuerier) QueryIsGPUShared(start, end time.Time) *source.Future[source.IsGPUSharedResult] {
	return q.serve(source.QueryIsGPUShared).QueryIsGPUShared(start, end)
}

func (q *swapQuerier) QueryDCGMDeviceInfo(start, end time.Time) *source.Future[source.DCGMDeviceInfoResult] {
	return q.serve(source.QueryDCGMDeviceInfo).QueryDCGMDeviceInfo(start, end)
}

func (q *swapQuerier) QueryDCGMDeviceUptime(start, end time.Time) *source.Future[source.DCGMDeviceUptimeResult] {
	return q.serve(source.QueryDCGMDeviceUptime).QueryDCGMDeviceUptime(start, end)
}

func (q *swapQuerier) QueryDCGMContainerUsageAvg(start, end time.Time) *source.Future[source.DCGMDeviceContainerUsageResult] {
	return q.serve(source.QueryDCGMContainerUsageAvg).QueryDCGMContainerUsageAvg(start, end)
}

func (q *swapQuerier) QueryDCGMContainerUsageMax(start, end time.Time) *source.Future[source.DCGMDeviceContainerUsageResult] {
	return q.serve(source.QueryDCGMContainerUsageMax).QueryDCGMContainerUsageMax(start, end)
}

func (q *swapQuerier) QueryPodPVCAllocation(start, end time.Time) *source.Future[source.PodPVCAllocationResult] {
	return q.serve(source.QueryPodPVCAllocation).QueryPodPVCAllocation(start, end)
}

func (q *swapQuerier) QueryPVCBytesRequested(start, end time.Time) *source.Future[source.PVCBytesRequestedResult] {
	return q.serve(source.QueryPVCBytesRequested).QueryPVCBytesRequested(start, end)
}

func (q *swapQuerier) QueryPVCInfo(start, end time.Time) *source.Future[source.PVCInfoResult] {
	return q.serve(source.QueryPVCInfo).QueryPVCInfo(start, end)
}

func (q *swapQuerier) QueryKMPVCInfo(start, end time.Time) *source.Future[source.PVCInfoResult] {
	return q.serve(source.QueryKMPVCInfo).QueryKMPVCInfo(start, end)
}

func (q *swapQuerier) QueryPVCUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	return q.serve(source.QueryPVCUptime).QueryPVCUptime(start, end)
}

func (q *swapQuerier) QueryPVCBytesUsedAverage(start, end time.Time) *source.Future[source.PVCUIDValueResult] {
	return q.serve(source.QueryPVCBytesUsedAverage).QueryPVCBytesUsedAverage(start, end)
}

func (q *swapQuerier) QueryPVCBytesUsedMax(start, end time.Time) *source.Future[source.PVCUIDValueResult] {
	return q.serve(source.QueryPVCBytesUsedMax).QueryPVCBytesUsedMax(start, end)
}

func (q *swapQuerier) QueryPVBytes(start, end time.Time) *source.Future[source.PVBytesResult] {
	return q.serve(source.QueryPVBytes).QueryPVBytes(start, end)
}

func (q *swapQuerier) QueryPVPricePerGiBHour(start, end time.Time) *source.Future[source.PVPricePerGiBHourResult] {
	return q.serve(source.QueryPVPricePerGiBHour).QueryPVPricePerGiBHour(start, end)
}

func (q *swapQuerier) QueryPVInfo(start, end time.Time) *source.Future[source.PVInfoResult] {
	return q.serve(source.QueryPVInfo).QueryPVInfo(start, end)
}

func (q *swapQuerier) QueryPVActiveMinutes(start, end time.Time) *source.Future[source.PVActiveMinutesResult] {
	return q.serve(source.QueryPVActiveMinutes).QueryPVActiveMinutes(start, end)
}

func (q *swapQuerier) QueryPVUsedAverage(start, end time.Time) *source.Future[source.PVUsedAvgResult] {
	return q.serve(source.QueryPVUsedAverage).QueryPVUsedAverage(start, end)
}

func (q *swapQuerier) QueryPVUsedMax(start, end time.Time) *source.Future[source.PVUsedMaxResult] {
	return q.serve(source.QueryPVUsedMax).QueryPVUsedMax(start, end)
}

func (q *swapQuerier) QueryKMPVInfo(start, end time.Time) *source.Future[source.PVInfoResult] {
	return q.serve(source.QueryKMPVInfo).QueryKMPVInfo(start, end)
}

func (q *swapQuerier) QueryPVUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	return q.serve(source.QueryPVUptime).QueryPVUptime(start, end)
}

func (q *swapQuerier) QueryDeploymentInfo(start, end time.Time) *source.Future[source.DeploymentInfoResult] {
	return q.serve(source.QueryDeploymentInfo).QueryDeploymentInfo(start, end)
}

func (q *swapQuerier) QueryDeploymentUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	return q.serve(source.QueryDeploymentUptime).QueryDeploymentUptime(start, end)
}

func (q *swapQuerier) QueryDeploymentLabels(start, end time.Time) *source.Future[source.LabelsResult] {
	return q.serve(source.QueryDeploymentLabels).QueryDeploymentLabels(start, end)
}

func (q *swapQuerier) QueryDeploymentAnnotations(start, end time.Time) *source.Future[source.AnnotationsResult] {
	return q.serve(source.QueryDeploymentAnnotations).QueryDeploymentAnnotations(start, end)
}

func (q *swapQuerier) QueryDeploymentMatchLabels(start, end time.Time) *source.Future[source.DeploymentLabelsResult] {
	return q.serve(source.QueryDeploymentMatchLabels).QueryDeploymentMatchLabels(start, end)
}

func (q *swapQuerier) QueryStatefulSetInfo(start, end time.Time) *source.Future[source.StatefulSetInfoResult] {
	return q.serve(source.QueryStatefulSetInfo).QueryStatefulSetInfo(start, end)
}

func (q *swapQuerier) QueryStatefulSetUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	return q.serve(source.QueryStatefulSetUptime).QueryStatefulSetUptime(start, end)
}

func (q *swapQuerier) QueryStatefulSetLabels(start, end time.Time) *source.Future[source.LabelsResult] {
	return q.serve(source.QueryStatefulSetLabels).QueryStatefulSetLabels(start, end)
}

func (q *swapQuerier) QueryStatefulSetAnnotations(start, end time.Time) *source.Future[source.AnnotationsResult] {
	return q.serve(source.QueryStatefulSetAnnotations).QueryStatefulSetAnnotations(start, end)
}

func (q *swapQuerier) QueryStatefulSetMatchLabels(start, end time.Time) *source.Future[source.StatefulSetLabelsResult] {
	return q.serve(source.QueryStatefulSetMatchLabels).QueryStatefulSetMatchLabels(start, end)
}

func (q *swapQuerier) QueryDaemonSetInfo(start, end time.Time) *source.Future[source.DaemonSetInfoResult] {
	return q.serve(source.QueryDaemonSetInfo).QueryDaemonSetInfo(start, end)
}

func (q *swapQuerier) QueryDaemonSetUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	return q.serve(source.QueryDaemonSetUptime).QueryDaemonSetUptime(start, end)
}

func (q *swapQuerier) QueryDaemonSetLabels(start, end time.Time) *source.Future[source.LabelsResult] {
	return q.serve(source.QueryDaemonSetLabels).QueryDaemonSetLabels(start, end)
}

func (q *swapQuerier) QueryDaemonSetAnnotations(start, end time.Time) *source.Future[source.AnnotationsResult] {
	return q.serve(source.QueryDaemonSetAnnotations).QueryDaemonSetAnnotations(start, end)
}

func (q *swapQuerier) QueryDaemonSetArguments(start, end time.Time) *source.Future[source.DaemonSetArgumentResult] {
	return q.serve(source.QueryDaemonSetArguments).QueryDaemonSetArguments(start, end)
}

func (q *swapQuerier) QueryJobInfo(start, end time.Time) *source.Future[source.JobInfoResult] {
	return q.serve(source.QueryJobInfo).QueryJobInfo(start, end)
}

func (q *swapQuerier) QueryJobUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	return q.serve(source.QueryJobUptime).QueryJobUptime(start, end)
}

func (q *swapQuerier) QueryJobLabels(start, end time.Time) *source.Future[source.LabelsResult] {
	return q.serve(source.QueryJobLabels).QueryJobLabels(start, end)
}

func (q *swapQuerier) QueryJobAnnotations(start, end time.Time) *source.Future[source.AnnotationsResult] {
	return q.serve(source.QueryJobAnnotations).QueryJobAnnotations(start, end)
}

func (q *swapQuerier) QueryCronJobInfo(start, end time.Time) *source.Future[source.CronJobInfoResult] {
	return q.serve(source.QueryCronJobInfo).QueryCronJobInfo(start, end)
}

func (q *swapQuerier) QueryCronJobUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	return q.serve(source.QueryCronJobUptime).QueryCronJobUptime(start, end)
}

func (q *swapQuerier) QueryCronJobLabels(start, end time.Time) *source.Future[source.LabelsResult] {
	return q.serve(source.QueryCronJobLabels).QueryCronJobLabels(start, end)
}

func (q *swapQuerier) QueryCronJobAnnotations(start, end time.Time) *source.Future[source.AnnotationsResult] {
	return q.serve(source.QueryCronJobAnnotations).QueryCronJobAnnotations(start, end)
}

func (q *swapQuerier) QueryReplicaSetInfo(start, end time.Time) *source.Future[source.ReplicaSetInfoResult] {
	return q.serve(source.QueryReplicaSetInfo).QueryReplicaSetInfo(start, end)
}

func (q *swapQuerier) QueryReplicaSetUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	return q.serve(source.QueryReplicaSetUptime).QueryReplicaSetUptime(start, end)
}

func (q *swapQuerier) QueryReplicaSetLabels(start, end time.Time) *source.Future[source.LabelsResult] {
	return q.serve(source.QueryReplicaSetLabels).QueryReplicaSetLabels(start, end)
}

func (q *swapQuerier) QueryReplicaSetAnnotations(start, end time.Time) *source.Future[source.AnnotationsResult] {
	return q.serve(source.QueryReplicaSetAnnotations).QueryReplicaSetAnnotations(start, end)
}

func (q *swapQuerier) QueryReplicaSetOwners(start, end time.Time) *source.Future[source.OwnerResult] {
	return q.serve(source.QueryReplicaSetOwners).QueryReplicaSetOwners(start, end)
}

func (q *swapQuerier) QueryNamespaceInfo(start, end time.Time) *source.Future[source.NamespaceInfoResult] {
	return q.serve(source.QueryNamespaceInfo).QueryNamespaceInfo(start, end)
}

func (q *swapQuerier) QueryNamespaceUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	return q.serve(source.QueryNamespaceUptime).QueryNamespaceUptime(start, end)
}

func (q *swapQuerier) QueryServiceInfo(start, end time.Time) *source.Future[source.ServiceInfoResult] {
	return q.serve(source.QueryServiceInfo).QueryServiceInfo(start, end)
}

func (q *swapQuerier) QueryServiceUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	return q.serve(source.QueryServiceUptime).QueryServiceUptime(start, end)
}

func (q *swapQuerier) QueryServiceSelectorLabels(start, end time.Time) *source.Future[source.ServiceLabelsResult] {
	return q.serve(source.QueryServiceSelectorLabels).QueryServiceSelectorLabels(start, end)
}

func (q *swapQuerier) QueryNetZoneGiB(start, end time.Time) *source.Future[source.NetZoneGiBResult] {
	return q.serve(source.QueryNetZoneGiB).QueryNetZoneGiB(start, end)
}

func (q *swapQuerier) QueryNetZonePricePerGiB(start, end time.Time) *source.Future[source.NetZonePricePerGiBResult] {
	return q.serve(source.QueryNetZonePricePerGiB).QueryNetZonePricePerGiB(start, end)
}

func (q *swapQuerier) QueryNetRegionGiB(start, end time.Time) *source.Future[source.NetRegionGiBResult] {
	return q.serve(source.QueryNetRegionGiB).QueryNetRegionGiB(start, end)
}

func (q *swapQuerier) QueryNetRegionPricePerGiB(start, end time.Time) *source.Future[source.NetRegionPricePerGiBResult] {
	return q.serve(source.QueryNetRegionPricePerGiB).QueryNetRegionPricePerGiB(start, end)
}

func (q *swapQuerier) QueryNetInternetGiB(start, end time.Time) *source.Future[source.NetInternetGiBResult] {
	return q.serve(source.QueryNetInternetGiB).QueryNetInternetGiB(start, end)
}

func (q *swapQuerier) QueryNetInternetPricePerGiB(start, end time.Time) *source.Future[source.NetInternetPricePerGiBResult] {
	return q.serve(source.QueryNetInternetPricePerGiB).QueryNetInternetPricePerGiB(start, end)
}

func (q *swapQuerier) QueryNetInternetServiceGiB(start, end time.Time) *source.Future[source.NetInternetServiceGiBResult] {
	return q.serve(source.QueryNetInternetServiceGiB).QueryNetInternetServiceGiB(start, end)
}

func (q *swapQuerier) QueryNetNatGatewayPricePerGiB(start, end time.Time) *source.Future[source.NetNatGatewayPricePerGiBResult] {
	return q.serve(source.QueryNetNatGatewayPricePerGiB).QueryNetNatGatewayPricePerGiB(start, end)
}

func (q *swapQuerier) QueryNetNatGatewayGiB(start, end time.Time) *source.Future[source.NetNatGatewayGiBResult] {
	return q.serve(source.QueryNetNatGatewayGiB).QueryNetNatGatewayGiB(start, end)
}

func (q *swapQuerier) QueryNetTransferBytes(start, end time.Time) *source.Future[source.NetTransferBytesResult] {
	return q.serve(source.QueryNetTransferBytes).QueryNetTransferBytes(start, end)
}

func (q *swapQuerier) QueryNetZoneIngressGiB(start, end time.Time) *source.Future[source.NetZoneIngressGiBResult] {
	return q.serve(source.QueryNetZoneIngressGiB).QueryNetZoneIngressGiB(start, end)
}

func (q *swapQuerier) QueryNetRegionIngressGiB(start, end time.Time) *source.Future[source.NetRegionIngressGiBResult] {
	return q.serve(source.QueryNetRegionIngressGiB).QueryNetRegionIngressGiB(start, end)
}

func (q *swapQuerier) QueryNetInternetIngressGiB(start, end time.Time) *source.Future[source.NetInternetIngressGiBResult] {
	return q.serve(source.QueryNetInternetIngressGiB).QueryNetInternetIngressGiB(start, end)
}

func (q *swapQuerier) QueryNetInternetServiceIngressGiB(start, end time.Time) *source.Future[source.NetInternetServiceIngressGiBResult] {
	return q.serve(source.QueryNetInternetServiceIngressGiB).QueryNetInternetServiceIngressGiB(start, end)
}

func (q *swapQuerier) QueryNetNatGatewayIngressPricePerGiB(start, end time.Time) *source.Future[source.NetNatGatewayPricePerGiBResult] {
	return q.serve(source.QueryNetNatGatewayIngressPricePerGiB).QueryNetNatGatewayIngressPricePerGiB(start, end)
}

func (q *swapQuerier) QueryNetNatGatewayIngressGiB(start, end time.Time) *source.Future[source.NetNatGatewayIngressGiBResult] {
	return q.serve(source.QueryNetNatGatewayIngressGiB).QueryNetNatGatewayIngressGiB(start, end)
}

func (q *swapQuerier) QueryNetReceiveBytes(start, end time.Time) *source.Future[source.NetReceiveBytesResult] {
	return q.serve(source.QueryNetReceiveBytes).QueryNetReceiveBytes(start, end)
}

func (q *swapQuerier) QueryNamespaceAnnotations(start, end time.Time) *source.Future[source.NamespaceAnnotationsResult] {
	return q.serve(source.QueryNamespaceAnnotations).QueryNamespaceAnnotations(start, end)
}

func (q *swapQuerier) QueryPodAnnotations(start, end time.Time) *source.Future[source.PodAnnotationsResult] {
	return q.serve(source.QueryPodAnnotations).QueryPodAnnotations(start, end)
}

func (q *swapQuerier) QueryNodeLabels(start, end time.Time) *source.Future[source.NodeLabelsResult] {
	return q.serve(source.QueryNodeLabels).QueryNodeLabels(start, end)
}

func (q *swapQuerier) QueryNamespaceLabels(start, end time.Time) *source.Future[source.NamespaceLabelsResult] {
	return q.serve(source.QueryNamespaceLabels).QueryNamespaceLabels(start, end)
}

func (q *swapQuerier) QueryPodLabels(start, end time.Time) *source.Future[source.PodLabelsResult] {
	return q.serve(source.QueryPodLabels).QueryPodLabels(start, end)
}

func (q *swapQuerier) QueryPodsWithDaemonSetOwner(start, end time.Time) *source.Future[source.PodsWithDaemonSetOwnerResult] {
	return q.serve(source.QueryPodsWithDaemonSetOwner).QueryPodsWithDaemonSetOwner(start, end)
}

func (q *swapQuerier) QueryPodsWithJobOwner(start, end time.Time) *source.Future[source.PodsWithJobOwnerResult] {
	return q.serve(source.QueryPodsWithJobOwner).QueryPodsWithJobOwner(start, end)
}

func (q *swapQuerier) QueryPodsWithReplicaSetOwner(start, end time.Time) *source.Future[source.PodsWithReplicaSetOwnerResult] {
	return q.serve(source.QueryPodsWithReplicaSetOwner).QueryPodsWithReplicaSetOwner(start, end)
}

func (q *swapQuerier) QueryReplicaSetsWithoutOwners(start, end time.Time) *source.Future[source.ReplicaSetsWithoutOwnersResult] {
	return q.serve(source.QueryReplicaSetsWithoutOwners).QueryReplicaSetsWithoutOwners(start, end)
}

func (q *swapQuerier) QueryReplicaSetsWithRollout(start, end time.Time) *source.Future[source.ReplicaSetsWithRolloutResult] {
	return q.serve(source.QueryReplicaSetsWithRollout).QueryReplicaSetsWithRollout(start, end)
}

func (q *swapQuerier) QueryResourceQuotaInfo(start, end time.Time) *source.Future[source.ResourceQuotaInfoResult] {
	return q.serve(source.QueryResourceQuotaInfo).QueryResourceQuotaInfo(start, end)
}

func (q *swapQuerier) QueryResourceQuotaUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	return q.serve(source.QueryResourceQuotaUptime).QueryResourceQuotaUptime(start, end)
}

func (q *swapQuerier) QueryResourceQuotaSpecCPURequestAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	return q.serve(source.QueryResourceQuotaSpecCPURequestAverage).QueryResourceQuotaSpecCPURequestAverage(start, end)
}

func (q *swapQuerier) QueryResourceQuotaSpecCPURequestMax(start, end time.Time) *source.Future[source.ResourceResult] {
	return q.serve(source.QueryResourceQuotaSpecCPURequestMax).QueryResourceQuotaSpecCPURequestMax(start, end)
}

func (q *swapQuerier) QueryResourceQuotaSpecRAMRequestAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	return q.serve(source.QueryResourceQuotaSpecRAMRequestAverage).QueryResourceQuotaSpecRAMRequestAverage(start, end)
}

func (q *swapQuerier) QueryResourceQuotaSpecRAMRequestMax(start, end time.Time) *source.Future[source.ResourceResult] {
	return q.serve(source.QueryResourceQuotaSpecRAMRequestMax).QueryResourceQuotaSpecRAMRequestMax(start, end)
}

func (q *swapQuerier) QueryResourceQuotaSpecCPULimitAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	return q.serve(source.QueryResourceQuotaSpecCPULimitAverage).QueryResourceQuotaSpecCPULimitAverage(start, end)
}

func (q *swapQuerier) QueryResourceQuotaSpecCPULimitMax(start, end time.Time) *source.Future[source.ResourceResult] {
	return q.serve(source.QueryResourceQuotaSpecCPULimitMax).QueryResourceQuotaSpecCPULimitMax(start, end)
}

func (q *swapQuerier) QueryResourceQuotaSpecRAMLimitAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	return q.serve(source.QueryResourceQuotaSpecRAMLimitAverage).QueryResourceQuotaSpecRAMLimitAverage(start, end)
}

func (q *swapQuerier) QueryResourceQuotaSpecRAMLimitMax(start, end time.Time) *source.Future[source.ResourceResult] {
	return q.serve(source.QueryResourceQuotaSpecRAMLimitMax).QueryResourceQuotaSpecRAMLimitMax(start, end)
}

func (q *swapQuerier) QueryResourceQuotaStatusUsedCPURequestAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	return q.serve(source.QueryResourceQuotaStatusUsedCPURequestAverage).QueryResourceQuotaStatusUsedCPURequestAverage(start, end)
}

func (q *swapQuerier) QueryResourceQuotaStatusUsedCPURequestMax(start, end time.Time) *source.Future[source.ResourceResult] {
	return q.serve(source.QueryResourceQuotaStatusUsedCPURequestMax).QueryResourceQuotaStatusUsedCPURequestMax(start, end)
}

func (q *swapQuerier) QueryResourceQuotaStatusUsedRAMRequestAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	return q.serve(source.QueryResourceQuotaStatusUsedRAMRequestAverage).QueryResourceQuotaStatusUsedRAMRequestAverage(start, end)
}

func (q *swapQuerier) QueryResourceQuotaStatusUsedRAMRequestMax(start, end time.Time) *source.Future[source.ResourceResult] {
	return q.serve(source.QueryResourceQuotaStatusUsedRAMRequestMax).QueryResourceQuotaStatusUsedRAMRequestMax(start, end)
}

func (q *swapQuerier) QueryResourceQuotaStatusUsedCPULimitAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	return q.serve(source.QueryResourceQuotaStatusUsedCPULimitAverage).QueryResourceQuotaStatusUsedCPULimitAverage(start, end)
}

func (q *swapQuerier) QueryResourceQuotaStatusUsedCPULimitMax(start, end time.Time) *source.Future[source.ResourceResult] {
	return q.serve(source.QueryResourceQuotaStatusUsedCPULimitMax).QueryResourceQuotaStatusUsedCPULimitMax(start, end)
}

func (q *swapQuerier) QueryResourceQuotaStatusUsedRAMLimitAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	return q.serve(source.QueryResourceQuotaStatusUsedRAMLimitAverage).QueryResourceQuotaStatusUsedRAMLimitAverage(start, end)
}

func (q *swapQuerier) QueryResourceQuotaStatusUsedRAMLimitMax(start, end time.Time) *source.Future[source.ResourceResult] {
	return q.serve(source.QueryResourceQuotaStatusUsedRAMLimitMax).QueryResourceQuotaStatusUsedRAMLimitMax(start, end)
}

func (q *swapQuerier) QueryInferencePromptTokens(start, end time.Time) *source.Future[source.InferenceTokensResult] {
	return q.serve(source.QueryInferencePromptTokens).QueryInferencePromptTokens(start, end)
}

func (q *swapQuerier) QueryInferenceGenerationTokens(start, end time.Time) *source.Future[source.InferenceTokensResult] {
	return q.serve(source.QueryInferenceGenerationTokens).QueryInferenceGenerationTokens(start, end)
}

func (q *swapQuerier) QueryInferenceInputProcessingTime(start, end time.Time) *source.Future[source.InferenceProcessingTimeResult] {
	return q.serve(source.QueryInferenceInputProcessingTime).QueryInferenceInputProcessingTime(start, end)
}

func (q *swapQuerier) QueryInferenceOutputProcessingTime(start, end time.Time) *source.Future[source.InferenceProcessingTimeResult] {
	return q.serve(source.QueryInferenceOutputProcessingTime).QueryInferenceOutputProcessingTime(start, end)
}

func (q *swapQuerier) QueryInferenceCachedTokens(start, end time.Time) *source.Future[source.InferenceTokensResult] {
	return q.serve(source.QueryInferenceCachedTokens).QueryInferenceCachedTokens(start, end)
}

func (q *swapQuerier) QueryInferenceCacheConfig(t time.Time) *source.Future[source.InferenceCacheConfigResult] {
	return q.serve(source.QueryInferenceCacheConfig).QueryInferenceCacheConfig(t)
}

func (q *swapQuerier) QueryDataCoverage(limitDays int) (time.Time, time.Time, error) {
	return q.serve(source.QueryDataCoverage).QueryDataCoverage(limitDays)
}

// newSwapCostModel returns a CostModel over a swapQuerier that swaps from state A to state B after the
// first query.
func newSwapCostModel(t *testing.T, start, end time.Time) (*CostModel, *swapQuerier) {
	t.Helper()
	const cluster, node = "cluster-one", "node-1"
	stateA := swapTestSnapshot(cluster, node, []string{"pod-a"}, start, end)
	stateB := swapTestSnapshot(cluster, node, []string{"pod-a", "pod-b"}, start, end)
	querier := &swapQuerier{states: [2]source.MetricsQuerier{stateA, stateB}, flipAfter: 1}

	confMan := config.NewConfigFileManager(storage.NewFileStorage("../../"))
	customProvider := &provider.CSVProvider{
		CSVLocation: "../../configs/pricing_schema_pv.csv",
		CustomProvider: &provider.CustomProvider{
			Config: provider.NewProviderConfig(confMan, "../../configs/default.json"),
		},
	}
	require.NoError(t, customProvider.DownloadPricingData())

	return &CostModel{DataSource: &swapDataSource{querier: querier, resolution: time.Minute}, Provider: customProvider}, querier
}

// requireSingleState fails if the querier served queries from more than one state, or was not pinned
// and released exactly once.
func requireSingleState(t *testing.T, querier *swapQuerier) {
	t.Helper()
	states := map[int]int{}
	for _, s := range querier.servedQueries() {
		states[s.state]++
	}
	require.NotEmpty(t, states, "no queries were served")
	if len(states) > 1 {
		t.Errorf("E3 violated: queries served from %d states (A=%d, B=%d)", len(states), states[swapStateA], states[swapStateB])
	}
	querier.mu.Lock()
	defer querier.mu.Unlock()
	if querier.pins != 1 || querier.releases != 1 {
		t.Errorf("expected one pin and release, got pins=%d releases=%d", querier.pins, querier.releases)
	}
}

func TestComputeAssets_ConsistentDataView(t *testing.T) {
	end := time.Now().UTC().Truncate(time.Hour)
	start := end.Add(-time.Hour)
	cm, querier := newSwapCostModel(t, start, end)

	_, err := cm.ComputeAssets(start, end)
	require.NoError(t, err)
	requireSingleState(t, querier)
}

func TestGetNetworkInsightSet_ConsistentDataView(t *testing.T) {
	end := time.Now().UTC().Truncate(time.Hour)
	start := end.Add(-time.Hour)
	cm, querier := newSwapCostModel(t, start, end)

	_, err := cm.GetNetworkInsightSet(start, end)
	require.NoError(t, err)
	requireSingleState(t, querier)
}
