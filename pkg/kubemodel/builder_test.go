package kubemodel

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/model/pb"
	kubepb "github.com/opencost/opencost/core/pkg/model/pb/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

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

// fakeDataSource implements source.OpenCostDataSource
type fakeDataSource struct {
	metricsQuerier source.MetricsQuerier
}

func (f *fakeDataSource) RegisterEndPoints(router *httprouter.Router)                   {}
func (f *fakeDataSource) RegisterDiagnostics(diagService diagnostics.DiagnosticService) {}
func (f *fakeDataSource) Metrics() source.MetricsQuerier {
	if f.metricsQuerier != nil {
		return f.metricsQuerier
	}
	return nil
}
func (f *fakeDataSource) ClusterMap() clusters.ClusterMap           { return &fakeClusterMap{} }
func (f *fakeDataSource) ClusterInfo() clusters.ClusterInfoProvider { return &fakeClusterInfo{} }
func (f *fakeDataSource) BatchDuration() time.Duration              { return time.Hour }
func (f *fakeDataSource) Resolution() time.Duration                 { return time.Minute }

// fakeMetricsQuerier implements source.MetricsQuerier with only the methods we need
type fakeMetricsQuerier struct {
	namespaceLabelsResult []*source.NamespaceLabelsResult
	namespaceLabelsError  error
}

func (f *fakeMetricsQuerier) QueryNamespaceLabels(start, end time.Time) *source.Future[source.NamespaceLabelsResult] {
	if f.namespaceLabelsError != nil {
		// For error cases, we can't use NewFutureFrom, so return an empty result
		return source.NewFutureFrom([]*source.NamespaceLabelsResult{})
	}
	return source.NewFutureFrom(f.namespaceLabelsResult)
}

// Stub implementations for all other required methods
func (f *fakeMetricsQuerier) QueryPVActiveMinutes(start, end time.Time) *source.Future[source.PVActiveMinutesResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryPVUsedAverage(start, end time.Time) *source.Future[source.PVUsedAvgResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryPVUsedMax(start, end time.Time) *source.Future[source.PVUsedMaxResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryLocalStorageActiveMinutes(start, end time.Time) *source.Future[source.LocalStorageActiveMinutesResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryLocalStorageCost(start, end time.Time) *source.Future[source.LocalStorageCostResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryLocalStorageUsedCost(start, end time.Time) *source.Future[source.LocalStorageUsedCostResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryLocalStorageUsedAvg(start, end time.Time) *source.Future[source.LocalStorageUsedAvgResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryLocalStorageUsedMax(start, end time.Time) *source.Future[source.LocalStorageUsedMaxResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryLocalStorageBytes(start, end time.Time) *source.Future[source.LocalStorageBytesResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryNodeActiveMinutes(start, end time.Time) *source.Future[source.NodeActiveMinutesResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryNodeCPUCoresCapacity(start, end time.Time) *source.Future[source.NodeCPUCoresCapacityResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryNodeCPUCoresAllocatable(start, end time.Time) *source.Future[source.NodeCPUCoresAllocatableResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryNodeRAMBytesCapacity(start, end time.Time) *source.Future[source.NodeRAMBytesCapacityResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryNodeRAMBytesAllocatable(start, end time.Time) *source.Future[source.NodeRAMBytesAllocatableResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryNodeGPUCount(start, end time.Time) *source.Future[source.NodeGPUCountResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryNodeCPUModeTotal(start, end time.Time) *source.Future[source.NodeCPUModeTotalResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryNodeIsSpot(start, end time.Time) *source.Future[source.NodeIsSpotResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryNodeRAMSystemPercent(start, end time.Time) *source.Future[source.NodeRAMSystemPercentResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryNodeRAMUserPercent(start, end time.Time) *source.Future[source.NodeRAMUserPercentResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryLBActiveMinutes(start, end time.Time) *source.Future[source.LBActiveMinutesResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryLBPricePerHr(start, end time.Time) *source.Future[source.LBPricePerHrResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryClusterManagementDuration(start, end time.Time) *source.Future[source.ClusterManagementDurationResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryClusterManagementPricePerHr(start, end time.Time) *source.Future[source.ClusterManagementPricePerHrResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryPods(start, end time.Time) *source.Future[source.PodsResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryPodsUID(start, end time.Time) *source.Future[source.PodsResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryRAMBytesAllocated(start, end time.Time) *source.Future[source.RAMBytesAllocatedResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryRAMRequests(start, end time.Time) *source.Future[source.RAMRequestsResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryRAMUsageAvg(start, end time.Time) *source.Future[source.RAMUsageAvgResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryRAMUsageMax(start, end time.Time) *source.Future[source.RAMUsageMaxResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryNodeRAMPricePerGiBHr(start, end time.Time) *source.Future[source.NodeRAMPricePerGiBHrResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryCPUCoresAllocated(start, end time.Time) *source.Future[source.CPUCoresAllocatedResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryCPURequests(start, end time.Time) *source.Future[source.CPURequestsResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryCPUUsageAvg(start, end time.Time) *source.Future[source.CPUUsageAvgResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryCPUUsageMax(start, end time.Time) *source.Future[source.CPUUsageMaxResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryNodeCPUPricePerHr(start, end time.Time) *source.Future[source.NodeCPUPricePerHrResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryGPUsAllocated(start, end time.Time) *source.Future[source.GPUsAllocatedResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryGPUsRequested(start, end time.Time) *source.Future[source.GPUsRequestedResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryGPUsUsageAvg(start, end time.Time) *source.Future[source.GPUsUsageAvgResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryGPUsUsageMax(start, end time.Time) *source.Future[source.GPUsUsageMaxResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryNodeGPUPricePerHr(start, end time.Time) *source.Future[source.NodeGPUPricePerHrResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryGPUInfo(start, end time.Time) *source.Future[source.GPUInfoResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryIsGPUShared(start, end time.Time) *source.Future[source.IsGPUSharedResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryPodPVCAllocation(start, end time.Time) *source.Future[source.PodPVCAllocationResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryPVCBytesRequested(start, end time.Time) *source.Future[source.PVCBytesRequestedResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryPVCInfo(start, end time.Time) *source.Future[source.PVCInfoResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryPVBytes(start, end time.Time) *source.Future[source.PVBytesResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryPVPricePerGiBHour(start, end time.Time) *source.Future[source.PVPricePerGiBHourResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryPVInfo(start, end time.Time) *source.Future[source.PVInfoResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryNetZoneGiB(start, end time.Time) *source.Future[source.NetZoneGiBResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryNetZonePricePerGiB(start, end time.Time) *source.Future[source.NetZonePricePerGiBResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryNetRegionGiB(start, end time.Time) *source.Future[source.NetRegionGiBResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryNetRegionPricePerGiB(start, end time.Time) *source.Future[source.NetRegionPricePerGiBResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryNetInternetGiB(start, end time.Time) *source.Future[source.NetInternetGiBResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryNetInternetPricePerGiB(start, end time.Time) *source.Future[source.NetInternetPricePerGiBResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryNetInternetServiceGiB(start, end time.Time) *source.Future[source.NetInternetServiceGiBResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryNetTransferBytes(start, end time.Time) *source.Future[source.NetTransferBytesResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryNetZoneIngressGiB(start, end time.Time) *source.Future[source.NetZoneIngressGiBResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryNetRegionIngressGiB(start, end time.Time) *source.Future[source.NetRegionIngressGiBResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryNetInternetIngressGiB(start, end time.Time) *source.Future[source.NetInternetIngressGiBResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryNetInternetServiceIngressGiB(start, end time.Time) *source.Future[source.NetInternetServiceIngressGiBResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryNetReceiveBytes(start, end time.Time) *source.Future[source.NetReceiveBytesResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryNamespaceAnnotations(start, end time.Time) *source.Future[source.NamespaceAnnotationsResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryPodAnnotations(start, end time.Time) *source.Future[source.PodAnnotationsResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryNodeLabels(start, end time.Time) *source.Future[source.NodeLabelsResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryPodLabels(start, end time.Time) *source.Future[source.PodLabelsResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryServiceLabels(start, end time.Time) *source.Future[source.ServiceLabelsResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryDeploymentLabels(start, end time.Time) *source.Future[source.DeploymentLabelsResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryStatefulSetLabels(start, end time.Time) *source.Future[source.StatefulSetLabelsResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryDaemonSetLabels(start, end time.Time) *source.Future[source.DaemonSetLabelsResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryJobLabels(start, end time.Time) *source.Future[source.JobLabelsResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryPodsWithReplicaSetOwner(start, end time.Time) *source.Future[source.PodsWithReplicaSetOwnerResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryReplicaSetsWithoutOwners(start, end time.Time) *source.Future[source.ReplicaSetsWithoutOwnersResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryReplicaSetsWithRollout(start, end time.Time) *source.Future[source.ReplicaSetsWithRolloutResult] {
	return nil
}
func (f *fakeMetricsQuerier) QueryDataCoverage(limitDays int) (time.Time, time.Time, error) {
	return time.Time{}, time.Time{}, nil
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		func() bool {
			for i := 0; i <= len(s)-len(substr); i++ {
				if s[i:i+len(substr)] == substr {
					return true
				}
			}
			return false
		}())
}

func TestNewBuilderValidation(t *testing.T) {
	t.Run("no datasource", func(t *testing.T) {
		_, err := NewBuilder(Config{})
		if !errors.Is(err, ErrNoDataSource) {
			t.Fatalf("expected ErrNoDataSource, got %v", err)
		}
	})

	t.Run("no hydrators", func(t *testing.T) {
		_, err := NewBuilder(Config{
			DataSource: &fakeDataSource{},
		})
		if !errors.Is(err, ErrNoHydrators) {
			t.Fatalf("expected ErrNoHydrators, got %v", err)
		}
	})

	noopHydrator := func(ctx context.Context, model *Model, ds source.OpenCostDataSource, start, end time.Time) error {
		return nil
	}

	t.Run("no cluster ID and no kube-system namespace", func(t *testing.T) {
		// When ClusterID is not provided and kube-system namespace can't be found,
		// NewBuilder should fail with an appropriate error
		ds := &fakeDataSource{
			metricsQuerier: &fakeMetricsQuerier{
				namespaceLabelsResult: []*source.NamespaceLabelsResult{},
				namespaceLabelsError:  nil,
			},
		}
		_, err := NewBuilder(Config{
			DataSource:  ds,
			Hydrators:   []ModelHydrator{noopHydrator},
			ClusterName: "test-cluster",
		})
		if err == nil {
			t.Fatal("expected error when cluster ID not provided and kube-system not found")
		}
		if !contains(err.Error(), "kube-system namespace UID not found") {
			t.Fatalf("expected kube-system error, got %v", err)
		}
	})

	t.Run("cluster ID defaults to kube-system UID", func(t *testing.T) {
		// When ClusterID is not provided, it should default to kube-system namespace UID
		kubeSystemUID := "abc-123-def-456"
		ds := &fakeDataSource{
			metricsQuerier: &fakeMetricsQuerier{
				namespaceLabelsResult: []*source.NamespaceLabelsResult{
					{Namespace: "default", UID: "other-uid"},
					{Namespace: "kube-system", UID: kubeSystemUID},
				},
				namespaceLabelsError: nil,
			},
		}
		builder, err := NewBuilder(Config{
			DataSource:  ds,
			Hydrators:   []ModelHydrator{noopHydrator},
			ClusterName: "test-cluster",
		})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if builder.clusterID != kubeSystemUID {
			t.Fatalf("expected cluster ID to be kube-system UID %q, got %q", kubeSystemUID, builder.clusterID)
		}
	})

	t.Run("no cluster name", func(t *testing.T) {
		_, err := NewBuilder(Config{
			DataSource:  &fakeDataSource{},
			Hydrators:   []ModelHydrator{noopHydrator},
			ClusterID:   "cluster-1",
			ClusterName: "",
		})
		if err == nil || err.Error() != "kubemodel: cluster name must be provided" {
			t.Fatalf("expected cluster name error, got %v", err)
		}
	})

	t.Run("valid config", func(t *testing.T) {
		builder, err := NewBuilder(Config{
			DataSource:  &fakeDataSource{},
			Hydrators:   []ModelHydrator{noopHydrator},
			ClusterID:   "cluster-1",
			ClusterName: "test-cluster",
		})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if builder == nil {
			t.Fatal("expected builder to be created")
		}
	})
}

func TestBuilderComputeModel(t *testing.T) {
	hydrator1 := func(ctx context.Context, model *Model, ds source.OpenCostDataSource, start, end time.Time) error {
		model.Nodes["node-1"] = &kubepb.Node{ID: "node-1", Name: "first"}
		return nil
	}

	hydrator2 := func(ctx context.Context, model *Model, ds source.OpenCostDataSource, start, end time.Time) error {
		model.Nodes["node-2"] = &kubepb.Node{ID: "node-2", Name: "second"}
		return nil
	}

	builder, err := NewBuilder(Config{
		DataSource:  &fakeDataSource{},
		Hydrators:   []ModelHydrator{hydrator1, hydrator2},
		ClusterID:   "cluster-1",
		ClusterName: "test-cluster",
		Provider:    kubepb.Provider_PROVIDER_GCP,
		Account:     "test-account",
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	ctx := context.Background()
	start := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	model, err := builder.ComputeModel(ctx, start, end)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// Check window
	if model.Window == nil {
		t.Fatal("expected model window to be set")
	}
	if model.Window.Resolution != pb.Resolution_RESOLUTION_1H {
		t.Fatalf("unexpected resolution: %v", model.Window.Resolution)
	}
	if !model.Window.Start.AsTime().Equal(start) {
		t.Fatalf("expected window start %v, got %v", start, model.Window.Start.AsTime())
	}

	// Check nodes populated by hydrators
	if len(model.Nodes) != 2 {
		t.Fatalf("expected 2 nodes from hydrators, got %d", len(model.Nodes))
	}

	// Check cluster metadata
	if model.Cluster == nil {
		t.Fatal("expected cluster information to be set")
	}
	if model.Cluster.ID != "cluster-1" {
		t.Fatalf("expected cluster ID 'cluster-1', got %q", model.Cluster.ID)
	}
	if model.Cluster.Name != "test-cluster" {
		t.Fatalf("expected cluster name 'test-cluster', got %q", model.Cluster.Name)
	}
	if model.Cluster.Provider != kubepb.Provider_PROVIDER_GCP {
		t.Fatalf("unexpected provider: %v", model.Cluster.Provider)
	}
	if model.Cluster.Account != "test-account" {
		t.Fatalf("expected account 'test-account', got %q", model.Cluster.Account)
	}
}

func TestBuilderComputeModelHydratorError(t *testing.T) {
	errorHydrator := func(ctx context.Context, model *Model, ds source.OpenCostDataSource, start, end time.Time) error {
		return errors.New("boom")
	}

	builder, err := NewBuilder(Config{
		DataSource:  &fakeDataSource{},
		Hydrators:   []ModelHydrator{errorHydrator},
		ClusterID:   "cluster-1",
		ClusterName: "test-cluster",
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	_, err = builder.ComputeModel(context.Background(), time.Now(), time.Now().Add(time.Hour))
	if err == nil {
		t.Fatal("expected error from hydrator")
	}
}

func TestBuilderComputeModelValidation(t *testing.T) {
	noopHydrator := func(ctx context.Context, model *Model, ds source.OpenCostDataSource, start, end time.Time) error {
		return nil
	}

	builder, err := NewBuilder(Config{
		DataSource:  &fakeDataSource{},
		Hydrators:   []ModelHydrator{noopHydrator},
		ClusterID:   "cluster-1",
		ClusterName: "test-cluster",
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	end := time.Now()

	t.Run("zero start time", func(t *testing.T) {
		_, err := builder.ComputeModel(context.Background(), time.Time{}, end)
		if err == nil {
			t.Fatal("expected error when start is zero")
		}
	})

	t.Run("zero end time", func(t *testing.T) {
		_, err := builder.ComputeModel(context.Background(), end, time.Time{})
		if err == nil {
			t.Fatal("expected error when end is zero")
		}
	})

	t.Run("end before start", func(t *testing.T) {
		_, err := builder.ComputeModel(context.Background(), end, end.Add(-time.Minute))
		if err == nil {
			t.Fatal("expected error when end precedes start")
		}
	})
}

func TestBuilderUnsupportedDuration(t *testing.T) {
	noopHydrator := func(ctx context.Context, model *Model, ds source.OpenCostDataSource, start, end time.Time) error {
		return nil
	}

	builder, err := NewBuilder(Config{
		DataSource:  &fakeDataSource{},
		Hydrators:   []ModelHydrator{noopHydrator},
		ClusterID:   "cluster-1",
		ClusterName: "test-cluster",
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err = builder.ComputeModel(context.Background(), start, start.Add(30*time.Minute))
	if err == nil {
		t.Fatal("expected error for unsupported duration")
	}
}

func TestDurationToResolution(t *testing.T) {
	cases := []struct {
		duration   time.Duration
		resolution pb.Resolution
	}{
		{10 * time.Minute, pb.Resolution_RESOLUTION_10M},
		{time.Hour, pb.Resolution_RESOLUTION_1H},
		{24 * time.Hour, pb.Resolution_RESOLUTION_1D},
	}

	for _, tc := range cases {
		got, err := DurationToResolution(tc.duration)
		if err != nil {
			t.Fatalf("unexpected err for %v: %v", tc.duration, err)
		}
		if got != tc.resolution {
			t.Fatalf("for %v expected %v, got %v", tc.duration, tc.resolution, got)
		}
	}
}

func TestResolutionToDuration(t *testing.T) {
	cases := []struct {
		resolution pb.Resolution
		duration   time.Duration
	}{
		{pb.Resolution_RESOLUTION_10M, 10 * time.Minute},
		{pb.Resolution_RESOLUTION_1H, time.Hour},
		{pb.Resolution_RESOLUTION_1D, 24 * time.Hour},
	}

	for _, tc := range cases {
		got, err := ResolutionToDuration(tc.resolution)
		if err != nil {
			t.Fatalf("unexpected err for %v: %v", tc.resolution, err)
		}
		if got != tc.duration {
			t.Fatalf("for %v expected %v, got %v", tc.resolution, tc.duration, got)
		}
	}
}
