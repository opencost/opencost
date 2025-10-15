package kubemodel

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/opencost/opencost/core/pkg/model/pb"
	kubepb "github.com/opencost/opencost/core/pkg/model/pb/kubemodel"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/julienschmidt/httprouter"
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
type fakeDataSource struct{}

func (f *fakeDataSource) RegisterEndPoints(router *httprouter.Router)           {}
func (f *fakeDataSource) RegisterDiagnostics(diagService diagnostics.DiagnosticService) {}
func (f *fakeDataSource) Metrics() source.MetricsQuerier                        { return nil }
func (f *fakeDataSource) ClusterMap() clusters.ClusterMap                       { return &fakeClusterMap{} }
func (f *fakeDataSource) ClusterInfo() clusters.ClusterInfoProvider             { return &fakeClusterInfo{} }
func (f *fakeDataSource) BatchDuration() time.Duration                          { return time.Hour }
func (f *fakeDataSource) Resolution() time.Duration                             { return time.Minute }

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

	t.Run("no cluster ID", func(t *testing.T) {
		_, err := NewBuilder(Config{
			DataSource: &fakeDataSource{},
			Hydrators:  []ModelHydrator{noopHydrator},
		})
		if err == nil || err.Error() != "kubemodel: cluster ID must be provided" {
			t.Fatalf("expected cluster ID error, got %v", err)
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