package exporter

import (
	"testing"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/exporter/pathing"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/pipelines"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/storage"
)

const (
	TestClusterId  = "test-cluster"
	TestResolution = 24 * time.Hour
)

type GenerateMockSet[T any] func(start, end time.Time) *T

type MockSource[T any] struct {
	generate GenerateMockSet[T]
}

func (ms *MockSource[T]) CanCompute(start, end time.Time) bool {
	return true
}
func (ms *MockSource[T]) Compute(start, end time.Time) (*T, error) {
	return ms.generate(start, end), nil
}
func (ms *MockSource[T]) Name() string {
	return pipelines.NameFor[T]()
}

func NewMockAllocationSource() exporter.ComputeSource[opencost.AllocationSet] {
	return &MockSource[opencost.AllocationSet]{
		generate: func(start, end time.Time) *opencost.AllocationSet { return opencost.GenerateMockAllocationSet(start) },
	}
}

func NewMockAssetSource() exporter.ComputeSource[opencost.AssetSet] {
	return &MockSource[opencost.AssetSet]{
		generate: func(start, end time.Time) *opencost.AssetSet {
			return opencost.GenerateMockAssetSet(start, TestResolution)
		},
	}
}

func NewMockNetworkInsightSource() exporter.ComputeSource[opencost.NetworkInsightSet] {
	return &MockSource[opencost.NetworkInsightSet]{
		generate: func(start, end time.Time) *opencost.NetworkInsightSet {
			return opencost.GenerateMockNetworkInsightSet(start, end)
		},
	}
}

func NewMockKubeModelSource() exporter.ComputeSource[kubemodel.KubeModelSet] {
	return &MockSource[kubemodel.KubeModelSet]{
		generate: func(start, end time.Time) *kubemodel.KubeModelSet {
			return opencost.GenerateMockKubeModelSet(start, end)
		},
	}
}

type MockDataSource struct {
	resolution time.Duration
}

func NewMockDataSource() *MockDataSource {
	return NewMockDataSourceWith(time.Minute)
}

func NewMockDataSourceWith(resolution time.Duration) *MockDataSource {
	return &MockDataSource{
		resolution: resolution,
	}
}

func (mds *MockDataSource) RegisterEndPoints(router *httprouter.Router)                   {}
func (mds *MockDataSource) RegisterDiagnostics(diagService diagnostics.DiagnosticService) {}
func (mds *MockDataSource) Metrics() source.MetricsQuerier                                { return nil }
func (mds *MockDataSource) ClusterMap() clusters.ClusterMap                               { return nil }
func (mds *MockDataSource) ClusterInfo() clusters.ClusterInfoProvider                     { return nil }
func (mds *MockDataSource) BatchDuration() time.Duration                                  { return time.Hour * 20000 }
func (mds *MockDataSource) Resolution() time.Duration                                     { return mds.resolution }

type MockPipelineComputeSource struct {
	allocSource     exporter.ComputeSource[opencost.AllocationSet]
	assetSource     exporter.ComputeSource[opencost.AssetSet]
	netSource       exporter.ComputeSource[opencost.NetworkInsightSet]
	kubeModelSource exporter.ComputeSource[kubemodel.KubeModelSet]
	ds              *MockDataSource
}

func NewMockPipelineComputeSource() *MockPipelineComputeSource {
	return &MockPipelineComputeSource{
		allocSource:     NewMockAllocationSource(),
		assetSource:     NewMockAssetSource(),
		netSource:       NewMockNetworkInsightSource(),
		kubeModelSource: NewMockKubeModelSource(),
		ds:              NewMockDataSource(),
	}
}

func NewMockPipelineComputeSourceWith(srcResolution time.Duration) *MockPipelineComputeSource {
	return &MockPipelineComputeSource{
		allocSource:     NewMockAllocationSource(),
		assetSource:     NewMockAssetSource(),
		netSource:       NewMockNetworkInsightSource(),
		kubeModelSource: NewMockKubeModelSource(),
		ds:              NewMockDataSourceWith(srcResolution),
	}
}

func (mpcs *MockPipelineComputeSource) ComputeAllocation(start, end time.Time) (*opencost.AllocationSet, error) {
	return mpcs.allocSource.Compute(start, end)
}
func (mpcs *MockPipelineComputeSource) ComputeAssets(start, end time.Time) (*opencost.AssetSet, error) {
	return mpcs.assetSource.Compute(start, end)
}
func (mpcs *MockPipelineComputeSource) ComputeNetworkInsights(start, end time.Time) (*opencost.NetworkInsightSet, error) {
	return mpcs.netSource.Compute(start, end)
}
func (mpcs *MockPipelineComputeSource) ComputeKubeModelSet(start, end time.Time) (*kubemodel.KubeModelSet, error) {
	return mpcs.kubeModelSource.Compute(start, end)
}
func (mpcs *MockPipelineComputeSource) GetDataSource() source.OpenCostDataSource {
	return mpcs.ds
}

type UnknownSet struct{}

func (u *UnknownSet) MarshalBinary() ([]byte, error) {
	return []byte{}, nil
}
func (u *UnknownSet) UnmarshalBinary(data []byte) error {
	return nil
}
func (u *UnknownSet) IsEmpty() bool {
	return false
}

type PipelineData[T any] interface {
	UnmarshalBinary(data []byte) error
	IsEmpty() bool
	*T
}

func ptr[T any](v T) *T {
	return &v
}

func TestExporters(t *testing.T) {
	t.Run("allocation exporter", func(t *testing.T) {
		allocSource := NewMockAllocationSource()
		memStore := storage.NewMemoryStorage()
		p, err := pathing.NewDefaultStoragePathFormatter(TestClusterId, pipelines.AllocationPipelineName, ptr(TestResolution))
		if err != nil {
			t.Fatalf("failed to create path formatter: %v", err)
		}

		allocExporter, err := NewComputePipelineExporter[opencost.AllocationSet](TestClusterId, TestResolution, memStore)
		if err != nil {
			t.Fatalf("failed to create allocation exporter: %v", err)
		}

		end := time.Now().UTC().Truncate(TestResolution)
		start := end.Add(-TestResolution)

		data, err := allocSource.Compute(start, end)
		if err != nil {
			t.Fatalf("failed to compute allocation data: %v", err)
		}

		err = allocExporter.Export(opencost.NewClosedWindow(start, end), data)
		if err != nil {
			t.Fatalf("failed to export allocation data: %v", err)
		}

		validateFileCreation[opencost.AllocationSet](t, memStore, p, start, end)
	})

	t.Run("asset exporter", func(t *testing.T) {
		assetSource := NewMockAssetSource()
		memStore := storage.NewMemoryStorage()
		p, err := pathing.NewDefaultStoragePathFormatter(TestClusterId, pipelines.AssetsPipelineName, ptr(TestResolution))
		if err != nil {
			t.Fatalf("failed to create path formatter: %v", err)
		}

		assetExporter, err := NewComputePipelineExporter[opencost.AssetSet](TestClusterId, TestResolution, memStore)
		if err != nil {
			t.Fatalf("failed to create allocation exporter: %v", err)
		}

		end := time.Now().UTC().Truncate(TestResolution)
		start := end.Add(-TestResolution)

		data, err := assetSource.Compute(start, end)
		if err != nil {
			t.Fatalf("failed to compute asset data: %v", err)
		}

		err = assetExporter.Export(opencost.NewClosedWindow(start, end), data)
		if err != nil {
			t.Fatalf("failed to export asset data: %v", err)
		}

		validateFileCreation[opencost.AssetSet](t, memStore, p, start, end)
	})

	t.Run("network insight exporter", func(t *testing.T) {
		netInsightSource := NewMockNetworkInsightSource()
		memStore := storage.NewMemoryStorage()
		p, err := pathing.NewDefaultStoragePathFormatter(TestClusterId, pipelines.NetworkInsightPipelineName, ptr(TestResolution))
		if err != nil {
			t.Fatalf("failed to create path formatter: %v", err)
		}

		netInsightExporter, err := NewComputePipelineExporter[opencost.NetworkInsightSet](TestClusterId, TestResolution, memStore)
		if err != nil {
			t.Fatalf("failed to create net insights exporter: %v", err)
		}

		end := time.Now().UTC().Truncate(TestResolution)
		start := end.Add(-TestResolution)

		data, err := netInsightSource.Compute(start, end)
		if err != nil {
			t.Fatalf("failed to compute net insights data: %v", err)
		}

		err = netInsightExporter.Export(opencost.NewClosedWindow(start, end), data)
		if err != nil {
			t.Fatalf("failed to export net insights data: %v", err)
		}

		validateFileCreation[opencost.NetworkInsightSet](t, memStore, p, start, end)
	})

	t.Run("KubeModel exporter", func(t *testing.T) {
		kubeModelSource := NewMockKubeModelSource()
		memStore := storage.NewMemoryStorage()
		p, err := pathing.NewDefaultStoragePathFormatter(TestClusterId, pipelines.KubeModelPipelineName, ptr(TestResolution))
		if err != nil {
			t.Fatalf("failed to create path formatter: %v", err)
		}

		kubeModelExporter, err := NewComputePipelineExporter[kubemodel.KubeModelSet](TestClusterId, TestResolution, memStore)
		if err != nil {
			t.Fatalf("failed to create KubeModel exporter: %v", err)
		}

		end := time.Now().UTC().Truncate(TestResolution)
		start := end.Add(-TestResolution)

		data, err := kubeModelSource.Compute(start, end)
		if err != nil {
			t.Fatalf("failed to compute KubeModel data: %v", err)
		}

		err = kubeModelExporter.Export(opencost.NewClosedWindow(start, end), data)
		if err != nil {
			t.Fatalf("failed to export KubeModel data: %v", err)
		}

		validateFileCreation[kubemodel.KubeModelSet](t, memStore, p, start, end)
	})

	t.Run("unknown exporter", func(t *testing.T) {
		memStore := storage.NewMemoryStorage()

		// Invalid pipeline
		_, err := NewComputePipelineExporter[UnknownSet](TestClusterId, TestResolution, memStore)
		if err == nil {
			t.Fatalf("expected error creating unknown pipeline exporter, got nil")
		}

		// Invalid cluster id
		_, err = NewComputePipelineExporter[opencost.AllocationSet]("", TestResolution, memStore)
		if err == nil {
			t.Fatalf("expected error creating allocation pipeline exporter with empty cluster id, got nil")
		}
	})
}

func TestPipelineExportControllers(t *testing.T) {
	t.Run("with custom export config", func(t *testing.T) {
		pipelineComputeSource := NewMockPipelineComputeSource()
		memStore := storage.NewMemoryStorage()

		exportControllers := NewPipelineExportControllers(memStore, pipelineComputeSource, PipelinesExportConfig{
			ClusterUID:                        TestClusterId,
			ClusterName:                       TestClusterId,
			AllocationPiplineResolutions:      []time.Duration{TestResolution},
			AssetPipelineResolutons:           []time.Duration{TestResolution},
			NetworkInsightPipelineResolutions: []time.Duration{TestResolution},
		})

		start := time.Now().UTC().Truncate(TestResolution)
		end := start.Add(TestResolution)

		// allow a single export to occur
		exportControllers.Start(time.Second)
		time.Sleep(time.Second + (750 * time.Millisecond))
		exportControllers.Stop()

		allocPath, err := pathing.NewDefaultStoragePathFormatter(TestClusterId, pipelines.AllocationPipelineName, ptr(TestResolution))
		if err != nil {
			t.Fatalf("failed to create allocations path formatter: %v", err)
		}
		assetPath, err := pathing.NewDefaultStoragePathFormatter(TestClusterId, pipelines.AssetsPipelineName, ptr(TestResolution))
		if err != nil {
			t.Fatalf("failed to create assets path formatter: %v", err)
		}
		netPath, err := pathing.NewDefaultStoragePathFormatter(TestClusterId, pipelines.NetworkInsightPipelineName, ptr(TestResolution))
		if err != nil {
			t.Fatalf("failed to create net insights path formatter: %v", err)
		}

		validateFileCreation[opencost.AllocationSet](t, memStore, allocPath, start, end)
		validateFileCreation[opencost.AssetSet](t, memStore, assetPath, start, end)
		validateFileCreation[opencost.NetworkInsightSet](t, memStore, netPath, start, end)
	})

	t.Run("with auto-set to minute resolution", func(t *testing.T) {
		pipelineComputeSource := NewMockPipelineComputeSourceWith(30 * time.Second)
		memStore := storage.NewMemoryStorage()

		exportControllers := NewPipelineExportControllers(memStore, pipelineComputeSource, PipelinesExportConfig{
			ClusterUID:                        TestClusterId,
			ClusterName:                       TestClusterId,
			AllocationPiplineResolutions:      []time.Duration{TestResolution},
			AssetPipelineResolutons:           []time.Duration{TestResolution},
			NetworkInsightPipelineResolutions: []time.Duration{TestResolution},
		})

		start := time.Now().UTC().Truncate(TestResolution)
		end := start.Add(TestResolution)

		// allow a single export to occur
		exportControllers.Start(time.Second)
		time.Sleep(time.Second + (750 * time.Millisecond))
		exportControllers.Stop()

		allocPath, err := pathing.NewDefaultStoragePathFormatter(TestClusterId, pipelines.AllocationPipelineName, ptr(TestResolution))
		if err != nil {
			t.Fatalf("failed to create allocations path formatter: %v", err)
		}
		assetPath, err := pathing.NewDefaultStoragePathFormatter(TestClusterId, pipelines.AssetsPipelineName, ptr(TestResolution))
		if err != nil {
			t.Fatalf("failed to create assets path formatter: %v", err)
		}
		netPath, err := pathing.NewDefaultStoragePathFormatter(TestClusterId, pipelines.NetworkInsightPipelineName, ptr(TestResolution))
		if err != nil {
			t.Fatalf("failed to create net insights path formatter: %v", err)
		}

		validateFileCreation[opencost.AllocationSet](t, memStore, allocPath, start, end)
		validateFileCreation[opencost.AssetSet](t, memStore, assetPath, start, end)
		validateFileCreation[opencost.NetworkInsightSet](t, memStore, netPath, start, end)
	})

	t.Run("with default export config", func(t *testing.T) {
		pipelineComputeSource := NewMockPipelineComputeSource()
		memStore := storage.NewMemoryStorage()

		exportControllers := NewPipelineExportControllers(memStore, pipelineComputeSource, NewPipelinesExportConfig(TestClusterId, TestClusterId))

		if len(exportControllers.AllocationExportController.Resolutions()) != 2 {
			t.Fatalf("expected 2 allocation resolutions, got %d", len(exportControllers.AllocationExportController.Resolutions()))
		}
		if len(exportControllers.AssetExportController.Resolutions()) != 2 {
			t.Fatalf("expected 2 asset resolutions, got %d", len(exportControllers.AssetExportController.Resolutions()))
		}
		if len(exportControllers.NetworkInsightExportController.Resolutions()) != 2 {
			t.Fatalf("expected 2 network insight resolutions, got %d", len(exportControllers.NetworkInsightExportController.Resolutions()))
		}
	})

	t.Run("with 2day source resolution", func(t *testing.T) {
		// make compute source use a source resolution of 48 hours
		pipelineComputeSource := NewMockPipelineComputeSourceWith(48 * time.Hour)
		memStore := storage.NewMemoryStorage()

		exportControllers := NewPipelineExportControllers(memStore, pipelineComputeSource, NewPipelinesExportConfig(TestClusterId, TestClusterId))

		if len(exportControllers.AllocationExportController.Resolutions()) != 0 {
			t.Fatalf("expected 0 allocation resolutions, got %d", len(exportControllers.AllocationExportController.Resolutions()))
		}
		if len(exportControllers.AssetExportController.Resolutions()) != 0 {
			t.Fatalf("expected 0 asset resolutions, got %d", len(exportControllers.AssetExportController.Resolutions()))
		}
		if len(exportControllers.NetworkInsightExportController.Resolutions()) != 0 {
			t.Fatalf("expected 0 network insight resolutions, got %d", len(exportControllers.NetworkInsightExportController.Resolutions()))
		}
	})

	t.Run("with empty cluster id", func(t *testing.T) {
		pipelineComputeSource := NewMockPipelineComputeSource()
		memStore := storage.NewMemoryStorage()

		exportControllers := NewPipelineExportControllers(memStore, pipelineComputeSource, NewPipelinesExportConfig("", ""))

		if len(exportControllers.AllocationExportController.Resolutions()) != 0 {
			t.Fatalf("expected 0 allocation resolutions, got %d", len(exportControllers.AllocationExportController.Resolutions()))
		}
		if len(exportControllers.AssetExportController.Resolutions()) != 0 {
			t.Fatalf("expected 0 asset resolutions, got %d", len(exportControllers.AssetExportController.Resolutions()))
		}
		if len(exportControllers.NetworkInsightExportController.Resolutions()) != 0 {
			t.Fatalf("expected 0 network insight resolutions, got %d", len(exportControllers.NetworkInsightExportController.Resolutions()))
		}
	})
}

// test helper function that will load a path from a storage implementation and ensure that the file is not empty and can be decoded, etc...
func validateFileCreation[T any, U PipelineData[T]](t *testing.T, memStore storage.Storage, p pathing.StoragePathFormatter[opencost.Window], start, end time.Time) {
	t.Helper()

	expectedPath := p.ToFullPath("", opencost.NewClosedWindow(start, end), "")

	fileContents, err := memStore.Read(expectedPath)
	if err != nil {
		t.Fatalf("failed to read file %s: %v", expectedPath, err)
	}
	if len(fileContents) == 0 {
		t.Fatalf("file %s is empty", expectedPath)
	}

	var set U = new(T)
	err = set.UnmarshalBinary(fileContents)
	if err != nil {
		t.Fatalf("failed to unmarshal data: %v", err)
	}

	if set.IsEmpty() {
		t.Fatalf("data set is empty")
	}
}
