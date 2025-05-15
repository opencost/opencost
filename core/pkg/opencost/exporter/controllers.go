package exporter

import (
	"time"

	export "github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/opencost/exporter/allocation"
	"github.com/opencost/opencost/core/pkg/opencost/exporter/asset"
	"github.com/opencost/opencost/core/pkg/opencost/exporter/networkinsight"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

// ComputePipelineSource is an interface that defines methods for computing all pipeline data.
// For all intents and purposes, this represents costmodel.CostModel. To interface allows tests to
// mock the costmodel.CostModel and return a different source for the pipeline.
type ComputePipelineSource interface {
	allocation.AllocationSource
	asset.AssetSource
	networkinsight.NetworkInsightSource

	GetDataSource() source.OpenCostDataSource
}

// PipelinesExportConfig is a configuration struct that contains the export resolutions for
// allocation, assets, and network insights pipelines.
type PipelinesExportConfig struct {
	AllocationPiplineResolutions      []time.Duration
	AssetPipelineResolutons           []time.Duration
	NetworkInsightPipelineResolutions []time.Duration
}

// defaultPipelineExportResolutions returns the default export configuration for the pipeline
// which is set to export hourly and daily.
func defaultPipelineExportResolutions() []time.Duration {
	return []time.Duration{
		time.Hour,
		24 * time.Hour,
	}
}

// DefaultPipelinesExportConfig returns the default export configuration for all pipelines
// which is set to export hourly and daily for allocations, assets, and network insights.
func DefaultPipelinesExportConfig() *PipelinesExportConfig {
	return &PipelinesExportConfig{
		AllocationPiplineResolutions:      defaultPipelineExportResolutions(),
		AssetPipelineResolutons:           defaultPipelineExportResolutions(),
		NetworkInsightPipelineResolutions: defaultPipelineExportResolutions(),
	}
}

// PipelineExportControllers is a facade that contains the export controllers for allocations, assets, and network insights.
type PipelineExportControllers struct {
	AllocationExportController     *export.ComputeExportControllerGroup[opencost.AllocationSet]
	AssetExportController          *export.ComputeExportControllerGroup[opencost.AssetSet]
	NetworkInsightExportController *export.ComputeExportControllerGroup[opencost.NetworkInsightSet]
}

// NewPipelineExportControllers creates a new PipelineExportControllers instance with the given cluster ID, storage implementation, cost model, and configuration.
// Setting the config to nil will use the default hourly and daily export resolutions for each pipeline.
func NewPipelineExportControllers(clusterId string, store storage.Storage, cm ComputePipelineSource, config *PipelinesExportConfig) *PipelineExportControllers {
	if config == nil {
		config = DefaultPipelinesExportConfig()
	}

	mins := int(cm.GetDataSource().Resolution().Minutes())
	if mins <= 0 {
		mins = 1
	}

	// minimum source/query resolution
	sourceResolution := time.Duration(mins) * time.Minute

	// allocation sources and exporters
	allocSource := allocation.NewAllocationComputeSource(cm)
	allocExportControllers := []*export.ComputeExportController[opencost.AllocationSet]{}

	for _, res := range config.AllocationPiplineResolutions {
		if res < sourceResolution {
			log.Warnf("Configured allocation pipeline resolution %dm is less than source resolution %dm. Not configuring the exporter for this resolution.", int64(res.Minutes()), int64(sourceResolution.Minutes()))
			continue
		}

		allocController, err := NewComputePipelineExportController(clusterId, store, allocSource, res, sourceResolution)
		if err != nil {
			log.Errorf("Failed to create allocation export controller for resolution: %s - %v", timeutil.DurationString(res), err)
			continue
		}

		allocExportControllers = append(allocExportControllers, allocController)
	}

	// asset sources and exporters
	assetSource := asset.NewAssetsComputeSource(cm)
	assetExportControllers := []*export.ComputeExportController[opencost.AssetSet]{}

	for _, res := range config.AssetPipelineResolutons {
		if res < sourceResolution {
			log.Warnf("Configured asset pipeline resolution %dm is less than source resolution %dm. Not configuring the exporter for this resolution.", int64(res.Minutes()), int64(sourceResolution.Minutes()))
			continue
		}

		assetController, err := NewComputePipelineExportController(clusterId, store, assetSource, res, sourceResolution)
		if err != nil {
			log.Errorf("Failed to create asset export controller for resolution: %s - %v", timeutil.DurationString(res), err)
			continue
		}

		assetExportControllers = append(assetExportControllers, assetController)
	}

	// network insights sources and exporters
	networkInsightSource := networkinsight.NewNetworkInsightsComputeSource(cm)
	networkInsightExportControllers := []*export.ComputeExportController[opencost.NetworkInsightSet]{}

	for _, res := range config.NetworkInsightPipelineResolutions {
		if res < sourceResolution {
			log.Warnf("Configured network insight pipeline resolution %dm is less than source resolution %dm. Not configuring the exporter for this resolution.", int64(res.Minutes()), int64(sourceResolution.Minutes()))
			continue
		}

		networkInsightController, err := NewComputePipelineExportController(clusterId, store, networkInsightSource, res, sourceResolution)
		if err != nil {
			log.Errorf("Failed to create network insight export controller for resolution: %s - %v", timeutil.DurationString(res), err)
			continue
		}

		networkInsightExportControllers = append(networkInsightExportControllers, networkInsightController)
	}

	return &PipelineExportControllers{
		AllocationExportController:     export.NewComputeExportControllerGroup(allocExportControllers...),
		AssetExportController:          export.NewComputeExportControllerGroup(assetExportControllers...),
		NetworkInsightExportController: export.NewComputeExportControllerGroup(networkInsightExportControllers...),
	}
}

func (pec *PipelineExportControllers) Start(interval time.Duration) {
	pec.AllocationExportController.Start(interval)
	pec.AssetExportController.Start(interval)
	pec.NetworkInsightExportController.Start(interval)
}

func (pec *PipelineExportControllers) Stop() {
	pec.AllocationExportController.Stop()
	pec.AssetExportController.Stop()
	pec.NetworkInsightExportController.Stop()
}
