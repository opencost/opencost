package exporter

import (
	"time"

	export "github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/pkg/costmodel"
	"github.com/opencost/opencost/pkg/exporter/allocation"
	"github.com/opencost/opencost/pkg/exporter/asset"
	"github.com/opencost/opencost/pkg/exporter/networkinsight"
)

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
func NewPipelineExportControllers(clusterId string, store storage.Storage, cm *costmodel.CostModel, config *PipelinesExportConfig) *PipelineExportControllers {
	if config == nil {
		config = DefaultPipelinesExportConfig()
	}

	mins := int(cm.DataSource.Resolution().Minutes())
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

		allocExporter := NewAllocationStorageExporter(clusterId, res, store)
		allocController := export.NewComputeExportController(allocSource, allocExporter, sourceResolution)

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
		assetExporter := NewAssetsStorageExporter(clusterId, res, store)
		assetController := export.NewComputeExportController(assetSource, assetExporter, sourceResolution)

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

		networkInsightExporter := NewNetworkInsightStorageExporter(clusterId, res, store)
		networkInsightController := export.NewComputeExportController(networkInsightSource, networkInsightExporter, sourceResolution)

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
