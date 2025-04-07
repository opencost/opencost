package exporter

import (
	"time"

	export "github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/pkg/costmodel"
	"github.com/opencost/opencost/pkg/exporter/allocation"
	"github.com/opencost/opencost/pkg/exporter/asset"
	"github.com/opencost/opencost/pkg/exporter/networkinsight"
)

type PipelineExportControllers struct {
	AllocationExportController     *export.ComputeExportControllerGroup[opencost.AllocationSet]
	AssetExportController          *export.ComputeExportControllerGroup[opencost.AssetSet]
	NetworkInsightExportController *export.ComputeExportControllerGroup[opencost.NetworkInsightSet]
}

func NewPipelineExportControllers(clusterId string, store storage.Storage, cm *costmodel.CostModel) *PipelineExportControllers {
	mins := int(cm.DataSource.Resolution().Minutes())
	if mins <= 0 {
		mins = 1
	}

	// minimum source/query resolution
	sourceResolution := time.Duration(mins) * time.Minute

	// allocation sources and exporters
	allocSource := allocation.NewAllocationComputeSource(cm)
	hourlyAllocExporter := NewAllocationStorageExporter(clusterId, time.Hour, store)
	dailyAllocExporter := NewAllocationStorageExporter(clusterId, 24*time.Hour, store)

	hourlyAllocController := export.NewComputeExportController(allocSource, hourlyAllocExporter, sourceResolution)
	dailyAllocController := export.NewComputeExportController(allocSource, dailyAllocExporter, sourceResolution)

	allocController := export.NewComputeExportControllerGroup(hourlyAllocController, dailyAllocController)

	// asset sources and exporters
	assetSource := asset.NewAssetsComputeSource(cm)
	hourlyAssetExporter := NewAssetsStorageExporter(clusterId, time.Hour, store)
	dailyAssetExporter := NewAssetsStorageExporter(clusterId, 24*time.Hour, store)

	hourlyAssetController := export.NewComputeExportController(assetSource, hourlyAssetExporter, sourceResolution)
	dailyAssetController := export.NewComputeExportController(assetSource, dailyAssetExporter, sourceResolution)

	assetController := export.NewComputeExportControllerGroup(hourlyAssetController, dailyAssetController)

	// network insights sources and exporters
	networkInsightSource := networkinsight.NewNetworkInsightsComputeSource(cm)
	hourlyNetworkInsightExporter := NewNetworkInsightStorageExporter(clusterId, time.Hour, store)
	dailyNetworkInsightExporter := NewNetworkInsightStorageExporter(clusterId, 24*time.Hour, store)

	hourlyNetworkInsightController := export.NewComputeExportController(networkInsightSource, hourlyNetworkInsightExporter, sourceResolution)
	dailyNetworkInsightController := export.NewComputeExportController(networkInsightSource, dailyNetworkInsightExporter, sourceResolution)

	networkInsightController := export.NewComputeExportControllerGroup(hourlyNetworkInsightController, dailyNetworkInsightController)

	return &PipelineExportControllers{
		AllocationExportController:     allocController,
		AssetExportController:          assetController,
		NetworkInsightExportController: networkInsightController,
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
