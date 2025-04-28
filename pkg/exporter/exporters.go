package exporter

import (
	"time"

	export "github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/exporter/pathing"
	"github.com/opencost/opencost/core/pkg/exporter/validator"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/pipelines"
	"github.com/opencost/opencost/core/pkg/storage"
)

func NewAllocationStorageExporter(clusterId string, resolution time.Duration, store storage.Storage) export.ComputeExporter[opencost.AllocationSet] {
	pathing, err := pathing.NewBingenStoragePathFormatter("", clusterId, pipelines.AllocationPipelineName, &resolution)
	if err != nil {
		log.Errorf("failed to create pathing formatter: %v", err)
		return nil
	}

	return export.NewComputeStorageExporter(
		pipelines.AllocationPipelineName,
		resolution,
		pathing,
		NewAllocationEncoder(),
		store,
		validator.NewSetValidator[opencost.AllocationSet](resolution),
	)
}

func NewAssetsStorageExporter(clusterId string, resolution time.Duration, store storage.Storage) export.ComputeExporter[opencost.AssetSet] {
	pathing, err := pathing.NewBingenStoragePathFormatter("", clusterId, pipelines.AssetsPipelineName, &resolution)
	if err != nil {
		log.Errorf("failed to create pathing formatter: %v", err)
		return nil
	}

	return export.NewComputeStorageExporter(
		pipelines.AssetsPipelineName,
		resolution,
		pathing,
		NewAssetsEncoder(),
		store,
		validator.NewSetValidator[opencost.AssetSet](resolution),
	)
}

func NewNetworkInsightStorageExporter(clusterId string, resolution time.Duration, store storage.Storage) export.ComputeExporter[opencost.NetworkInsightSet] {
	pathing, err := pathing.NewBingenStoragePathFormatter("", clusterId, pipelines.NetworkInsightPipelineName, &resolution)
	if err != nil {
		log.Errorf("failed to create pathing formatter: %v", err)
		return nil
	}

	return export.NewComputeStorageExporter(
		pipelines.NetworkInsightPipelineName,
		resolution,
		pathing,
		NewNetworkInsightEncoder(),
		store,
		validator.NewSetValidator[opencost.NetworkInsightSet](resolution),
	)
}
