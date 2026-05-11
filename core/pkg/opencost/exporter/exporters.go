package exporter

import (
	"fmt"
	"time"

	export "github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/exporter/pathing"
	"github.com/opencost/opencost/core/pkg/exporter/validator"
	"github.com/opencost/opencost/core/pkg/pipelines"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"github.com/opencost/opencost/core/pkg/util/typeutil"
)

// NewComputePipelineExporter creates a new `ComputeExporter[T]` instance which is used to export computed data
// by window for a specific pipeline.
func NewComputePipelineExporter[T any, U export.BinaryMarshalerPtr[T], S validator.SetConstraint[T]](
	clusterName string,
	resolution time.Duration,
	store storage.Storage,
) (export.ComputeExporter[T], error) {
	pipelineName := pipelines.NameFor[T]()
	if pipelineName == "" {
		return nil, fmt.Errorf("failed to extract pipeline name for type: %s", typeutil.TypeOf[T]())
	}

	pathing, err := pathing.NewDefaultStoragePathFormatter(clusterName, pipelineName, &resolution)
	if err != nil {
		return nil, fmt.Errorf("failed to create path formatter: %w", err)
	}

	var encoder export.Encoder[T]
	encoder = export.NewBingenEncoder[T, U]()

	return export.NewComputeStorageExporter(
		pathing,
		encoder,
		store,
		validator.NewSetValidator[T, S](resolution),
	), nil
}

// NewComputePipelineExportController creates a new `ComputeExportController[T]` instance which is used to export computed data
// using the provided source, storage, resolution, and source resolution.
func NewComputePipelineExportController[T any, U export.BinaryMarshalerPtr[T], S validator.SetConstraint[T]](
	clusterName string,
	store storage.Storage,
	source export.ComputeSource[T],
	resolution time.Duration,
) (*export.ComputeExportController[T], error) {
	exporter, err := NewComputePipelineExporter[T, U, S](clusterName, resolution, store)
	if err != nil {
		return nil, fmt.Errorf("failed to create compute exporter: %w", err)
	}

	return export.NewComputeExportController(source, exporter, resolution), nil
}

// NewKubeModelComputePipelineExporter creates a new `ComputeExporter[T]` instance which is used to export computed data
// by window for a specific pipeline.
func NewKubeModelComputePipelineExporter[T any, U export.BinaryMarshalerPtr[T], S validator.SetConstraint[T]](
	appName string,
	clusterId string,
	resolution time.Duration,
	store storage.Storage,
) (export.ComputeExporter[T], error) {
	pipelineName := pipelines.NameFor[T]()
	if pipelineName == "" {
		return nil, fmt.Errorf("failed to extract pipeline name for type: %s", typeutil.TypeOf[T]())
	}
	res := timeutil.FormatStoreResolution(resolution)
	pathing, err := pathing.NewKubeModelStoragePathFormatter(appName, clusterId, res)
	if err != nil {
		return nil, fmt.Errorf("failed to create path formatter: %w", err)
	}

	encoder := export.NewBingenFileEncoder[T, U]()

	return export.NewComputeStorageExporter(
		pathing,
		encoder,
		store,
		validator.NewSetValidator[T, S](resolution),
	), nil
}

// NewKubeModelComputePipelineExportController creates a new `ComputeExportController[T]` instance which is used to export computed data
// using the provided source, storage, resolution, and source resolution.
func NewKubeModelComputePipelineExportController[T any, U export.BinaryMarshalerPtr[T], S validator.SetConstraint[T]](
	appName string,
	clusterId string,
	store storage.Storage,
	source export.ComputeSource[T],
	resolution time.Duration,
) (*export.ComputeExportController[T], error) {
	exporter, err := NewKubeModelComputePipelineExporter[T, U, S](appName, clusterId, resolution, store)
	if err != nil {
		return nil, fmt.Errorf("failed to create compute exporter: %w", err)
	}

	return export.NewComputeExportController(source, exporter, resolution), nil
}
