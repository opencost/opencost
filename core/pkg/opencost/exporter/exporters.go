package exporter

import (
	"compress/gzip"
	"fmt"
	"time"

	export "github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/exporter/pathing"
	"github.com/opencost/opencost/core/pkg/exporter/validator"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/pipelines"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"github.com/opencost/opencost/core/pkg/util/typeutil"
)

// ExportCompressionLevel is an enumeration value for allowing a streaming compute exporter to enable
// compression at specific gzip levels.
type ExportCompressionLevel int

// IsValid returns false when the integer value of the `ExportCompressionLevel` isn't a valid input.
func (ecl ExportCompressionLevel) IsValid() bool {
	// level is default or none
	if ecl == ExportCompressionLevelNone || ecl == ExportCompressionLevelDefault {
		return true
	}

	// level is within 1-9 bounds
	return ecl >= ExportCompressionLevelBestSpeed && ecl <= ExportCompressionLevelBestCompression
}

const (
	ExportCompressionLevelNone            ExportCompressionLevel = gzip.NoCompression
	ExportCompressionLevelBestSpeed       ExportCompressionLevel = gzip.BestSpeed
	ExportCompressionLevelBestCompression ExportCompressionLevel = gzip.BestCompression
	ExportCompressionLevelDefault         ExportCompressionLevel = gzip.DefaultCompression
)

type ComputeExporterConfig struct {
	AppName     string
	ClusterUID  string
	ClusterName string
	Resolution  time.Duration
	Streaming   bool
	Compression ExportCompressionLevel
}

// NewComputePipelineExporter creates a new `ComputeExporter[T]` instance which is used to export computed data
// by window for a specific pipeline.
func NewComputePipelineExporter[T any, U export.BinaryMarshalerPtr[T], S validator.SetConstraint[T]](
	config ComputeExporterConfig,
	store storage.Storage,
) (export.ComputeExporter[T], error) {

	pathing, err := GetExporterPathing[T, U, S](config)
	if err != nil {
		return nil, fmt.Errorf("failed to create path formatter: %w", err)
	}

	if !config.Compression.IsValid() {
		return nil, fmt.Errorf("invalid compression level passed: %d is not a valid compression level", int(config.Compression))
	}

	var encoder export.Encoder[T]
	if config.Streaming && config.Compression != ExportCompressionLevelNone {
		encoder = export.NewGZipEncoderWithLevel(export.NewBingenEncoder[T, U](), int(config.Compression))
	} else {
		encoder = export.NewBingenEncoder[T, U]()
	}

	return export.NewComputeStorageExporter(
		pathing,
		encoder,
		store,
		validator.NewSetValidator[T, S](config.Resolution),
		config.Streaming,
	), nil
}

// NewComputePipelineExportController creates a new `ComputeExportController[T]` instance which is used to export computed data
// using the provided source, storage, resolution, and source resolution.
func NewComputePipelineExportController[T any, U export.BinaryMarshalerPtr[T], S validator.SetConstraint[T]](
	config ComputeExporterConfig,
	store storage.Storage,
	source export.ComputeSource[T],
) (*export.ComputeExportController[T], error) {
	exporter, err := NewComputePipelineExporter[T, U, S](config, store)
	if err != nil {
		return nil, fmt.Errorf("failed to create compute exporter: %w", err)
	}

	return export.NewComputeExportController(source, exporter, config.Resolution), nil
}

func GetExporterPathing[T any, U export.BinaryMarshalerPtr[T], S validator.SetConstraint[T]](
	config ComputeExporterConfig,
) (pathing.StoragePathFormatter[opencost.Window], error) {
	pipelineName := pipelines.NameFor[T]()
	if pipelineName == "" {
		return nil, fmt.Errorf("failed to extract pipeline name for type: %s", typeutil.TypeOf[T]())
	}
	res := timeutil.FormatStoreResolution(config.Resolution)

	var pathFormatter pathing.StoragePathFormatter[opencost.Window]
	var err error

	switch pipelineName {
	case pipelines.KubeModelPipelineName:
		pathFormatter, err = pathing.NewKubeModelStoragePathFormatter(config.AppName, config.ClusterUID, res)
	default:
		// Use ClusterName for "clusterId" here to maintain legacy pattern
		pathFormatter, err = pathing.NewDefaultStoragePathFormatter(config.ClusterName, pipelineName, &config.Resolution)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create path formatter[%s]: %w", pipelineName, err)
	}
	return pathFormatter, nil
}
