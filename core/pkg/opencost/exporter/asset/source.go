package asset

import (
	"time"

	"github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/pipelines"
)

type AssetSource interface {
	ComputeAssets(start, end time.Time) (*opencost.AssetSet, error)
}

type AssetsComputeSource struct {
	src AssetSource
}

// NewAssetsComputeSource creates an `exporter.ComputeSource[opencost.AssetSet]` implementation
func NewAssetsComputeSource(src AssetSource) exporter.ComputeSource[opencost.AssetSet] {
	return &AssetsComputeSource{
		src: src,
	}
}

// CanCompute should return true iff the ComputeSource can effectively act as
// a source of T data for the given time range. For example, a ComputeSource
// with two-day coverage cannot fulfill a range from three days ago, and should
// not be left to return an error in Compute. Instead, it should report that is
// cannot compute and allow another Source to handle the computation.
func (acs *AssetsComputeSource) CanCompute(start, end time.Time) bool {
	return true
}

// Compute should compute a single T for the given time range.
func (acs *AssetsComputeSource) Compute(start, end time.Time) (*opencost.AssetSet, error) {
	return acs.src.ComputeAssets(start, end)
}

// Name returns the name of the ComputeSource
func (acs *AssetsComputeSource) Name() string {
	return pipelines.AssetsPipelineName
}
