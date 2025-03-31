package asset

import (
	"time"

	"github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/pipelines"
	"github.com/opencost/opencost/pkg/costmodel"
)

type AssetsComputeSource struct {
	cm *costmodel.CostModel
}

// NewAssetsComputeSource creates an `exporter.ComputeSource[opencost.AssetSet]` implementation
func NewAssetsComputeSource(cm *costmodel.CostModel) exporter.ComputeSource[opencost.AssetSet] {
	return &AssetsComputeSource{
		cm: cm,
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

// Compute should compute a single T for the given time range, optionally using the given resolution.
func (acs *AssetsComputeSource) Compute(start, end time.Time, resolution time.Duration) (*opencost.AssetSet, error) {
	return acs.cm.ComputeAssets(start, end)
}

// Name returns the name of the ComputeSource
func (acs *AssetsComputeSource) Name() string {
	return pipelines.AssetsPipelineName
}
