package source

import (
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/pipelines"
	"github.com/opencost/opencost/pkg/costmodel"
)

type AllocationComputeSource struct {
	cm *costmodel.CostModel
}

// CanCompute should return true iff the ComputeSource can effectively act as
// a source of T data for the given time range. For example, a ComputeSource
// with two-day coverage cannot fulfill a range from three days ago, and should
// not be left to return an error in Compute. Instead, it should report that is
// cannot compute and allow another Source to handle the computation.
func (acs *AllocationComputeSource) CanCompute(start, end time.Time) bool {
	return true
}

// Compute should compute a single T for the given time range, optionally using the given resolution.
func (acs *AllocationComputeSource) Compute(start, end time.Time, resolution time.Duration) (*opencost.AllocationSet, error) {
	return acs.cm.ComputeAllocation(start, end, resolution)
}

// Name returns the name of the ComputeSource
func (acs *AllocationComputeSource) Name() string {
	return pipelines.AllocationPipelineName
}
