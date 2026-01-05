package networkinsight

import (
	"time"

	"github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/pipelines"
)

type NetworkInsightSource interface {
	ComputeNetworkInsights(start, end time.Time) (*opencost.NetworkInsightSet, error)
}

type NetworkInsightsComputeSource struct {
	src NetworkInsightSource
}

// NewNetworkInsightsComputeSource creates an `exporter.ComputeSource[opencost.NetworkInsightSet]` implementation
func NewNetworkInsightsComputeSource(src NetworkInsightSource) exporter.ComputeSource[opencost.NetworkInsightSet] {
	return &NetworkInsightsComputeSource{
		src: src,
	}
}

// CanCompute should return true iff the ComputeSource can effectively act as
// a source of T data for the given time range. For example, a ComputeSource
// with two-day coverage cannot fulfill a range from three days ago, and should
// not be left to return an error in Compute. Instead, it should report that is
// cannot compute and allow another Source to handle the computation.
func (acs *NetworkInsightsComputeSource) CanCompute(start, end time.Time) bool {
	return true
}

// Compute should compute a single T for the given time range, optionally using the given resolution.
func (acs *NetworkInsightsComputeSource) Compute(start, end time.Time) (*opencost.NetworkInsightSet, error) {
	return acs.src.ComputeNetworkInsights(start, end)
}

// Name returns the name of the ComputeSource
func (acs *NetworkInsightsComputeSource) Name() string {
	return pipelines.NetworkInsightPipelineName
}
