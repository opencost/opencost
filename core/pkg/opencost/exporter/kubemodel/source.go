package kubemodel

import (
	"time"

	"github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/pipelines"
)

type KubeModelSource interface {
	ComputeKubeModelSet(start, end time.Time) (*kubemodel.KubeModelSet, error)
}

type KubeModelComputeSource struct {
	src KubeModelSource
}

// NewKubeModelComputeSource creates an `exporter.ComputeSource[opencost.KubeModelSet]` implementation
func NewKubeModelComputeSource(src KubeModelSource) exporter.ComputeSource[kubemodel.KubeModelSet] {
	return &KubeModelComputeSource{
		src: src,
	}
}

// CanCompute should return true iff the ComputeSource can effectively act as
// a source of T data for the given time range. For example, a ComputeSource
// with two-day coverage cannot fulfill a range from three days ago, and should
// not be left to return an error in Compute. Instead, it should report that is
// cannot compute and allow another Source to handle the computation.
func (acs *KubeModelComputeSource) CanCompute(start, end time.Time) bool {
	return true
}

// Compute should compute a single T for the given time range.
func (acs *KubeModelComputeSource) Compute(start, end time.Time) (*kubemodel.KubeModelSet, error) {
	return acs.src.ComputeKubeModelSet(start, end)
}

// Name returns the name of the ComputeSource
func (acs *KubeModelComputeSource) Name() string {
	return pipelines.KubeModelPipelineName
}
