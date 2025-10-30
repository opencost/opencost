package costmodel

import (
	"errors"
	"time"

	"github.com/opencost/opencost/core/pkg/model/kubemodel"
)

// ComputeKubeModel uses the CostModel instance to compute an KubeModelSet
// for the window defined by the given start and end times. The KubeModels
// returned are unaggregated (i.e. down to the container level).
func (cm *CostModel) ComputeKubeModel(start, end time.Time) (*kubemodel.KubeModelSet, error) {

	// TODO: use cm.DataSource to query for metrics and hydrate a *kubemodel.KubeModelSet

	return nil, errors.New("not implemented")
}
