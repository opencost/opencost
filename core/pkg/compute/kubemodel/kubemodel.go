package kubemodel

import (
	"errors"
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

type KubeModel struct {
	ds         source.OpenCostDataSource
	clusterUID string
}

func NewKubeModel(clusterUID string, dataSource source.OpenCostDataSource) (*KubeModel, error) {
	if dataSource == nil {
		return nil, errors.New("OpenCostDataSource cannot be nil")
	}

	km := &KubeModel{
		ds:         dataSource,
		clusterUID: clusterUID,
	}

	km.clusterUID = clusterUID

	log.Debugf("NewKubeModel(%s)", km.clusterUID)

	return km, nil
}

type computeFunc func(*kubemodel.KubeModelSet, time.Time, time.Time) error

// ComputeKubeModel uses the CostModel instance to compute an KubeModelSet
// for the window defined by the given start and end times. The KubeModels
// returned are unaggregated (i.e. down to the container level).
func (km *KubeModel) ComputeKubeModelSet(start, end time.Time) (*kubemodel.KubeModelSet, error) {
	kms := kubemodel.NewKubeModelSet(start, end)

	computeFuncs := []computeFunc{
		km.computeCluster,
		km.computeNamespaces,
		km.computeResourceQuotas,
		km.computeServices,
		km.computeDeployments,
		km.computeStatefulSets,
		km.computeDaemonSets,
		km.computeJobs,
		km.computeCronJobs,
		km.computeReplicaSets,
		km.computeNodes,
		km.computePersistentVolumes,
		km.computePersistentVolumeClaims,
		km.computePods,
		km.computeContainers,
		km.computeDCGMDevices,
	}

	for _, f := range computeFuncs {
		if err := f(kms, start, end); err != nil {
			kms.Error(err)
			return kms, fmt.Errorf("error computing kubemodel for (%s, %s): %w", start.Format(time.DateTime), end.Format(time.DateTime), err)
		}
	}

	kms.Metadata.CompletedAt = time.Now().UTC()

	return kms, nil
}
