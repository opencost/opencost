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
	forceV1    bool
	clusterUID string
}

func NewKubeModel(clusterUID string, forceV1 bool, dataSource source.OpenCostDataSource) (*KubeModel, error) {
	if dataSource == nil {
		return nil, errors.New("OpenCostDataSource cannot be nil")
	}

	km := &KubeModel{
		ds:         dataSource,
		forceV1:    forceV1,
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

	computeFuncs := km.computeFuncs(start, end)

	for _, f := range computeFuncs {
		if err := f(kms, start, end); err != nil {
			kms.Error(err)
			return kms, fmt.Errorf("error computing kubemodel for (%s, %s): %w", start.Format(time.DateTime), end.Format(time.DateTime), err)
		}
	}

	kms.Metadata.CompletedAt = time.Now().UTC()

	return kms, nil
}

// computeFuncs returns the set of compute functions to run for the window,
// in struct field order. If cluster_info is not yet reporting the
// complete_kubemodel label for any cluster in the result, the source has not
// been upgraded to emit a full kubemodel, so only the minimal set of
// resources (cluster, namespaces, resource quotas) is computed. The
// FORCE_KUBEMODEL_V1 env var skips the check entirely and always returns the
// minimal set.
func (km *KubeModel) computeFuncs(start, end time.Time) []computeFunc {
	KubeModelV1ComputeFucs := []computeFunc{
		km.computeCluster,
		km.computeNamespaces,
		km.computeResourceQuotas,
	}

	if km.forceV1 {
		return KubeModelV1ComputeFucs
	}

	results, err := km.ds.Metrics().QueryClusterKubeModelVersion(start, end).Await()
	if err != nil {
		log.Errorf("computeFuncs: querying cluster complete kubemodel: %s", err)
		return KubeModelV1ComputeFucs
	}

	// If the window contains a result which is missing the version number then the window will produce incomplete KubeModel data
	// and should not export
	for _, res := range results {
		if res.Version == "" {
			return KubeModelV1ComputeFucs
		}
	}

	return []computeFunc{
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
		//km.computeDCGMDevices,
	}
}
