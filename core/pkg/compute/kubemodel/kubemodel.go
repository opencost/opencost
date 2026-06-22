package kubemodel

import (
	"errors"
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

const logTimeFmt string = "2006-01-02T15:04:05"

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

// ComputeKubeModel uses the CostModel instance to compute an KubeModelSet
// for the window defined by the given start and end times. The KubeModels
// returned are unaggregated (i.e. down to the container level).
func (km *KubeModel) ComputeKubeModelSet(start, end time.Time) (*kubemodel.KubeModelSet, error) {
	// 1. Initialize new KubeModelSet for requested Window
	kms := kubemodel.NewKubeModelSet(start, end)

	// 2. Query CostModel for each set of objects
	var err error

	// 2.1 Compute Cluster
	err = km.computeCluster(kms, start, end)
	if err != nil {
		kms.Error(err)
		return kms, fmt.Errorf("error computing kubemodel.Cluster for (%s, %s): %w", start.Format(logTimeFmt), end.Format(logTimeFmt), err)
	}

	// 2.2 Compute Nodes
	err = km.computeNodes(kms, start, end)
	if err != nil {
		kms.Error(err)
	}

	// 2.3 Compute Namespaces
	err = km.computeNamespaces(kms, start, end)
	if err != nil {
		kms.Error(err)
	}

	// 2.5 Compute Pods
	err = km.computePods(kms, start, end)
	if err != nil {
		kms.Error(err)
	}

	// 2.6 Compute Deployments
	err = km.computeDeployments(kms, start, end)
	if err != nil {
		kms.Error(err)
	}

	// 2.7 Compute StatefulSets
	err = km.computeStatefulSets(kms, start, end)
	if err != nil {
		kms.Error(err)
	}

	// 2.8 Compute DaemonSets
	err = km.computeDaemonSets(kms, start, end)
	if err != nil {
		kms.Error(err)
	}

	// 2.9 Compute Jobs
	err = km.computeJobs(kms, start, end)
	if err != nil {
		kms.Error(err)
	}

	// 2.10 Compute CronJobs
	err = km.computeCronJobs(kms, start, end)
	if err != nil {
		kms.Error(err)
	}

	// 2.11 Compute ReplicaSets
	err = km.computeReplicaSets(kms, start, end)
	if err != nil {
		kms.Error(err)
	}

	// 2.12 Compute Containers
	err = km.computeContainers(kms, start, end)
	if err != nil {
		kms.Error(err)
	}

	// 2.13 Compute ResourceQuotas
	err = km.computeResourceQuotas(kms, start, end)
	if err != nil {
		kms.Error(err)
	}

	// 2.14 Compute Services
	err = km.computeServices(kms, start, end)
	if err != nil {
		kms.Error(err)
	}

	// 2.15 Compute PersistentVolumes
	err = km.computePersistentVolumes(kms, start, end)
	if err != nil {
		kms.Error(err)
	}

	// 2.16 Compute PersistentVolumeClaims
	err = km.computePersistentVolumeClaims(kms, start, end)
	if err != nil {
		kms.Error(err)
	}

	// 2.17 Compute DCGM Devices
	err = km.computeDCGMDevices(kms, start, end)
	if err != nil {
		kms.Error(err)
	}

	// 3. Mark KubeModelSet as completed
	kms.Metadata.CompletedAt = time.Now().UTC()

	return kms, nil
}