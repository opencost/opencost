package costmodel

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
)

const logTimeFmt string = "2006-01-02T15:04:05"

// ComputeKubeModel uses the CostModel instance to compute an KubeModelSet
// for the window defined by the given start and end times. The KubeModels
// returned are unaggregated (i.e. down to the container level).
func (cm *CostModel) ComputeKubeModel(start, end time.Time) (*kubemodel.KubeModelSet, error) {
	// Initialize new KubeModelSet for requested Window
	kms := kubemodel.NewKubeModelSet(start, end)

	// Query CostModel for each set of objects
	var err error

	kms.Cluster, err = cm.kmComputeCluster(start, end)
	if err != nil {
		kms.Metadata.Errors = append(kms.Metadata.Errors, err)
		return kms, fmt.Errorf("error computing kubemodel.Cluster for (%s, %s): %w", start.Format(logTimeFmt), end.Format(logTimeFmt), err)
	}
	kms.Metadata.ObjectCount += 1

	kms.Namespaces, err = cm.kmComputeNamespaces(start, end)
	if err != nil {
		kms.Metadata.Errors = append(kms.Metadata.Errors, err)
	}
	kms.Metadata.ObjectCount += len(kms.Namespaces)

	kms.ResourceQuotas, err = cm.kmComputeResourceQuotas(start, end)
	if err != nil {
		kms.Metadata.Errors = append(kms.Metadata.Errors, err)
	}
	kms.Metadata.ObjectCount += len(kms.ResourceQuotas)

	return kms, nil
}

func (cm *CostModel) kmComputeCluster(start, end time.Time) (*kubemodel.Cluster, error) {

	// TODO: determine where Cluster data comes from
	//  - Should it come from direct queries?
	//  - Or should it come from pre-processed data from other objects?

	return &kubemodel.Cluster{
		ID:   env.GetClusterID(), // TODO: should we instead grab these from Metrics()?
		Name: env.GetClusterID(), // TODO: do we still want to use this env var for Name?
	}, nil
}

func (cm *CostModel) kmComputeNamespaces(start, end time.Time) (map[string]*kubemodel.Namespace, error) {

	// TODO: determine where Namespace data comes from
	//  - Should it come from direct queries?
	//  - Or should it come from pre-processed data from other objects?

	nsMap := map[string]*kubemodel.Namespace{}

	return nsMap, nil
}

func (cm *CostModel) kmComputeResourceQuotas(start, end time.Time) (map[string]*kubemodel.ResourceQuota, error) {
	rqMap := map[string]*kubemodel.ResourceQuota{}

	// TODO: make ResourceQuota queries when they are available in OpenCost

	return rqMap, nil
}
