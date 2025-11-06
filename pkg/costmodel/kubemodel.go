package costmodel

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

const logTimeFmt string = "2006-01-02T15:04:05"

// ComputeKubeModel uses the CostModel instance to compute an KubeModelSet
// for the window defined by the given start and end times. The KubeModels
// returned are unaggregated (i.e. down to the container level).
func (cm *CostModel) ComputeKubeModel(start, end time.Time) (*kubemodel.KubeModelSet, error) {
	// 1. Initialize new KubeModelSet for requested Window
	kms := kubemodel.NewKubeModelSet(start, end)

	// 2. Query CostModel for each set of objects
	var err error

	// 2.1 Compute Cluster
	err = cm.kmComputeCluster(kms, start, end)
	if err != nil {
		// TODO: Add diagnostic instead of returning early
		return kms, fmt.Errorf("error computing kubemodel.Cluster for (%s, %s): %w", start.Format(logTimeFmt), end.Format(logTimeFmt), err)
	}

	// 2.2 Compute Namespaces
	err = cm.kmComputeNamespaces(kms, start, end)
	if err != nil {
		// TODO: Add diagnostic
	}

	// 2.3 Compute ResourceQuotas
	err = cm.kmComputeResourceQuotas(kms, start, end)
	if err != nil {
		// TODO: Add diagnostic
	}

	// 3. Mark KubeModelSet as completed
	kms.Metadata.CompletedAt = time.Now().UTC()

	return kms, nil
}

func (cm *CostModel) kmComputeCluster(kms *kubemodel.KubeModelSet, start, end time.Time) error {

	// TODO: determine where Cluster data comes from
	//  - Should it come from direct queries?
	//  - Or should it come from pre-processed data from other objects?

	kms.Cluster = &kubemodel.Cluster{
		ID:   env.GetClusterID(), // TODO: should we instead grab these from Metrics()?
		Name: env.GetClusterID(), // TODO: do we still want to use this env var for Name?
	}

	return nil
}

func (cm *CostModel) kmComputeNamespaces(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	ds := cm.DataSource.Metrics()

	nsLabelsResultFuture := source.WithGroup(grp, ds.QueryNamespaceLabels(start, end))
	nsAnnosResultFuture := source.WithGroup(grp, ds.QueryNamespaceAnnotations(start, end))

	nsLabelsResult, _ := nsLabelsResultFuture.Await()
	nsAnnosResult, _ := nsAnnosResultFuture.Await()

	for _, res := range nsLabelsResult {
		kms.RegisterNamespace(res.UID, res.Namespace)
		kms.Cluster.Namespaces[res.UID].Labels = res.Labels
	}

	for _, res := range nsAnnosResult {
		kms.RegisterNamespace(res.UID, res.Namespace)
		kms.Cluster.Namespaces[res.UID].Annotations = res.Annotations
	}

	return nil
}

func (cm *CostModel) kmComputeResourceQuotas(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	// TODO: Implement resource quota computation for hierarchical model
	// The resource quotas need to be registered within their respective namespaces
	// which are nested under kms.Cluster.Namespaces[namespaceID].ResourceQuotas
	return nil
}
