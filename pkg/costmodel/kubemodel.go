package costmodel

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/stats"
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

	err = cm.kmComputeCluster(kms, start, end)
	if err != nil {
		kms.Metadata.Errors = append(kms.Metadata.Errors, err)
		return kms, fmt.Errorf("error computing kubemodel.Cluster for (%s, %s): %w", start.Format(logTimeFmt), end.Format(logTimeFmt), err)
	}

	err = cm.kmComputeNamespaces(kms, start, end)
	if err != nil {
		kms.Metadata.Errors = append(kms.Metadata.Errors, err)
	}
	kms.Metadata.ObjectCount += len(kms.Namespaces)

	err = cm.kmComputeResourceQuotas(kms, start, end)
	if err != nil {
		kms.Metadata.Errors = append(kms.Metadata.Errors, err)
	}
	kms.Metadata.ObjectCount += len(kms.ResourceQuotas)

	return kms, nil
}

func (cm *CostModel) kmComputeCluster(kms *kubemodel.KubeModelSet, start, end time.Time) error {

	// TODO: determine where Cluster data comes from
	//  - Should it come from direct queries?
	//  - Or should it come from pre-processed data from other objects?

	kms.Cluster = &kubemodel.Cluster{
		UID:  env.GetClusterID(), // TODO: should we instead grab these from Metrics()?
		Name: env.GetClusterID(), // TODO: do we still want to use this env var for Name?
	}

	kms.Metadata.ObjectCount += 1

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
		kms.Namespaces[res.UID].Labels = res.Labels
	}

	for _, res := range nsAnnosResult {
		kms.RegisterNamespace(res.UID, res.Namespace)
		kms.Namespaces[res.UID].Annotations = res.Annotations
	}

	return nil
}

func (cm *CostModel) kmComputeResourceQuotas(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	ds := cm.DataSource.Metrics()

	// spec.hard.requests
	rqSpecCPURequestAverageResultFuture := source.WithGroup(grp, ds.QueryResourceQuotaSpecCPURequestAverage(start, end))
	rqSpecCPURequestMaxResultFuture := source.WithGroup(grp, ds.QueryResourceQuotaSpecCPURequestMax(start, end))
	rqSpecRAMRequestAverageResultFuture := source.WithGroup(grp, ds.QueryResourceQuotaSpecRAMRequestAverage(start, end))
	rqSpecRAMRequestMaxResultFuture := source.WithGroup(grp, ds.QueryResourceQuotaSpecRAMRequestMax(start, end))

	// spec.hard.limits
	rqSpecCPULimitAverageResultFuture := source.WithGroup(grp, ds.QueryResourceQuotaSpecCPULimitAverage(start, end))
	rqSpecCPULimitMaxResultFuture := source.WithGroup(grp, ds.QueryResourceQuotaSpecCPULimitMax(start, end))
	rqSpecRAMLimitAverageResultFuture := source.WithGroup(grp, ds.QueryResourceQuotaSpecRAMLimitAverage(start, end))
	rqSpecRAMLimitMaxResultFuture := source.WithGroup(grp, ds.QueryResourceQuotaSpecRAMLimitMax(start, end))

	// status.used.requests
	rqStatusUsedCPURequestAverageResultFuture := source.WithGroup(grp, ds.QueryResourceQuotaStatusUsedCPURequestAverage(start, end))
	rqStatusUsedCPURequestMaxResultFuture := source.WithGroup(grp, ds.QueryResourceQuotaStatusUsedCPURequestMax(start, end))
	rqStatusUsedRAMRequestAverageResultFuture := source.WithGroup(grp, ds.QueryResourceQuotaStatusUsedRAMRequestAverage(start, end))
	rqStatusUsedRAMRequestMaxResultFuture := source.WithGroup(grp, ds.QueryResourceQuotaStatusUsedRAMRequestMax(start, end))

	// status.used.limits
	rqStatusUsedCPULimitAverageResultFuture := source.WithGroup(grp, ds.QueryResourceQuotaStatusUsedCPULimitAverage(start, end))
	rqStatusUsedCPULimitMaxResultFuture := source.WithGroup(grp, ds.QueryResourceQuotaStatusUsedCPULimitMax(start, end))
	rqStatusUsedRAMLimitAverageResultFuture := source.WithGroup(grp, ds.QueryResourceQuotaStatusUsedRAMLimitAverage(start, end))
	rqStatusUsedRAMLimitMaxResultFuture := source.WithGroup(grp, ds.QueryResourceQuotaStatusUsedRAMLimitMax(start, end))

	rqSpecCPURequestAverageResult, _ := rqSpecCPURequestAverageResultFuture.Await()
	for _, res := range rqSpecCPURequestAverageResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		kms.ResourceQuotas[res.UID].Spec.Hard.Requests.Set(kubemodel.ResourceCPU, kubemodel.UnitCPUCore, stats.Avg, res.Data[0].Value)
	}

	rqSpecCPURequestMaxResult, _ := rqSpecCPURequestMaxResultFuture.Await()
	for _, res := range rqSpecCPURequestMaxResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		kms.ResourceQuotas[res.UID].Spec.Hard.Requests.Set(kubemodel.ResourceCPU, kubemodel.UnitCPUCore, stats.Max, res.Data[0].Value)
	}

	rqSpecRAMRequestAverageResult, _ := rqSpecRAMRequestAverageResultFuture.Await()
	for _, res := range rqSpecRAMRequestAverageResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		kms.ResourceQuotas[res.UID].Spec.Hard.Requests.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, stats.Avg, res.Data[0].Value)
	}

	rqSpecRAMRequestMaxResult, _ := rqSpecRAMRequestMaxResultFuture.Await()
	for _, res := range rqSpecRAMRequestMaxResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		kms.ResourceQuotas[res.UID].Spec.Hard.Requests.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, stats.Max, res.Data[0].Value)
	}

	rqSpecCPULimitAverageResult, _ := rqSpecCPULimitAverageResultFuture.Await()
	for _, res := range rqSpecCPULimitAverageResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		kms.ResourceQuotas[res.UID].Spec.Hard.Limits.Set(kubemodel.ResourceCPU, kubemodel.UnitCPUCore, stats.Avg, res.Data[0].Value)
	}

	rqSpecCPULimitMaxResult, _ := rqSpecCPULimitMaxResultFuture.Await()
	for _, res := range rqSpecCPULimitMaxResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		kms.ResourceQuotas[res.UID].Spec.Hard.Limits.Set(kubemodel.ResourceCPU, kubemodel.UnitCPUCore, stats.Max, res.Data[0].Value)
	}

	rqSpecRAMLimitAverageResult, _ := rqSpecRAMLimitAverageResultFuture.Await()
	for _, res := range rqSpecRAMLimitAverageResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		kms.ResourceQuotas[res.UID].Spec.Hard.Limits.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, stats.Avg, res.Data[0].Value)
	}

	rqSpecRAMLimitMaxResult, _ := rqSpecRAMLimitMaxResultFuture.Await()
	for _, res := range rqSpecRAMLimitMaxResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		kms.ResourceQuotas[res.UID].Spec.Hard.Limits.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, stats.Max, res.Data[0].Value)
	}

	rqStatusUsedCPURequestAverageResult, _ := rqStatusUsedCPURequestAverageResultFuture.Await()
	for _, res := range rqStatusUsedCPURequestAverageResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		kms.ResourceQuotas[res.UID].Status.Used.Requests.Set(kubemodel.ResourceCPU, kubemodel.UnitCPUCore, stats.Avg, res.Data[0].Value)
	}

	rqStatusUsedCPURequestMaxResult, _ := rqStatusUsedCPURequestMaxResultFuture.Await()
	for _, res := range rqStatusUsedCPURequestMaxResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		kms.ResourceQuotas[res.UID].Status.Used.Requests.Set(kubemodel.ResourceCPU, kubemodel.UnitCPUCore, stats.Max, res.Data[0].Value)
	}

	rqStatusUsedRAMRequestAverageResult, _ := rqStatusUsedRAMRequestAverageResultFuture.Await()
	for _, res := range rqStatusUsedRAMRequestAverageResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		kms.ResourceQuotas[res.UID].Status.Used.Requests.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, stats.Avg, res.Data[0].Value)
	}

	rqStatusUsedRAMRequestMaxResult, _ := rqStatusUsedRAMRequestMaxResultFuture.Await()
	for _, res := range rqStatusUsedRAMRequestMaxResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		kms.ResourceQuotas[res.UID].Status.Used.Requests.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, stats.Max, res.Data[0].Value)
	}

	rqStatusUsedCPULimitAverageResult, _ := rqStatusUsedCPULimitAverageResultFuture.Await()
	for _, res := range rqStatusUsedCPULimitAverageResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		kms.ResourceQuotas[res.UID].Status.Used.Limits.Set(kubemodel.ResourceCPU, kubemodel.UnitCPUCore, stats.Avg, res.Data[0].Value)
	}

	rqStatusUsedCPULimitMaxResult, _ := rqStatusUsedCPULimitMaxResultFuture.Await()
	for _, res := range rqStatusUsedCPULimitMaxResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		kms.ResourceQuotas[res.UID].Status.Used.Limits.Set(kubemodel.ResourceCPU, kubemodel.UnitCPUCore, stats.Max, res.Data[0].Value)
	}

	rqStatusUsedRAMLimitAverageResult, _ := rqStatusUsedRAMLimitAverageResultFuture.Await()
	for _, res := range rqStatusUsedRAMLimitAverageResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		kms.ResourceQuotas[res.UID].Status.Used.Limits.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, stats.Avg, res.Data[0].Value)
	}

	rqStatusUsedRAMLimitMaxResult, _ := rqStatusUsedRAMLimitMaxResultFuture.Await()
	for _, res := range rqStatusUsedRAMLimitMaxResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		kms.ResourceQuotas[res.UID].Status.Used.Limits.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, stats.Max, res.Data[0].Value)
	}

	return nil
}
