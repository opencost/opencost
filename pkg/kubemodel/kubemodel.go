package kubemodel

import (
	"errors"
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

const logTimeFmt string = "2006-01-02T15:04:05"

type KubeModel struct {
	ds         source.OpenCostDataSource
	clusterUID string
}

func NewKubeModel(dataSource source.OpenCostDataSource) (*KubeModel, error) {
	if dataSource == nil {
		return nil, errors.New("OpenCostDataSource cannot be nil")
	}

	km := &KubeModel{ds: dataSource}

	clusterUID, err := km.computeClusterUID(time.Now().UTC())
	if err != nil {
		return nil, fmt.Errorf("error computing cluster UID: %w", err)
	}

	km.clusterUID = clusterUID

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
	err = km.computeCluster(kms)
	if err != nil {
		kms.Metadata.Errors = append(kms.Metadata.Errors, err.Error())
		return kms, fmt.Errorf("error computing kubemodel.Cluster for (%s, %s): %w", start.Format(logTimeFmt), end.Format(logTimeFmt), err)
	}

	// 2.2 Compute Namespaces
	err = km.computeNamespaces(kms, start, end)
	if err != nil {
		kms.Metadata.Errors = append(kms.Metadata.Errors, err.Error())
	}

	// 2.3 Compute ResourceQuotas
	err = km.computeResourceQuotas(kms, start, end)
	if err != nil {
		kms.Metadata.Errors = append(kms.Metadata.Errors, err.Error())
	}

	// 3. Mark KubeModelSet as completed
	kms.Metadata.CompletedAt = time.Now().UTC()

	return kms, nil
}

// TODO: come up with a better way to pull kube-system namespace UID from Metrics()?
func (km *KubeModel) computeClusterUID(start time.Time) (string, error) {
	// TODO: what (start, end) here? will this always work? or will it fail,
	// e.g. right after a clean install?
	start = start.Truncate(km.ds.Resolution())
	end := start.Add(km.ds.Resolution())

	nsLabelsResult, _ := km.ds.Metrics().QueryNamespaceLabels(start, end).Await()
	for _, res := range nsLabelsResult {
		if res.Namespace == "kube-system" {
			log.Infof("KubeModel: detected cluster UID from kube-system: %s", res.UID)
			return res.UID, nil
		}
	}

	clusterUID := env.GetClusterID()
	if clusterUID != "" {
		log.Warnf("KubeModel: failed to infer cluster UID from kube-system: using env var: %s", clusterUID)
		return clusterUID, nil
	}

	return "", errors.New("failed to detect cluster UID")
}

// TODO: should we periodically check the ClusterUID?
// TODO: where do we get the additional information? km.ds.ClusterInfo().GetClusterInfo() is a map[string]string...
func (km *KubeModel) computeCluster(kms *kubemodel.KubeModelSet) error {
	kms.Cluster = &kubemodel.Cluster{
		UID:  km.clusterUID,
		Name: env.GetClusterID(), // TODO: do we still want to use this env var for Name?
	}

	return nil
}

func (km *KubeModel) computeNamespaces(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	nsLabelsResultFuture := source.WithGroup(grp, metrics.QueryNamespaceLabels(start, end))
	nsAnnosResultFuture := source.WithGroup(grp, metrics.QueryNamespaceAnnotations(start, end))

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

func (km *KubeModel) computeResourceQuotas(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	// spec.hard.requests
	rqSpecCPURequestAverageResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaSpecCPURequestAverage(start, end))
	rqSpecCPURequestMaxResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaSpecCPURequestMax(start, end))
	rqSpecRAMRequestAverageResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaSpecRAMRequestAverage(start, end))
	rqSpecRAMRequestMaxResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaSpecRAMRequestMax(start, end))

	// spec.hard.limits
	rqSpecCPULimitAverageResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaSpecCPULimitAverage(start, end))
	rqSpecCPULimitMaxResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaSpecCPULimitMax(start, end))
	rqSpecRAMLimitAverageResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaSpecRAMLimitAverage(start, end))
	rqSpecRAMLimitMaxResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaSpecRAMLimitMax(start, end))

	// status.used.requests
	rqStatusUsedCPURequestAverageResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaStatusUsedCPURequestAverage(start, end))
	rqStatusUsedCPURequestMaxResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaStatusUsedCPURequestMax(start, end))
	rqStatusUsedRAMRequestAverageResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaStatusUsedRAMRequestAverage(start, end))
	rqStatusUsedRAMRequestMaxResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaStatusUsedRAMRequestMax(start, end))

	// status.used.limits
	rqStatusUsedCPULimitAverageResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaStatusUsedCPULimitAverage(start, end))
	rqStatusUsedCPULimitMaxResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaStatusUsedCPULimitMax(start, end))
	rqStatusUsedRAMLimitAverageResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaStatusUsedRAMLimitAverage(start, end))
	rqStatusUsedRAMLimitMaxResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaStatusUsedRAMLimitMax(start, end))

	rqSpecCPURequestAverageResult, _ := rqSpecCPURequestAverageResultFuture.Await()
	for _, res := range rqSpecCPURequestAverageResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		mcpu := res.Data[0].Value * 1000
		kms.ResourceQuotas[res.UID].Spec.Hard.Requests.Set(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatAvg, mcpu)
	}

	rqSpecCPURequestMaxResult, _ := rqSpecCPURequestMaxResultFuture.Await()
	for _, res := range rqSpecCPURequestMaxResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		mcpu := res.Data[0].Value * 1000
		kms.ResourceQuotas[res.UID].Spec.Hard.Requests.Set(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatMax, mcpu)
	}

	rqSpecRAMRequestAverageResult, _ := rqSpecRAMRequestAverageResultFuture.Await()
	for _, res := range rqSpecRAMRequestAverageResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		kms.ResourceQuotas[res.UID].Spec.Hard.Requests.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatAvg, res.Data[0].Value)
	}

	rqSpecRAMRequestMaxResult, _ := rqSpecRAMRequestMaxResultFuture.Await()
	for _, res := range rqSpecRAMRequestMaxResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		kms.ResourceQuotas[res.UID].Spec.Hard.Requests.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatMax, res.Data[0].Value)
	}

	rqSpecCPULimitAverageResult, _ := rqSpecCPULimitAverageResultFuture.Await()
	for _, res := range rqSpecCPULimitAverageResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		mcpu := res.Data[0].Value * 1000
		kms.ResourceQuotas[res.UID].Spec.Hard.Limits.Set(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatAvg, mcpu)
	}

	rqSpecCPULimitMaxResult, _ := rqSpecCPULimitMaxResultFuture.Await()
	for _, res := range rqSpecCPULimitMaxResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		mcpu := res.Data[0].Value * 1000
		kms.ResourceQuotas[res.UID].Spec.Hard.Limits.Set(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatMax, mcpu)
	}

	rqSpecRAMLimitAverageResult, _ := rqSpecRAMLimitAverageResultFuture.Await()
	for _, res := range rqSpecRAMLimitAverageResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		kms.ResourceQuotas[res.UID].Spec.Hard.Limits.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatAvg, res.Data[0].Value)
	}

	rqSpecRAMLimitMaxResult, _ := rqSpecRAMLimitMaxResultFuture.Await()
	for _, res := range rqSpecRAMLimitMaxResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		kms.ResourceQuotas[res.UID].Spec.Hard.Limits.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatMax, res.Data[0].Value)
	}

	rqStatusUsedCPURequestAverageResult, _ := rqStatusUsedCPURequestAverageResultFuture.Await()
	for _, res := range rqStatusUsedCPURequestAverageResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		mcpu := res.Data[0].Value * 1000
		kms.ResourceQuotas[res.UID].Status.Used.Requests.Set(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatAvg, mcpu)
	}

	rqStatusUsedCPURequestMaxResult, _ := rqStatusUsedCPURequestMaxResultFuture.Await()
	for _, res := range rqStatusUsedCPURequestMaxResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		mcpu := res.Data[0].Value * 1000
		kms.ResourceQuotas[res.UID].Status.Used.Requests.Set(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatMax, mcpu)
	}

	rqStatusUsedRAMRequestAverageResult, _ := rqStatusUsedRAMRequestAverageResultFuture.Await()
	for _, res := range rqStatusUsedRAMRequestAverageResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		kms.ResourceQuotas[res.UID].Status.Used.Requests.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatAvg, res.Data[0].Value)
	}

	rqStatusUsedRAMRequestMaxResult, _ := rqStatusUsedRAMRequestMaxResultFuture.Await()
	for _, res := range rqStatusUsedRAMRequestMaxResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		kms.ResourceQuotas[res.UID].Status.Used.Requests.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatMax, res.Data[0].Value)
	}

	rqStatusUsedCPULimitAverageResult, _ := rqStatusUsedCPULimitAverageResultFuture.Await()
	for _, res := range rqStatusUsedCPULimitAverageResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		mcpu := res.Data[0].Value * 1000
		kms.ResourceQuotas[res.UID].Status.Used.Limits.Set(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatAvg, mcpu)
	}

	rqStatusUsedCPULimitMaxResult, _ := rqStatusUsedCPULimitMaxResultFuture.Await()
	for _, res := range rqStatusUsedCPULimitMaxResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		mcpu := res.Data[0].Value * 1000
		kms.ResourceQuotas[res.UID].Status.Used.Limits.Set(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatMax, mcpu)
	}

	rqStatusUsedRAMLimitAverageResult, _ := rqStatusUsedRAMLimitAverageResultFuture.Await()
	for _, res := range rqStatusUsedRAMLimitAverageResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		kms.ResourceQuotas[res.UID].Status.Used.Limits.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatAvg, res.Data[0].Value)
	}

	rqStatusUsedRAMLimitMaxResult, _ := rqStatusUsedRAMLimitMaxResultFuture.Await()
	for _, res := range rqStatusUsedRAMLimitMaxResult {
		kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		kms.ResourceQuotas[res.UID].Status.Used.Limits.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatMax, res.Data[0].Value)
	}

	return nil
}
