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
		diagnostic := &kubemodel.DiagnosticResult{
			ID:          fmt.Sprintf("cluster-compute-%d", time.Now().Unix()),
			Name:        "ClusterCompute",
			Description: "Failed to compute cluster data",
			Category:    "cluster",
			Timestamp:   time.Now().UTC(),
			Error:       err.Error(),
		}
		kms.Metadata.Diagnostics = append(kms.Metadata.Diagnostics, diagnostic)
		return kms, fmt.Errorf("error computing kubemodel.Cluster for (%s, %s): %w", start.Format(logTimeFmt), end.Format(logTimeFmt), err)
	}

	// 2.2 Compute Namespaces
	err = cm.kmComputeNamespaces(kms, start, end)
	if err != nil {
		diagnostic := &kubemodel.DiagnosticResult{
			ID:          fmt.Sprintf("namespace-compute-%d", time.Now().Unix()),
			Name:        "NamespaceCompute",
			Description: "Failed to compute namespace data",
			Category:    "namespace",
			Timestamp:   time.Now().UTC(),
			Error:       err.Error(),
		}
		kms.Metadata.Diagnostics = append(kms.Metadata.Diagnostics, diagnostic)
	}

	// 2.3 Compute ResourceQuotas
	err = cm.kmComputeResourceQuotas(kms, start, end)
	if err != nil {
		diagnostic := &kubemodel.DiagnosticResult{
			ID:          fmt.Sprintf("resourcequota-compute-%d", time.Now().Unix()),
			Name:        "ResourceQuotaCompute",
			Description: "Failed to compute resource quota data",
			Category:    "resourcequota",
			Timestamp:   time.Now().UTC(),
			Error:       err.Error(),
		}
		kms.Metadata.Diagnostics = append(kms.Metadata.Diagnostics, diagnostic)
	}

	// 3. Mark KubeModelSet as completed
	kms.Metadata.CompletedAt = time.Now().UTC()

	return kms, nil
}

func (cm *CostModel) kmComputeCluster(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	// Use kube-system namespace UID as the cluster ID for uniqueness
	ds := cm.DataSource.Metrics()

	// Query kube-system namespace to get cluster ID
	kubeSystemLabelsFuture := ds.QueryNamespaceLabels(start, end)
	kubeSystemLabelsResult, _ := kubeSystemLabelsFuture.Await()

	var clusterID string
	var clusterName string

	// Find kube-system namespace
	for _, res := range kubeSystemLabelsResult {
		if res.Namespace == "kube-system" {
			clusterID = res.UID
			break
		}
	}

	// Fallback to environment variable if kube-system not found
	if clusterID == "" {
		clusterID = env.GetClusterID()
	}

	// Use env var for cluster name (can be overridden with a label in the future)
	clusterName = env.GetClusterID()

	kms.Cluster = &kubemodel.Cluster{
		ID:    clusterID,
		Name:  clusterName,
		Start: start,
		End:   end,
		// Initialize maps for hierarchical structure
		Nodes:             make(map[string]*kubemodel.Node),
		Namespaces:        make(map[string]*kubemodel.Namespace),
		PersistentVolumes: make(map[string]*kubemodel.Volume),
		LoadBalancers:     make(map[string]*kubemodel.Service),
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
	// Resource quotas are nested within namespaces in the hierarchical model
	// Query all resource quota metrics and populate them in their respective namespaces

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

	// Helper function to ensure namespace and resource quota exist
	ensureResourceQuota := func(uid, name, namespaceName string) *kubemodel.ResourceQuota {
		// Find namespace by name using index
		ns, ok := kms.GetNamespaceByName(namespaceName)
		if !ok {
			return nil
		}

		// Check if resource quota already exists
		if rq, ok := ns.ResourceQuotas[uid]; ok {
			return rq
		}

		// Create new resource quota
		rq := &kubemodel.ResourceQuota{
			UID:          uid,
			NamespaceUID: ns.ID,
			Name:         name,
			Spec: &kubemodel.ResourceQuotaSpec{
				Hard: &kubemodel.ResourceQuotaSpecHard{
					Requests: make(kubemodel.ResourceQuantities),
					Limits:   make(kubemodel.ResourceQuantities),
				},
			},
			Status: &kubemodel.ResourceQuotaStatus{
				Used: &kubemodel.ResourceQuotaStatusUsed{
					Requests: make(kubemodel.ResourceQuantities),
					Limits:   make(kubemodel.ResourceQuantities),
				},
			},
		}
		ns.ResourceQuotas[uid] = rq
		return rq
	}

	// Process spec.hard.requests
	rqSpecCPURequestAverageResult, _ := rqSpecCPURequestAverageResultFuture.Await()
	for _, res := range rqSpecCPURequestAverageResult {
		if rq := ensureResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			mcpu := res.Data[0].Value * 1000
			rq.Spec.Hard.Requests.Set(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.Avg, mcpu)
		}
	}

	rqSpecCPURequestMaxResult, _ := rqSpecCPURequestMaxResultFuture.Await()
	for _, res := range rqSpecCPURequestMaxResult {
		if rq := ensureResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			mcpu := res.Data[0].Value * 1000
			rq.Spec.Hard.Requests.Set(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.Max, mcpu)
		}
	}

	rqSpecRAMRequestAverageResult, _ := rqSpecRAMRequestAverageResultFuture.Await()
	for _, res := range rqSpecRAMRequestAverageResult {
		if rq := ensureResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			rq.Spec.Hard.Requests.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.Avg, res.Data[0].Value)
		}
	}

	rqSpecRAMRequestMaxResult, _ := rqSpecRAMRequestMaxResultFuture.Await()
	for _, res := range rqSpecRAMRequestMaxResult {
		if rq := ensureResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			rq.Spec.Hard.Requests.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.Max, res.Data[0].Value)
		}
	}

	// Process spec.hard.limits
	rqSpecCPULimitAverageResult, _ := rqSpecCPULimitAverageResultFuture.Await()
	for _, res := range rqSpecCPULimitAverageResult {
		if rq := ensureResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			mcpu := res.Data[0].Value * 1000
			rq.Spec.Hard.Limits.Set(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.Avg, mcpu)
		}
	}

	rqSpecCPULimitMaxResult, _ := rqSpecCPULimitMaxResultFuture.Await()
	for _, res := range rqSpecCPULimitMaxResult {
		if rq := ensureResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			mcpu := res.Data[0].Value * 1000
			rq.Spec.Hard.Limits.Set(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.Max, mcpu)
		}
	}

	rqSpecRAMLimitAverageResult, _ := rqSpecRAMLimitAverageResultFuture.Await()
	for _, res := range rqSpecRAMLimitAverageResult {
		if rq := ensureResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			rq.Spec.Hard.Limits.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.Avg, res.Data[0].Value)
		}
	}

	rqSpecRAMLimitMaxResult, _ := rqSpecRAMLimitMaxResultFuture.Await()
	for _, res := range rqSpecRAMLimitMaxResult {
		if rq := ensureResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			rq.Spec.Hard.Limits.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.Max, res.Data[0].Value)
		}
	}

	// Process status.used.requests
	rqStatusUsedCPURequestAverageResult, _ := rqStatusUsedCPURequestAverageResultFuture.Await()
	for _, res := range rqStatusUsedCPURequestAverageResult {
		if rq := ensureResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			mcpu := res.Data[0].Value * 1000
			rq.Status.Used.Requests.Set(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.Avg, mcpu)
		}
	}

	rqStatusUsedCPURequestMaxResult, _ := rqStatusUsedCPURequestMaxResultFuture.Await()
	for _, res := range rqStatusUsedCPURequestMaxResult {
		if rq := ensureResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			mcpu := res.Data[0].Value * 1000
			rq.Status.Used.Requests.Set(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.Max, mcpu)
		}
	}

	rqStatusUsedRAMRequestAverageResult, _ := rqStatusUsedRAMRequestAverageResultFuture.Await()
	for _, res := range rqStatusUsedRAMRequestAverageResult {
		if rq := ensureResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			rq.Status.Used.Requests.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.Avg, res.Data[0].Value)
		}
	}

	rqStatusUsedRAMRequestMaxResult, _ := rqStatusUsedRAMRequestMaxResultFuture.Await()
	for _, res := range rqStatusUsedRAMRequestMaxResult {
		if rq := ensureResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			rq.Status.Used.Requests.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.Max, res.Data[0].Value)
		}
	}

	// Process status.used.limits
	rqStatusUsedCPULimitAverageResult, _ := rqStatusUsedCPULimitAverageResultFuture.Await()
	for _, res := range rqStatusUsedCPULimitAverageResult {
		if rq := ensureResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			mcpu := res.Data[0].Value * 1000
			rq.Status.Used.Limits.Set(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.Avg, mcpu)
		}
	}

	rqStatusUsedCPULimitMaxResult, _ := rqStatusUsedCPULimitMaxResultFuture.Await()
	for _, res := range rqStatusUsedCPULimitMaxResult {
		if rq := ensureResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			mcpu := res.Data[0].Value * 1000
			rq.Status.Used.Limits.Set(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.Max, mcpu)
		}
	}

	rqStatusUsedRAMLimitAverageResult, _ := rqStatusUsedRAMLimitAverageResultFuture.Await()
	for _, res := range rqStatusUsedRAMLimitAverageResult {
		if rq := ensureResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			rq.Status.Used.Limits.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.Avg, res.Data[0].Value)
		}
	}

	rqStatusUsedRAMLimitMaxResult, _ := rqStatusUsedRAMLimitMaxResultFuture.Await()
	for _, res := range rqStatusUsedRAMLimitMaxResult {
		if rq := ensureResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			rq.Status.Used.Limits.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.Max, res.Data[0].Value)
		}
	}

	return nil
}
