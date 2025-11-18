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
			UID:         fmt.Sprintf("cluster-compute-%d", time.Now().Unix()),
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
			UID:         fmt.Sprintf("namespace-compute-%d", time.Now().Unix()),
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
			UID:         fmt.Sprintf("resourcequota-compute-%d", time.Now().Unix()),
			Name:        "ResourceQuotaCompute",
			Description: "Failed to compute resource quota data",
			Category:    "resourcequota",
			Timestamp:   time.Now().UTC(),
			Error:       err.Error(),
		}
		kms.Metadata.Diagnostics = append(kms.Metadata.Diagnostics, diagnostic)
	}

	// 2.4 Compute Nodes
	err = cm.kmComputeNodes(kms, start, end)
	if err != nil {
		diagnostic := &kubemodel.DiagnosticResult{
			UID:         fmt.Sprintf("node-compute-%d", time.Now().Unix()),
			Name:        "NodeCompute",
			Description: "Failed to compute node data",
			Category:    "node",
			Timestamp:   time.Now().UTC(),
			Error:       err.Error(),
		}
		kms.Metadata.Diagnostics = append(kms.Metadata.Diagnostics, diagnostic)
	}

	// 2.5 Compute Pods
	err = cm.kmComputePods(kms, start, end)
	if err != nil {
		diagnostic := &kubemodel.DiagnosticResult{
			UID:         fmt.Sprintf("pod-compute-%d", time.Now().Unix()),
			Name:        "PodCompute",
			Description: "Failed to compute pod data",
			Category:    "pod",
			Timestamp:   time.Now().UTC(),
			Error:       err.Error(),
		}
		kms.Metadata.Diagnostics = append(kms.Metadata.Diagnostics, diagnostic)
	}

	// 2.6 Compute Containers
	err = cm.kmComputeContainers(kms, start, end)
	if err != nil {
		diagnostic := &kubemodel.DiagnosticResult{
			UID:         fmt.Sprintf("container-compute-%d", time.Now().Unix()),
			Name:        "ContainerCompute",
			Description: "Failed to compute container data",
			Category:    "container",
			Timestamp:   time.Now().UTC(),
			Error:       err.Error(),
		}
		kms.Metadata.Diagnostics = append(kms.Metadata.Diagnostics, diagnostic)
	}

	// 2.7 Compute Controllers
	err = cm.kmComputeControllers(kms, start, end)
	if err != nil {
		diagnostic := &kubemodel.DiagnosticResult{
			UID:         fmt.Sprintf("controller-compute-%d", time.Now().Unix()),
			Name:        "ControllerCompute",
			Description: "Failed to compute controller data",
			Category:    "controller",
			Timestamp:   time.Now().UTC(),
			Error:       err.Error(),
		}
		kms.Metadata.Diagnostics = append(kms.Metadata.Diagnostics, diagnostic)
	}

	// 2.8 Compute Services
	err = cm.kmComputeServices(kms, start, end)
	if err != nil {
		diagnostic := &kubemodel.DiagnosticResult{
			UID:         fmt.Sprintf("service-compute-%d", time.Now().Unix()),
			Name:        "ServiceCompute",
			Description: "Failed to compute service data",
			Category:    "service",
			Timestamp:   time.Now().UTC(),
			Error:       err.Error(),
		}
		kms.Metadata.Diagnostics = append(kms.Metadata.Diagnostics, diagnostic)
	}

	// 2.9 Compute PVCs
	err = cm.kmComputePVCs(kms, start, end)
	if err != nil {
		diagnostic := &kubemodel.DiagnosticResult{
			UID:         fmt.Sprintf("pvc-compute-%d", time.Now().Unix()),
			Name:        "PVCCompute",
			Description: "Failed to compute PVC data",
			Category:    "pvc",
			Timestamp:   time.Now().UTC(),
			Error:       err.Error(),
		}
		kms.Metadata.Diagnostics = append(kms.Metadata.Diagnostics, diagnostic)
	}

	// 2.10 Compute Volumes
	err = cm.kmComputeVolumes(kms, start, end)
	if err != nil {
		diagnostic := &kubemodel.DiagnosticResult{
			UID:         fmt.Sprintf("volume-compute-%d", time.Now().Unix()),
			Name:        "VolumeCompute",
			Description: "Failed to compute volume data",
			Category:    "volume",
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

	var clusterUID string
	var clusterName string

	// Find kube-system namespace
	for _, res := range kubeSystemLabelsResult {
		if res.Namespace == "kube-system" {
			clusterUID = res.UID
			break
		}
	}

	// Fallback to environment variable if kube-system not found
	if clusterUID == "" {
		clusterUID = env.GetClusterID()
	}

	// Use env var for cluster name (can be overridden with a label in the future)
	clusterName = env.GetClusterID()

	kms.Cluster = &kubemodel.Cluster{
		UID:   clusterUID,
		Name:  clusterName,
		Start: start,
		End:   end,
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
		kms.Namespaces[res.UID].Labels = res.Labels
	}

	for _, res := range nsAnnosResult {
		kms.RegisterNamespace(res.UID, res.Namespace)
		kms.Namespaces[res.UID].Annotations = res.Annotations
	}

	return nil
}

func (cm *CostModel) kmComputeResourceQuotas(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	// Resource quotas are stored in a flat structure in KubeModelSet.ResourceQuotas
	// Query all resource quota metrics and populate them

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

	// Helper function to get or create resource quota in flat structure
	getOrCreateResourceQuota := func(uid, name, namespaceName string) *kubemodel.ResourceQuota {
		// Check if resource quota already exists in flat map
		if rq, ok := kms.ResourceQuotas[uid]; ok {
			return rq
		}

		// Register new resource quota using the Register function
		err := kms.RegisterResourceQuota(uid, name, namespaceName)
		if err != nil {
			// Namespace doesn't exist
			return nil
		}

		// Return the newly created resource quota
		return kms.ResourceQuotas[uid]
	}

	// Process spec.hard.requests
	rqSpecCPURequestAverageResult, _ := rqSpecCPURequestAverageResultFuture.Await()
	for _, res := range rqSpecCPURequestAverageResult {
		if rq := getOrCreateResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			mcpu := res.Data[0].Value * 1000
			rq.Spec.Hard.Requests.Set(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatAvg, mcpu)
		}
	}

	rqSpecCPURequestMaxResult, _ := rqSpecCPURequestMaxResultFuture.Await()
	for _, res := range rqSpecCPURequestMaxResult {
		if rq := getOrCreateResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			mcpu := res.Data[0].Value * 1000
			rq.Spec.Hard.Requests.Set(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatMax, mcpu)
		}
	}

	rqSpecRAMRequestAverageResult, _ := rqSpecRAMRequestAverageResultFuture.Await()
	for _, res := range rqSpecRAMRequestAverageResult {
		if rq := getOrCreateResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			rq.Spec.Hard.Requests.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatAvg, res.Data[0].Value)
		}
	}

	rqSpecRAMRequestMaxResult, _ := rqSpecRAMRequestMaxResultFuture.Await()
	for _, res := range rqSpecRAMRequestMaxResult {
		if rq := getOrCreateResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			rq.Spec.Hard.Requests.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatMax, res.Data[0].Value)
		}
	}

	// Process spec.hard.limits
	rqSpecCPULimitAverageResult, _ := rqSpecCPULimitAverageResultFuture.Await()
	for _, res := range rqSpecCPULimitAverageResult {
		if rq := getOrCreateResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			mcpu := res.Data[0].Value * 1000
			rq.Spec.Hard.Limits.Set(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatAvg, mcpu)
		}
	}

	rqSpecCPULimitMaxResult, _ := rqSpecCPULimitMaxResultFuture.Await()
	for _, res := range rqSpecCPULimitMaxResult {
		if rq := getOrCreateResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			mcpu := res.Data[0].Value * 1000
			rq.Spec.Hard.Limits.Set(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatMax, mcpu)
		}
	}

	rqSpecRAMLimitAverageResult, _ := rqSpecRAMLimitAverageResultFuture.Await()
	for _, res := range rqSpecRAMLimitAverageResult {
		if rq := getOrCreateResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			rq.Spec.Hard.Limits.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatAvg, res.Data[0].Value)
		}
	}

	rqSpecRAMLimitMaxResult, _ := rqSpecRAMLimitMaxResultFuture.Await()
	for _, res := range rqSpecRAMLimitMaxResult {
		if rq := getOrCreateResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			rq.Spec.Hard.Limits.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatMax, res.Data[0].Value)
		}
	}

	// Process status.used.requests
	rqStatusUsedCPURequestAverageResult, _ := rqStatusUsedCPURequestAverageResultFuture.Await()
	for _, res := range rqStatusUsedCPURequestAverageResult {
		if rq := getOrCreateResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			mcpu := res.Data[0].Value * 1000
			rq.Status.Used.Requests.Set(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatAvg, mcpu)
		}
	}

	rqStatusUsedCPURequestMaxResult, _ := rqStatusUsedCPURequestMaxResultFuture.Await()
	for _, res := range rqStatusUsedCPURequestMaxResult {
		if rq := getOrCreateResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			mcpu := res.Data[0].Value * 1000
			rq.Status.Used.Requests.Set(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatMax, mcpu)
		}
	}

	rqStatusUsedRAMRequestAverageResult, _ := rqStatusUsedRAMRequestAverageResultFuture.Await()
	for _, res := range rqStatusUsedRAMRequestAverageResult {
		if rq := getOrCreateResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			rq.Status.Used.Requests.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatAvg, res.Data[0].Value)
		}
	}

	rqStatusUsedRAMRequestMaxResult, _ := rqStatusUsedRAMRequestMaxResultFuture.Await()
	for _, res := range rqStatusUsedRAMRequestMaxResult {
		if rq := getOrCreateResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			rq.Status.Used.Requests.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatMax, res.Data[0].Value)
		}
	}

	// Process status.used.limits
	rqStatusUsedCPULimitAverageResult, _ := rqStatusUsedCPULimitAverageResultFuture.Await()
	for _, res := range rqStatusUsedCPULimitAverageResult {
		if rq := getOrCreateResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			mcpu := res.Data[0].Value * 1000
			rq.Status.Used.Limits.Set(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatAvg, mcpu)
		}
	}

	rqStatusUsedCPULimitMaxResult, _ := rqStatusUsedCPULimitMaxResultFuture.Await()
	for _, res := range rqStatusUsedCPULimitMaxResult {
		if rq := getOrCreateResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			mcpu := res.Data[0].Value * 1000
			rq.Status.Used.Limits.Set(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatMax, mcpu)
		}
	}

	rqStatusUsedRAMLimitAverageResult, _ := rqStatusUsedRAMLimitAverageResultFuture.Await()
	for _, res := range rqStatusUsedRAMLimitAverageResult {
		if rq := getOrCreateResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			rq.Status.Used.Limits.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatAvg, res.Data[0].Value)
		}
	}

	rqStatusUsedRAMLimitMaxResult, _ := rqStatusUsedRAMLimitMaxResultFuture.Await()
	for _, res := range rqStatusUsedRAMLimitMaxResult {
		if rq := getOrCreateResourceQuota(res.UID, res.ResourceQuota, res.Namespace); rq != nil {
			rq.Status.Used.Limits.Set(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatMax, res.Data[0].Value)
		}
	}

	return nil
}

func (cm *CostModel) kmComputeNodes(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	ds := cm.DataSource.Metrics()

	// Query all node-related metrics in parallel
	nodeLabelsResultFuture := source.WithGroup(grp, ds.QueryNodeLabels(start, end))
	nodeActiveMinutesResultFuture := source.WithGroup(grp, ds.QueryNodeActiveMinutes(start, end))
	nodeCPUCoresCapacityResultFuture := source.WithGroup(grp, ds.QueryNodeCPUCoresCapacity(start, end))
	nodeRAMBytesCapacityResultFuture := source.WithGroup(grp, ds.QueryNodeRAMBytesCapacity(start, end))

	// Await results
	nodeLabelsResult, _ := nodeLabelsResultFuture.Await()
	nodeActiveMinutesResult, _ := nodeActiveMinutesResultFuture.Await()
	nodeCPUCoresCapacityResult, _ := nodeCPUCoresCapacityResultFuture.Await()
	nodeRAMBytesCapacityResult, _ := nodeRAMBytesCapacityResultFuture.Await()

	// Process node labels first to register all nodes
	for _, res := range nodeLabelsResult {
		kms.RegisterNode(res.Node, res.Node)
		if node, ok := kms.Nodes[res.Node]; ok {
			node.Labels = res.Labels
			node.Start = start
			node.End = end
		}
	}

	// Process active minutes
	for _, res := range nodeActiveMinutesResult {
		if node, ok := kms.Nodes[res.Node]; ok {
			// Active minutes indicates the node was active during the window
			_ = res.Data // Could use this to set more precise start/end times
			node.Start = start
			node.End = end
		}
	}

	// Process CPU capacity
	for _, res := range nodeCPUCoresCapacityResult {
		if node, ok := kms.Nodes[res.Node]; ok && len(res.Data) > 0 {
			// Convert cores to millicores
			node.CpuMillicoreSeconds = uint64(res.Data[0].Value * 1000)
		}
	}

	// Process RAM capacity
	for _, res := range nodeRAMBytesCapacityResult {
		if node, ok := kms.Nodes[res.Node]; ok && len(res.Data) > 0 {
			node.RAMByteSeconds = uint64(res.Data[0].Value)
		}
	}

	return nil
}

func (cm *CostModel) kmComputePods(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	ds := cm.DataSource.Metrics()

	// Query pod metadata
	podsUIDResultFuture := source.WithGroup(grp, ds.QueryPodsUID(start, end))
	podLabelsResultFuture := source.WithGroup(grp, ds.QueryPodLabels(start, end))
	podAnnosResultFuture := source.WithGroup(grp, ds.QueryPodAnnotations(start, end))

	// Query pod metrics
	cpuRequestsResultFuture := source.WithGroup(grp, ds.QueryCPURequests(start, end))
	cpuUsageAvgResultFuture := source.WithGroup(grp, ds.QueryCPUUsageAvg(start, end))
	cpuUsageMaxResultFuture := source.WithGroup(grp, ds.QueryCPUUsageMax(start, end))
	ramRequestsResultFuture := source.WithGroup(grp, ds.QueryRAMRequests(start, end))
	ramUsageAvgResultFuture := source.WithGroup(grp, ds.QueryRAMUsageAvg(start, end))
	ramUsageMaxResultFuture := source.WithGroup(grp, ds.QueryRAMUsageMax(start, end))
	netTransferResultFuture := source.WithGroup(grp, ds.QueryNetTransferBytes(start, end))
	netReceiveResultFuture := source.WithGroup(grp, ds.QueryNetReceiveBytes(start, end))

	// Await results
	podsUIDResult, _ := podsUIDResultFuture.Await()
	podLabelsResult, _ := podLabelsResultFuture.Await()
	podAnnosResult, _ := podAnnosResultFuture.Await()
	cpuRequestsResult, _ := cpuRequestsResultFuture.Await()
	cpuUsageAvgResult, _ := cpuUsageAvgResultFuture.Await()
	cpuUsageMaxResult, _ := cpuUsageMaxResultFuture.Await()
	ramRequestsResult, _ := ramRequestsResultFuture.Await()
	ramUsageAvgResult, _ := ramUsageAvgResultFuture.Await()
	ramUsageMaxResult, _ := ramUsageMaxResultFuture.Await()
	netTransferResult, _ := netTransferResultFuture.Await()
	netReceiveResult, _ := netReceiveResultFuture.Await()

	// Process pod UIDs to register pods
	for _, res := range podsUIDResult {
		kms.RegisterPod(res.UID, res.Pod, res.Namespace)
		if pod, ok := kms.Pods[res.UID]; ok {
			pod.Start = start
			pod.End = end
		}
	}

	// Process labels
	for _, res := range podLabelsResult {
		kms.RegisterPod(res.UID, res.Pod, res.Namespace)
		if pod, ok := kms.Pods[res.UID]; ok {
			pod.Labels = res.Labels
		}
	}

	// Process annotations
	for _, res := range podAnnosResult {
		kms.RegisterPod(res.UID, res.Pod, res.Namespace)
		if pod, ok := kms.Pods[res.UID]; ok {
			pod.Annotations = res.Annotations
		}
	}

	// Process CPU requests (aggregate container-level data to pod)
	for _, res := range cpuRequestsResult {
		if pod, ok := kms.Pods[res.UID]; ok && len(res.Data) > 0 {
			// Set node ID from container metrics (ContainerMetricResult has Node field)
			if pod.NodeUID == "" {
				pod.NodeUID = res.Node
			}
			// Convert cores to millicores and sum
			pod.CpuMillicoreRequestAverage += uint64(res.Data[0].Value * 1000)
		}
	}

	// Process CPU usage average
	for _, res := range cpuUsageAvgResult {
		if pod, ok := kms.Pods[res.UID]; ok && len(res.Data) > 0 {
			pod.CpuMillicoreUsageAverage += uint64(res.Data[0].Value * 1000)
		}
	}

	// Process CPU usage max
	for _, res := range cpuUsageMaxResult {
		if pod, ok := kms.Pods[res.UID]; ok && len(res.Data) > 0 {
			pod.CpuMillicoreUsageMax += uint64(res.Data[0].Value * 1000)
		}
	}

	// Process RAM requests
	for _, res := range ramRequestsResult {
		if pod, ok := kms.Pods[res.UID]; ok && len(res.Data) > 0 {
			pod.RAMByteRequestAverage += uint64(res.Data[0].Value)
		}
	}

	// Process RAM usage average
	for _, res := range ramUsageAvgResult {
		if pod, ok := kms.Pods[res.UID]; ok && len(res.Data) > 0 {
			pod.RAMByteUsageAverage += uint64(res.Data[0].Value)
		}
	}

	// Process RAM usage max
	for _, res := range ramUsageMaxResult {
		if pod, ok := kms.Pods[res.UID]; ok && len(res.Data) > 0 {
			pod.RAMByteUsageMax += uint64(res.Data[0].Value)
		}
	}

	// Process network transfer bytes
	for _, res := range netTransferResult {
		if pod, ok := kms.Pods[res.UID]; ok && len(res.Data) > 0 {
			pod.NetworkTransferBytes += uint64(res.Data[0].Value)
		}
	}

	// Process network receive bytes
	for _, res := range netReceiveResult {
		if pod, ok := kms.Pods[res.UID]; ok && len(res.Data) > 0 {
			pod.NetworkReceiveBytes += uint64(res.Data[0].Value)
		}
	}

	return nil
}

func (cm *CostModel) kmComputeContainers(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	ds := cm.DataSource.Metrics()

	// Query container metrics
	cpuRequestsResultFuture := source.WithGroup(grp, ds.QueryCPURequests(start, end))
	cpuUsageAvgResultFuture := source.WithGroup(grp, ds.QueryCPUUsageAvg(start, end))
	cpuUsageMaxResultFuture := source.WithGroup(grp, ds.QueryCPUUsageMax(start, end))
	ramRequestsResultFuture := source.WithGroup(grp, ds.QueryRAMRequests(start, end))
	ramUsageAvgResultFuture := source.WithGroup(grp, ds.QueryRAMUsageAvg(start, end))
	ramUsageMaxResultFuture := source.WithGroup(grp, ds.QueryRAMUsageMax(start, end))

	// Await results
	cpuRequestsResult, _ := cpuRequestsResultFuture.Await()
	cpuUsageAvgResult, _ := cpuUsageAvgResultFuture.Await()
	cpuUsageMaxResult, _ := cpuUsageMaxResultFuture.Await()
	ramRequestsResult, _ := ramRequestsResultFuture.Await()
	ramUsageAvgResult, _ := ramUsageAvgResultFuture.Await()
	ramUsageMaxResult, _ := ramUsageMaxResultFuture.Await()

	// Helper to get or create container
	getOrCreateContainer := func(podUID, containerName string) *kubemodel.Container {
		containerID := podUID + "/" + containerName
		if container, ok := kms.Containers[containerID]; ok {
			return container
		}

		// Create new container
		kms.RegisterContainer(containerID, containerName, podUID)
		if container, ok := kms.Containers[containerID]; ok {
			container.Start = start
			container.End = end
			return container
		}
		return nil
	}

	// Process CPU requests
	for _, res := range cpuRequestsResult {
		if container := getOrCreateContainer(res.UID, res.Container); container != nil && len(res.Data) > 0 {
			container.CpuMillicoreRequestAverage = uint64(res.Data[0].Value * 1000)
		}
	}

	// Process CPU usage average
	for _, res := range cpuUsageAvgResult {
		if container := getOrCreateContainer(res.UID, res.Container); container != nil && len(res.Data) > 0 {
			container.CpuMillicoreUsageAverage = uint64(res.Data[0].Value * 1000)
		}
	}

	// Process CPU usage max
	for _, res := range cpuUsageMaxResult {
		if container := getOrCreateContainer(res.UID, res.Container); container != nil && len(res.Data) > 0 {
			container.CpuMillicoreUsageMax = uint64(res.Data[0].Value * 1000)
		}
	}

	// Process RAM requests
	for _, res := range ramRequestsResult {
		if container := getOrCreateContainer(res.UID, res.Container); container != nil && len(res.Data) > 0 {
			container.RAMByteRequestAverage = uint64(res.Data[0].Value)
		}
	}

	// Process RAM usage average
	for _, res := range ramUsageAvgResult {
		if container := getOrCreateContainer(res.UID, res.Container); container != nil && len(res.Data) > 0 {
			container.RAMByteUsageAverage = uint64(res.Data[0].Value)
		}
	}

	// Process RAM usage max
	for _, res := range ramUsageMaxResult {
		if container := getOrCreateContainer(res.UID, res.Container); container != nil && len(res.Data) > 0 {
			container.RAMByteUsageMax = uint64(res.Data[0].Value)
		}
	}

	return nil
}

func (cm *CostModel) kmComputeControllers(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	ds := cm.DataSource.Metrics()

	// Query controller labels
	deploymentLabelsResultFuture := source.WithGroup(grp, ds.QueryDeploymentLabels(start, end))
	statefulSetLabelsResultFuture := source.WithGroup(grp, ds.QueryStatefulSetLabels(start, end))
	daemonSetLabelsResultFuture := source.WithGroup(grp, ds.QueryDaemonSetLabels(start, end))
	jobLabelsResultFuture := source.WithGroup(grp, ds.QueryJobLabels(start, end))
	podsWithRSOwnerResultFuture := source.WithGroup(grp, ds.QueryPodsWithReplicaSetOwner(start, end))

	// Await results
	deploymentLabelsResult, _ := deploymentLabelsResultFuture.Await()
	statefulSetLabelsResult, _ := statefulSetLabelsResultFuture.Await()
	daemonSetLabelsResult, _ := daemonSetLabelsResultFuture.Await()
	jobLabelsResult, _ := jobLabelsResultFuture.Await()
	podsWithRSOwnerResult, _ := podsWithRSOwnerResultFuture.Await()

	// Process deployments
	for _, res := range deploymentLabelsResult {
		kms.RegisterController(res.UID, res.Deployment, res.Namespace, string(kubemodel.ControllerKindDeployment))
		if controller, ok := kms.Controllers[res.UID]; ok {
			controller.Labels = res.Labels
			controller.Start = start
			controller.End = end
		}
	}

	// Process statefulsets
	for _, res := range statefulSetLabelsResult {
		kms.RegisterController(res.UID, res.StatefulSet, res.Namespace, string(kubemodel.ControllerKindStatefulSet))
		if controller, ok := kms.Controllers[res.UID]; ok {
			controller.Labels = res.Labels
			controller.Start = start
			controller.End = end
		}
	}

	// Process daemonsets
	for _, res := range daemonSetLabelsResult {
		kms.RegisterController(res.UID, res.DaemonSet, res.Namespace, string(kubemodel.ControllerKindDaemonSet))
		if controller, ok := kms.Controllers[res.UID]; ok {
			controller.Labels = res.Labels
			controller.Start = start
			controller.End = end
		}
	}

	// Process jobs
	for _, res := range jobLabelsResult {
		kms.RegisterController(res.UID, res.Job, res.Namespace, string(kubemodel.ControllerKindJob))
		if controller, ok := kms.Controllers[res.UID]; ok {
			controller.Labels = res.Labels
			controller.Start = start
			controller.End = end
		}
	}

	// Process pod-controller relationships
	// TODO: Implement proper pod-to-controller mapping through ReplicaSets
	// PodsWithReplicaSetOwnerResult only provides pod-to-ReplicaSet mapping
	// Need additional query (QueryReplicaSetsWithRollout) to map ReplicaSets to Controllers
	for _, res := range podsWithRSOwnerResult {
		_ = res // Placeholder for future implementation
	}

	return nil
}

func (cm *CostModel) kmComputeServices(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	ds := cm.DataSource.Metrics()

	// Query service labels
	serviceLabelsResultFuture := source.WithGroup(grp, ds.QueryServiceLabels(start, end))
	netTransferResultFuture := source.WithGroup(grp, ds.QueryNetInternetServiceGiB(start, end))

	// Await results
	serviceLabelsResult, _ := serviceLabelsResultFuture.Await()
	netTransferResult, _ := netTransferResultFuture.Await()

	// Process service labels
	for _, res := range serviceLabelsResult {
		kms.RegisterService(res.UID, res.Service, res.Namespace)
		if service, ok := kms.Services[res.UID]; ok {
			service.Labels = res.Labels
			service.Start = start
			service.End = end
		}
	}

	// Process network transfer for services
	for _, res := range netTransferResult {
		if service, ok := kms.Services[res.UID]; ok && len(res.Data) > 0 {
			// Convert GiB to bytes
			service.NetworkTransferBytes = uint64(res.Data[0].Value * 1024 * 1024 * 1024)
		}
	}

	return nil
}

func (cm *CostModel) kmComputePVCs(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	ds := cm.DataSource.Metrics()

	// Query PVC info and metrics
	pvcInfoResultFuture := source.WithGroup(grp, ds.QueryPVCInfo(start, end))
	pvcBytesRequestedResultFuture := source.WithGroup(grp, ds.QueryPVCBytesRequested(start, end))
	podPVCAllocationResultFuture := source.WithGroup(grp, ds.QueryPodPVCAllocation(start, end))

	// Await results
	pvcInfoResult, _ := pvcInfoResultFuture.Await()
	pvcBytesRequestedResult, _ := pvcBytesRequestedResultFuture.Await()
	podPVCAllocationResult, _ := podPVCAllocationResultFuture.Await()

	// Process PVC info
	for _, res := range pvcInfoResult {
		kms.RegisterPVC(res.UID, res.PersistentVolumeClaim, res.Namespace)
		if pvc, ok := kms.PersistentVolumeClaims[res.UID]; ok {
			pvc.StorageClass = res.StorageClass
			pvc.VolumeName = res.VolumeName
			pvc.Start = start
			pvc.End = end
		}
	}

	// Process PVC bytes requested
	for _, res := range pvcBytesRequestedResult {
		if pvc, ok := kms.PersistentVolumeClaims[res.UID]; ok && len(res.Data) > 0 {
			pvc.RequestedBytes = uint64(res.Data[0].Value)
		}
	}

	// Process pod-PVC allocations
	for _, res := range podPVCAllocationResult {
		if pvc, ok := kms.PersistentVolumeClaims[res.UID]; ok {
			// Link PVC to pod - look up pod UID by name
			podID := res.UID
			pvc.PodUID = &podID
		}
	}

	return nil
}

func (cm *CostModel) kmComputeVolumes(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	ds := cm.DataSource.Metrics()

	// Query PV info and metrics
	pvInfoResultFuture := source.WithGroup(grp, ds.QueryPVInfo(start, end))
	pvBytesResultFuture := source.WithGroup(grp, ds.QueryPVBytes(start, end))
	pvActiveMinutesResultFuture := source.WithGroup(grp, ds.QueryPVActiveMinutes(start, end))

	// Await results
	pvInfoResult, _ := pvInfoResultFuture.Await()
	pvBytesResult, _ := pvBytesResultFuture.Await()
	pvActiveMinutesResult, _ := pvActiveMinutesResultFuture.Await()

	// Process PV info
	for _, res := range pvInfoResult {
		kms.RegisterVolume(res.PersistentVolume, res.PersistentVolume)
		if volume, ok := kms.Volumes[res.PersistentVolume]; ok {
			volume.StorageClass = res.StorageClass
			volume.Start = start
			volume.End = end
		}
	}

	// Process PV bytes
	for _, res := range pvBytesResult {
		if volume, ok := kms.Volumes[res.PersistentVolume]; ok && len(res.Data) > 0 {
			volume.Size = uint64(res.Data[0].Value)
		}
	}

	// Process active minutes to confirm volume was active
	for _, res := range pvActiveMinutesResult {
		if volume, ok := kms.Volumes[res.PersistentVolume]; ok {
			_ = res.Data // Volume exists and was active
			volume.Start = start
			volume.End = end
		}
	}

	return nil
}
