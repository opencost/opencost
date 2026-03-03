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

	// 2.4 Compute Pods
	err = km.computePods(kms, start, end)
	if err != nil {
		kms.Error(err)
	}

	// 2.5 Compute Deployments
	err = km.computeDeployments(kms, start, end)
	if err != nil {
		kms.Error(err)
	}

	// 2.6 Compute ResourceQuotas
	err = km.computeResourceQuotas(kms, start, end)
	if err != nil {
		kms.Error(err)
	}

	// 3. Mark KubeModelSet as completed
	kms.Metadata.CompletedAt = time.Now().UTC()

	return kms, nil
}

func (km *KubeModel) computeCluster(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	kms.Cluster = &kubemodel.Cluster{
		UID:  km.clusterUID,
		Name: env.GetClusterID(),
	}

	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()
	clusterUptimeResultFuture := source.WithGroup(grp, metrics.QueryClusterUptime(start, end))

	clusterUptimeResult, _ := clusterUptimeResultFuture.Await()

	if len(clusterUptimeResult) != 1 {
		kms.Errorf("%d clusters returning from cluster uptime query", len(clusterUptimeResult))
	}

	for _, res := range clusterUptimeResult {
		if res.UID == km.clusterUID {
			s, e := res.GetStartEnd(start, end, km.ds.Resolution())
			kms.Cluster.Start = s
			kms.Cluster.End = e
		}
	}

	return nil
}

func (km *KubeModel) computeNodes(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	nodeInfoResultFuture := source.WithGroup(grp, metrics.QueryNodeInfo(start, end))
	nodeUptimeResultFuture := source.WithGroup(grp, metrics.QueryNodeUptime(start, end))
	nodeLabelsResultFuture := source.WithGroup(grp, metrics.QueryNodeLabels(start, end))
	nodeCPUCoresCapacityResultFuture := source.WithGroup(grp, metrics.QueryNodeCPUCoresCapacity(start, end))
	nodeRAMBytesCapacityResultFuture := source.WithGroup(grp, metrics.QueryNodeRAMBytesCapacity(start, end))
	nodeGPUCapacityResultFuture := source.WithGroup(grp, metrics.QueryNodeGPUCount(start, end))

	nodeMap := make(map[string]*kubemodel.Node)

	nodeInfoResult, _ := nodeInfoResultFuture.Await()
	for _, res := range nodeInfoResult {
		nodeMap[res.UID] = &kubemodel.Node{
			UID:          res.UID,
			ProviderID:   res.ProviderID,
			Name:         res.Node,
			InstanceType: res.InstanceType,
		}
	}

	nodeUptimeResult, _ := nodeUptimeResultFuture.Await()
	for _, res := range nodeUptimeResult {
		node, ok := nodeMap[res.UID]
		if !ok {
			log.Warnf("node with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}

		node.Start = res.First
		node.End = res.Last
	}

	nodeCPUCoresCapacityResult, _ := nodeCPUCoresCapacityResultFuture.Await()
	for _, res := range nodeCPUCoresCapacityResult {
		node, ok := nodeMap[res.UID]
		if !ok {
			log.Warnf("node with UID '%s' has not been initialized to add CPU cores capacity", res.UID)
			continue
		}
		node.CPUMilliCores = res.CPUCores * 1000
	}

	nodeRAMBytesCapacityResult, _ := nodeRAMBytesCapacityResultFuture.Await()
	for _, res := range nodeRAMBytesCapacityResult {
		node, ok := nodeMap[res.UID]
		if !ok {
			log.Warnf("node with UID '%s' has not been initialized to add RAM bytes capacity", res.UID)
			continue
		}
		node.RAMBytes = res.RAMBytes
	}

	nodeGPUCapacityResult, _ := nodeGPUCapacityResultFuture.Await()
	for _, res := range nodeGPUCapacityResult {
		node, ok := nodeMap[res.UID]
		if !ok {
			log.Warnf("node with UID '%s' has not been initialized to add GPU capacity", res.UID)
			continue
		}
		node.GPUCount = res.GPUCount
	}

	nodeLabelsResult, _ := nodeLabelsResultFuture.Await()
	for _, res := range nodeLabelsResult {
		node, ok := nodeMap[res.UID]
		if !ok {
			log.Warnf("node with UID '%s' has not been initialized to add labels", res.UID)
			continue
		}
		node.Labels = res.Labels
	}

	for _, node := range nodeMap {
		err := kms.RegisterNode(node)
		if err != nil {
			log.Warnf("Failed to register node: %s", err.Error())
		}
	}

	return nil
}

func (km *KubeModel) computeNamespaces(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	nsUptimeResultFuture := source.WithGroup(grp, metrics.QueryNamespaceUptime(start, end))
	nsLabelsResultFuture := source.WithGroup(grp, metrics.QueryNamespaceLabels(start, end))
	nsAnnosResultFuture := source.WithGroup(grp, metrics.QueryNamespaceAnnotations(start, end))

	nsUptimeResult, _ := nsUptimeResultFuture.Await()
	nsLabelsResult, _ := nsLabelsResultFuture.Await()
	nsAnnosResult, _ := nsAnnosResultFuture.Await()

	for _, res := range nsLabelsResult {
		err := kms.RegisterNamespace(res.UID, res.Namespace)
		if err != nil {
			log.Warnf("error registering namespace (%s, %s): %s", res.UID, res.Namespace, err)
			continue
		}
		kms.Namespaces[res.UID].Labels = res.Labels
	}

	for _, res := range nsAnnosResult {
		err := kms.RegisterNamespace(res.UID, res.Namespace)
		if err != nil {
			log.Warnf("error registering namespace (%s, %s): %s", res.UID, res.Namespace, err)
			continue
		}
		kms.Namespaces[res.UID].Annotations = res.Annotations
	}

	for _, res := range nsUptimeResult {
		if _, ok := kms.Namespaces[res.UID]; !ok {
			log.Warnf("could not find ns with uid '%s'", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		kms.Namespaces[res.UID].Start = s
		kms.Namespaces[res.UID].End = e
	}

	return nil
}

func (km *KubeModel) computePods(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	podInfoResultFuture := source.WithGroup(grp, metrics.QueryPodInfo(start, end))
	podUptimeResultFuture := source.WithGroup(grp, metrics.QueryPodUptime(start, end))
	podOwnerResultFuture := source.WithGroup(grp, metrics.QueryPodOwners(start, end))
	podLabelsResultFuture := source.WithGroup(grp, metrics.QueryPodLabels(start, end))
	podAnnosResultFuture := source.WithGroup(grp, metrics.QueryPodAnnotations(start, end))

	podMap := make(map[string]*kubemodel.Pod)

	podInfoResult, _ := podInfoResultFuture.Await()
	for _, res := range podInfoResult {
		podMap[res.UID] = &kubemodel.Pod{
			UID:          res.UID,
			Name:         res.Pod,
			NamespaceUID: res.NamespaceUID,
			NodeUID:      res.NodeUID,
		}
	}

	podUptimeResult, _ := podUptimeResultFuture.Await()
	for _, res := range podUptimeResult {
		pod, ok := podMap[res.UID]
		if !ok {
			log.Warnf("pod with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}

		pod.Start = res.First
		pod.End = res.Last
	}

	podOwnersResult, _ := podOwnerResultFuture.Await()
	for _, res := range podOwnersResult {
		pod, ok := podMap[res.UID]
		if !ok {
			log.Warnf("pod with UID '%s' has not been initialized to add labels", res.UID)
			continue
		}
		pod.Owners = append(pod.Owners, kubemodel.Owner{
			UID:  res.OwnerUID,
			Kind: kubemodel.ParseOwnerKind(res.OwnerKind),
		})
	}

	podLabelsResult, _ := podLabelsResultFuture.Await()
	for _, res := range podLabelsResult {
		pod, ok := podMap[res.UID]
		if !ok {
			log.Warnf("pod with UID '%s' has not been initialized to add labels", res.UID)
			continue
		}
		pod.Labels = res.Labels
	}

	podAnnosResult, _ := podAnnosResultFuture.Await()
	for _, res := range podAnnosResult {
		pod, ok := podMap[res.UID]
		if !ok {
			log.Warnf("pod with UID '%s' has not been initialized to add annotations", res.UID)
			continue
		}
		pod.Annotations = res.Annotations
	}

	for _, pod := range podMap {

		err := kms.RegisterPod(pod)
		if err != nil {
			log.Warnf("Failed to register pod: %s", err.Error())
		}
	}

	return nil
}

func (km *KubeModel) computeDeployments(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	deploymentInfoResultFuture := source.WithGroup(grp, metrics.QueryDeploymentInfo(start, end))
	deploymentUptimeResultFuture := source.WithGroup(grp, metrics.QueryDeploymentUptime(start, end))
	deploymentLabelsResultFuture := source.WithGroup(grp, metrics.QueryDeploymentLabels(start, end))
	deploymentAnnotationsResultFuture := source.WithGroup(grp, metrics.QueryDeploymentAnnotations(start, end))
	deploymentMatchLabelsResultFuture := source.WithGroup(grp, metrics.QueryDeploymentMatchLabels(start, end))

	deploymentMap := make(map[string]*kubemodel.Deployment)

	deploymentInfoResult, _ := deploymentInfoResultFuture.Await()
	for _, res := range deploymentInfoResult {
		deploymentMap[res.UID] = &kubemodel.Deployment{
			UID:          res.UID,
			Name:         res.Deployment,
			NamespaceUID: res.NameSpaceUID,
		}
	}

	deploymentUptimeResult, _ := deploymentUptimeResultFuture.Await()
	for _, res := range deploymentUptimeResult {
		deployment, ok := deploymentMap[res.UID]
		if !ok {
			log.Warnf("deployment with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}

		deployment.Start = res.First
		deployment.End = res.Last
	}

	deploymentLabelsResult, _ := deploymentLabelsResultFuture.Await()
	for _, res := range deploymentLabelsResult {
		deployment, ok := deploymentMap[res.UID]
		if !ok {
			log.Warnf("deployment with UID '%s' has not been initialized to add labels", res.UID)
			continue
		}
		deployment.Labels = res.Labels
	}

	deploymentAnnotationsResult, _ := deploymentAnnotationsResultFuture.Await()
	for _, res := range deploymentAnnotationsResult {
		deployment, ok := deploymentMap[res.UID]
		if !ok {
			log.Warnf("deployment with UID '%s' has not been initialized to add annotations", res.UID)
			continue
		}
		deployment.Annotations = res.Annotations
	}

	deploymentMatchLabelsResult, _ := deploymentMatchLabelsResultFuture.Await()
	for _, res := range deploymentMatchLabelsResult {
		deployment, ok := deploymentMap[res.UID]
		if !ok {
			log.Warnf("deployment with UID '%s' has not been initialized to add match labels", res.UID)
			continue
		}
		deployment.MatchLabels = res.Labels
	}

	for _, deployment := range deploymentMap {
		err := kms.RegisterDeployment(deployment)
		if err != nil {
			log.Warnf("Failed to register deployment: %s", err.Error())
		}
	}

	return nil
}

func (km *KubeModel) computeResourceQuotas(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	rqUptimeResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaUptime(start, end))

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
		err := kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		if err != nil {
			log.Warnf("error registering resource quota (%s, %s, %s): %s", res.UID, res.ResourceQuota, res.Namespace, err)
			continue
		}

		mcpu := res.Data[0].Value * 1000
		kms.ResourceQuotas[res.UID].Spec.Hard.SetRequest(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatAvg, mcpu)
	}

	rqSpecCPURequestMaxResult, _ := rqSpecCPURequestMaxResultFuture.Await()
	for _, res := range rqSpecCPURequestMaxResult {
		err := kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		if err != nil {
			log.Warnf("error registering resource quota (%s, %s, %s): %s", res.UID, res.ResourceQuota, res.Namespace, err)
			continue
		}

		mcpu := res.Data[0].Value * 1000
		kms.ResourceQuotas[res.UID].Spec.Hard.SetRequest(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatMax, mcpu)
	}

	rqSpecRAMRequestAverageResult, _ := rqSpecRAMRequestAverageResultFuture.Await()
	for _, res := range rqSpecRAMRequestAverageResult {
		err := kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		if err != nil {
			log.Warnf("error registering resource quota (%s, %s, %s): %s", res.UID, res.ResourceQuota, res.Namespace, err)
			continue
		}

		kms.ResourceQuotas[res.UID].Spec.Hard.SetRequest(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatAvg, res.Data[0].Value)
	}

	rqSpecRAMRequestMaxResult, _ := rqSpecRAMRequestMaxResultFuture.Await()
	for _, res := range rqSpecRAMRequestMaxResult {
		err := kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		if err != nil {
			log.Warnf("error registering resource quota (%s, %s, %s): %s", res.UID, res.ResourceQuota, res.Namespace, err)
			continue
		}

		kms.ResourceQuotas[res.UID].Spec.Hard.SetRequest(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatMax, res.Data[0].Value)
	}

	rqSpecCPULimitAverageResult, _ := rqSpecCPULimitAverageResultFuture.Await()
	for _, res := range rqSpecCPULimitAverageResult {
		err := kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		if err != nil {
			log.Warnf("error registering resource quota (%s, %s, %s): %s", res.UID, res.ResourceQuota, res.Namespace, err)
			continue
		}

		mcpu := res.Data[0].Value * 1000
		kms.ResourceQuotas[res.UID].Spec.Hard.SetLimit(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatAvg, mcpu)
	}

	rqSpecCPULimitMaxResult, _ := rqSpecCPULimitMaxResultFuture.Await()
	for _, res := range rqSpecCPULimitMaxResult {
		err := kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		if err != nil {
			log.Warnf("error registering resource quota (%s, %s, %s): %s", res.UID, res.ResourceQuota, res.Namespace, err)
			continue
		}

		mcpu := res.Data[0].Value * 1000
		kms.ResourceQuotas[res.UID].Spec.Hard.SetLimit(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatMax, mcpu)
	}

	rqSpecRAMLimitAverageResult, _ := rqSpecRAMLimitAverageResultFuture.Await()
	for _, res := range rqSpecRAMLimitAverageResult {
		err := kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		if err != nil {
			log.Warnf("error registering resource quota (%s, %s, %s): %s", res.UID, res.ResourceQuota, res.Namespace, err)
			continue
		}

		kms.ResourceQuotas[res.UID].Spec.Hard.SetLimit(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatAvg, res.Data[0].Value)
	}

	rqSpecRAMLimitMaxResult, _ := rqSpecRAMLimitMaxResultFuture.Await()
	for _, res := range rqSpecRAMLimitMaxResult {
		err := kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		if err != nil {
			log.Warnf("error registering resource quota (%s, %s, %s): %s", res.UID, res.ResourceQuota, res.Namespace, err)
			continue
		}

		kms.ResourceQuotas[res.UID].Spec.Hard.SetLimit(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatMax, res.Data[0].Value)
	}

	rqStatusUsedCPURequestAverageResult, _ := rqStatusUsedCPURequestAverageResultFuture.Await()
	for _, res := range rqStatusUsedCPURequestAverageResult {
		err := kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		if err != nil {
			log.Warnf("error registering resource quota (%s, %s, %s): %s", res.UID, res.ResourceQuota, res.Namespace, err)
			continue
		}

		mcpu := res.Data[0].Value * 1000
		kms.ResourceQuotas[res.UID].Status.Used.SetRequest(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatAvg, mcpu)
	}

	rqStatusUsedCPURequestMaxResult, _ := rqStatusUsedCPURequestMaxResultFuture.Await()
	for _, res := range rqStatusUsedCPURequestMaxResult {
		err := kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		if err != nil {
			log.Warnf("error registering resource quota (%s, %s, %s): %s", res.UID, res.ResourceQuota, res.Namespace, err)
			continue
		}

		mcpu := res.Data[0].Value * 1000
		kms.ResourceQuotas[res.UID].Status.Used.SetRequest(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatMax, mcpu)
	}

	rqStatusUsedRAMRequestAverageResult, _ := rqStatusUsedRAMRequestAverageResultFuture.Await()
	for _, res := range rqStatusUsedRAMRequestAverageResult {
		err := kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		if err != nil {
			log.Warnf("error registering resource quota (%s, %s, %s): %s", res.UID, res.ResourceQuota, res.Namespace, err)
			continue
		}

		kms.ResourceQuotas[res.UID].Status.Used.SetRequest(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatAvg, res.Data[0].Value)
	}

	rqStatusUsedRAMRequestMaxResult, _ := rqStatusUsedRAMRequestMaxResultFuture.Await()
	for _, res := range rqStatusUsedRAMRequestMaxResult {
		err := kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		if err != nil {
			log.Warnf("error registering resource quota (%s, %s, %s): %s", res.UID, res.ResourceQuota, res.Namespace, err)
			continue
		}

		kms.ResourceQuotas[res.UID].Status.Used.SetRequest(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatMax, res.Data[0].Value)
	}

	rqStatusUsedCPULimitAverageResult, _ := rqStatusUsedCPULimitAverageResultFuture.Await()
	for _, res := range rqStatusUsedCPULimitAverageResult {
		err := kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		if err != nil {
			log.Warnf("error registering resource quota (%s, %s, %s): %s", res.UID, res.ResourceQuota, res.Namespace, err)
			continue
		}

		mcpu := res.Data[0].Value * 1000
		kms.ResourceQuotas[res.UID].Status.Used.SetLimit(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatAvg, mcpu)
	}

	rqStatusUsedCPULimitMaxResult, _ := rqStatusUsedCPULimitMaxResultFuture.Await()
	for _, res := range rqStatusUsedCPULimitMaxResult {
		err := kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		if err != nil {
			log.Warnf("error registering resource quota (%s, %s, %s): %s", res.UID, res.ResourceQuota, res.Namespace, err)
			continue
		}

		mcpu := res.Data[0].Value * 1000
		kms.ResourceQuotas[res.UID].Status.Used.SetLimit(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatMax, mcpu)
	}

	rqStatusUsedRAMLimitAverageResult, _ := rqStatusUsedRAMLimitAverageResultFuture.Await()
	for _, res := range rqStatusUsedRAMLimitAverageResult {
		err := kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		if err != nil {
			log.Warnf("error registering resource quota (%s, %s, %s): %s", res.UID, res.ResourceQuota, res.Namespace, err)
			continue
		}

		kms.ResourceQuotas[res.UID].Status.Used.SetLimit(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatAvg, res.Data[0].Value)
	}

	rqStatusUsedRAMLimitMaxResult, _ := rqStatusUsedRAMLimitMaxResultFuture.Await()
	for _, res := range rqStatusUsedRAMLimitMaxResult {
		err := kms.RegisterResourceQuota(res.UID, res.ResourceQuota, res.Namespace)
		if err != nil {
			log.Warnf("error registering resource quota (%s, %s, %s): %s", res.UID, res.ResourceQuota, res.Namespace, err)
			continue
		}

		kms.ResourceQuotas[res.UID].Status.Used.SetLimit(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatMax, res.Data[0].Value)
	}

	rqUptimeResult, _ := rqUptimeResultFuture.Await()
	for _, res := range rqUptimeResult {
		if _, ok := kms.ResourceQuotas[res.UID]; !ok {
			log.Warnf("could not find rq with uid '%s'", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		kms.ResourceQuotas[res.UID].Start = s
		kms.ResourceQuotas[res.UID].End = e
	}

	return nil
}
