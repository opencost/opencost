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

func (km *KubeModel) computeCluster(kms *kubemodel.KubeModelSet, start, end time.Time) error {

	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()
	clusterInfoResultFuture := source.WithGroup(grp, metrics.QueryClusterInfo(start, end))
	clusterUptimeResultFuture := source.WithGroup(grp, metrics.QueryClusterUptime(start, end))

	clusterMap := make(map[string]*kubemodel.Cluster)

	clusterInfoResult, _ := clusterInfoResultFuture.Await()
	for _, res := range clusterInfoResult {
		clusterMap[res.UID] = &kubemodel.Cluster{
			UID:      res.UID,
			Provider: kubemodel.ParseProvider(res.Provider),
			Account:  res.AccountID,
			Name:     res.Cluster,
			Region:   res.Region,
		}
	}

	clusterUptimeResult, _ := clusterUptimeResultFuture.Await()
	for _, res := range clusterUptimeResult {
		cluster, ok := clusterMap[res.UID]
		if !ok {
			log.Warnf("cluster with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		cluster.Start = s
		cluster.End = e
	}

	cluster, ok := clusterMap[km.clusterUID]
	if !ok {
		return fmt.Errorf("failed to compute cluster with UID '%s'", km.clusterUID)
	}

	kms.RegisterCluster(cluster)

	return nil
}

func (km *KubeModel) computeNodes(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	nodeInfoResultFuture := source.WithGroup(grp, metrics.QueryNodeInfo(start, end))
	nodeUptimeResultFuture := source.WithGroup(grp, metrics.QueryNodeUptime(start, end))
	nodeLabelsResultFuture := source.WithGroup(grp, metrics.QueryNodeLabels(start, end))
	// TODO make sure that UID is being populated correctly here
	nodeIsSpotResultFuture := source.WithGroup(grp, metrics.QueryNodeIsSpot(start, end))
	nodeResourceCapacitiesFuture := source.WithGroup(grp, metrics.QueryNodeResourceCapacities(start, end))
	nodeResourcesAllocatableFuture := source.WithGroup(grp, metrics.QueryNodeResourcesAllocatable(start, end))
	// TODO before merge add Node UID to Nodestates metrics
	//localStorageBytesFuture := source.WithGroup(grp, metrics.QueryLocalStorageBytes(start, end))
	//localStorageUsedAvgFuture := source.WithGroup(grp, metrics.QueryLocalStorageUsedAvg(start, end))
	//localStorageUsedMaxFuture := source.WithGroup(grp, metrics.QueryLocalStorageUsedMax(start, end))

	nodeMap := make(map[string]*kubemodel.Node)

	nodeInfoResult, _ := nodeInfoResultFuture.Await()
	for _, res := range nodeInfoResult {
		nodeMap[res.UID] = &kubemodel.Node{
			UID:                  res.UID,
			ProviderID:           res.ProviderID,
			Name:                 res.Node,
			InstanceType:         res.InstanceType,
			ResourceCapacities:   make(kubemodel.ResourceQuantities),
			ResourcesAllocatable: make(kubemodel.ResourceQuantities),
		}
	}

	nodeUptimeResult, _ := nodeUptimeResultFuture.Await()
	for _, res := range nodeUptimeResult {
		node, ok := nodeMap[res.UID]
		if !ok {
			log.Warnf("node with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		node.Start = s
		node.End = e
	}

	nodeResourceCapacitiesResult, _ := nodeResourceCapacitiesFuture.Await()
	for _, res := range nodeResourceCapacitiesResult {
		node, ok := nodeMap[res.UID]
		if !ok {
			log.Warnf("node with UID '%s' has not been initialized to add resource capacities", res.UID)
			continue
		}
		resource, unit, value := resourceUnitValue(res.Resource, res.Unit, res.Value)
		node.ResourceCapacities.Set(resource, unit, kubemodel.StatAvg, value)
	}

	nodeResourcesAllocatableResult, _ := nodeResourcesAllocatableFuture.Await()
	for _, res := range nodeResourcesAllocatableResult {
		node, ok := nodeMap[res.UID]
		if !ok {
			log.Warnf("node with UID '%s' has not been initialized to add resources allocatable", res.UID)
			continue
		}
		resource, unit, value := resourceUnitValue(res.Resource, res.Unit, res.Value)
		node.ResourcesAllocatable.Set(resource, unit, kubemodel.StatAvg, value)
	}

	nodeIsSpotResult, _ := nodeIsSpotResultFuture.Await()
	for _, res := range nodeIsSpotResult {
		node, ok := nodeMap[res.UID]
		if !ok {
			log.Warnf("node with UID '%s' has not been initialized to add spot status", res.UID)
			continue
		}
		if len(res.Data) > 0 {
			node.Preemptible = res.Data[0].Value > 0
		}
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

	//localStorageBytesResult, _ := localStorageBytesFuture.Await()
	//for _, res := range localStorageBytesResult {
	//	uid, ok := nodeNameToUID[res.Instance]
	//	if !ok {
	//		continue
	//	}
	//	node := nodeMap[uid]
	//	if len(res.Data) > 0 {
	//		node.FileSystem.CapacityBytes = res.Data[0].Value
	//	}
	//}
	//
	//localStorageUsedAvgResult, _ := localStorageUsedAvgFuture.Await()
	//for _, res := range localStorageUsedAvgResult {
	//	uid, ok := nodeNameToUID[res.Instance]
	//	if !ok {
	//		continue
	//	}
	//	node := nodeMap[uid]
	//	if len(res.Data) > 0 {
	//		node.FileSystem.UsageByteAvg = res.Data[0].Value
	//	}
	//}
	//
	//localStorageUsedMaxResult, _ := localStorageUsedMaxFuture.Await()
	//for _, res := range localStorageUsedMaxResult {
	//	uid, ok := nodeNameToUID[res.Instance]
	//	if !ok {
	//		continue
	//	}
	//	node := nodeMap[uid]
	//	if len(res.Data) > 0 {
	//		node.FileSystem.UsageByteMax = res.Data[0].Value
	//	}
	//}

	for _, node := range nodeMap {
		err := kms.RegisterNode(node)
		if err != nil {
			log.Warnf("Failed to register node: %s", err.Error())
		}
	}

	return nil
}

// resourceUnitValue converts prometheus resource/unit strings from ResourceResult
// into kubemodel types, applying any necessary unit conversions.
func resourceUnitValue(resource, unit string, value float64) (kubemodel.Resource, kubemodel.Unit, float64) {
	switch resource {
	case "cpu":
		return kubemodel.ResourceCPU, kubemodel.UnitCore, value
	case "memory":
		return kubemodel.ResourceMemory, kubemodel.UnitByte, value
	default:
		return kubemodel.Resource(resource), kubemodel.Unit(unit), value
	}
}

// computeDevices must take place after computePod and computeNode
func (km *KubeModel) computeNamespaces(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	nsInfoResultFuture := source.WithGroup(grp, metrics.QueryNamespaceInfo(start, end))
	nsUptimeResultFuture := source.WithGroup(grp, metrics.QueryNamespaceUptime(start, end))
	nsLabelsResultFuture := source.WithGroup(grp, metrics.QueryNamespaceLabels(start, end))
	nsAnnosResultFuture := source.WithGroup(grp, metrics.QueryNamespaceAnnotations(start, end))

	nsMap := make(map[string]*kubemodel.Namespace)

	// Initialize namespaces from info
	nsInfoResult, _ := nsInfoResultFuture.Await()
	for _, res := range nsInfoResult {
		nsMap[res.UID] = &kubemodel.Namespace{
			UID:  res.UID,
			Name: res.Namespace,
		}
	}

	nsUptimeResult, _ := nsUptimeResultFuture.Await()
	for _, res := range nsUptimeResult {
		ns, ok := nsMap[res.UID]
		if !ok {
			log.Warnf("namespace with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		ns.Start = s
		ns.End = e
	}

	nsLabelsResult, _ := nsLabelsResultFuture.Await()
	for _, res := range nsLabelsResult {
		ns, ok := nsMap[res.UID]
		if !ok {
			log.Warnf("namespace with UID '%s' has not been initialized to add labels", res.UID)
			continue
		}
		ns.Labels = res.Labels
	}

	nsAnnosResult, _ := nsAnnosResultFuture.Await()
	for _, res := range nsAnnosResult {
		ns, ok := nsMap[res.UID]
		if !ok {
			log.Warnf("namespace with UID '%s' has not been initialized to add annotations", res.UID)
			continue
		}
		ns.Annotations = res.Annotations
	}

	for _, namespace := range nsMap {
		err := kms.RegisterNamespace(namespace)
		if err != nil {
			log.Warnf("Failed to register namespace: %s", err.Error())
		}
	}

	return nil
}

func (km *KubeModel) computePods(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	podInfoResultFuture := source.WithGroup(grp, metrics.QueryPodInfo(start, end))
	podUptimeResultFuture := source.WithGroup(grp, metrics.QueryPodUptime(start, end))
	podOwnerResultFuture := source.WithGroup(grp, metrics.QueryPodOwners(start, end))
	podPVCVolumesResultFuture := source.WithGroup(grp, metrics.QueryPodPVCVolumes(start, end))
	podLabelsResultFuture := source.WithGroup(grp, metrics.QueryPodLabels(start, end))
	podAnnosResultFuture := source.WithGroup(grp, metrics.QueryPodAnnotations(start, end))

	podNetworkEgressBytesResultFuture := source.WithGroup(grp, metrics.QueryPodNetworkEgressBytes(start, end))
	podNetworkIngressBytesResultFuture := source.WithGroup(grp, metrics.QueryPodNetworkIngressBytes(start, end))

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
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		pod.Start = s
		pod.End = e
	}

	podOwnersResult, _ := podOwnerResultFuture.Await()
	for _, res := range podOwnersResult {
		pod, ok := podMap[res.UID]
		if !ok {
			log.Warnf("pod with UID '%s' has not been initialized to add labels", res.UID)
			continue
		}
		pod.Owners = append(pod.Owners, kubemodel.Owner{
			UID:        res.OwnerUID,
			Kind:       kubemodel.ParseOwnerKind(res.OwnerKind),
			Controller: res.Controller,
		})
	}

	podPVCVolumesResult, _ := podPVCVolumesResultFuture.Await()
	for _, res := range podPVCVolumesResult {
		pod, ok := podMap[res.UID]
		if !ok {
			log.Warnf("pod with UID '%s' has not been initialized to add PVC volumes", res.UID)
			continue
		}
		pod.PVCVolumes = append(pod.PVCVolumes, kubemodel.PodPVCVolumes{
			Name:                     res.PodVolumeName,
			PersistentVolumeClaimUID: res.PVCUID,
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

	appendDetail := func(uid string, dir kubemodel.TrafficDirection, tt kubemodel.TrafficType, isNatGateway bool, endpoint string, bytes float64) {
		pod, ok := podMap[uid]
		if !ok || bytes <= 0 {
			return
		}
		pod.NetworkTrafficDetails = append(pod.NetworkTrafficDetails, kubemodel.NetworkTrafficDetail{
			PodUID:           uid,
			TrafficDirection: dir,
			TrafficType:      tt,
			IsNatGateway:     isNatGateway,
			Endpoint:         endpoint,
			Bytes:            bytes,
		})
	}

	networkTrafficType := func(res *source.PodNetworkBytesResult) (kubemodel.TrafficType, bool) {
		if res.Internet {
			return kubemodel.TrafficTypeInternet, true
		}
		if !res.SameRegion {
			return kubemodel.TrafficTypeCrossRegion, true
		}
		if !res.SameZone {
			return kubemodel.TrafficTypeCrossZone, true
		}
		return "", false
	}

	podNetworkEgressResult, _ := podNetworkEgressBytesResultFuture.Await()
	for _, res := range podNetworkEgressResult {
		tt, ok := networkTrafficType(res)
		if !ok {
			continue
		}
		appendDetail(res.UID, kubemodel.TrafficDirectionEgress, tt, res.NatGateway, res.Service, res.Value)
	}

	podNetworkIngressResult, _ := podNetworkIngressBytesResultFuture.Await()
	for _, res := range podNetworkIngressResult {
		tt, ok := networkTrafficType(res)
		if !ok {
			continue
		}
		appendDetail(res.UID, kubemodel.TrafficDirectionIngress, tt, res.NatGateway, res.Service, res.Value)
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
			NamespaceUID: res.NamespaceUID,
		}
	}

	deploymentUptimeResult, _ := deploymentUptimeResultFuture.Await()
	for _, res := range deploymentUptimeResult {
		deployment, ok := deploymentMap[res.UID]
		if !ok {
			log.Warnf("deployment with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		deployment.Start = s
		deployment.End = e
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

func (km *KubeModel) computeStatefulSets(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	statefulSetInfoResultFuture := source.WithGroup(grp, metrics.QueryStatefulSetInfo(start, end))
	statefulSetUptimeResultFuture := source.WithGroup(grp, metrics.QueryStatefulSetUptime(start, end))
	statefulSetLabelsResultFuture := source.WithGroup(grp, metrics.QueryStatefulSetLabels(start, end))
	statefulSetAnnotationsResultFuture := source.WithGroup(grp, metrics.QueryStatefulSetAnnotations(start, end))
	statefulSetMatchLabelsResultFuture := source.WithGroup(grp, metrics.QueryStatefulSetMatchLabels(start, end))

	statefulSetMap := make(map[string]*kubemodel.StatefulSet)

	statefulSetInfoResult, _ := statefulSetInfoResultFuture.Await()
	for _, res := range statefulSetInfoResult {
		statefulSetMap[res.UID] = &kubemodel.StatefulSet{
			UID:          res.UID,
			Name:         res.StatefulSet,
			NamespaceUID: res.NamespaceUID,
		}
	}

	statefulSetUptimeResult, _ := statefulSetUptimeResultFuture.Await()
	for _, res := range statefulSetUptimeResult {
		statefulSet, ok := statefulSetMap[res.UID]
		if !ok {
			log.Warnf("statefulset with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		statefulSet.Start = s
		statefulSet.End = e
	}

	statefulSetLabelsResult, _ := statefulSetLabelsResultFuture.Await()
	for _, res := range statefulSetLabelsResult {
		statefulSet, ok := statefulSetMap[res.UID]
		if !ok {
			log.Warnf("statefulset with UID '%s' has not been initialized to add labels", res.UID)
			continue
		}
		statefulSet.Labels = res.Labels
	}

	statefulSetAnnotationsResult, _ := statefulSetAnnotationsResultFuture.Await()
	for _, res := range statefulSetAnnotationsResult {
		statefulSet, ok := statefulSetMap[res.UID]
		if !ok {
			log.Warnf("statefulset with UID '%s' has not been initialized to add annotations", res.UID)
			continue
		}
		statefulSet.Annotations = res.Annotations
	}

	statefulSetMatchLabelsResult, _ := statefulSetMatchLabelsResultFuture.Await()
	for _, res := range statefulSetMatchLabelsResult {
		statefulSet, ok := statefulSetMap[res.UID]
		if !ok {
			log.Warnf("statefulset with UID '%s' has not been initialized to add match labels", res.UID)
			continue
		}
		statefulSet.MatchLabels = res.Labels
	}

	for _, statefulSet := range statefulSetMap {
		err := kms.RegisterStatefulSet(statefulSet)
		if err != nil {
			log.Warnf("Failed to register statefulset: %s", err.Error())
		}
	}

	return nil
}

func (km *KubeModel) computeDaemonSets(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	daemonSetInfoResultFuture := source.WithGroup(grp, metrics.QueryDaemonSetInfo(start, end))
	daemonSetUptimeResultFuture := source.WithGroup(grp, metrics.QueryDaemonSetUptime(start, end))
	daemonSetLabelsResultFuture := source.WithGroup(grp, metrics.QueryDaemonSetLabels(start, end))
	daemonSetAnnotationsResultFuture := source.WithGroup(grp, metrics.QueryDaemonSetAnnotations(start, end))

	daemonSetMap := make(map[string]*kubemodel.DaemonSet)

	daemonSetInfoResult, _ := daemonSetInfoResultFuture.Await()
	for _, res := range daemonSetInfoResult {
		daemonSetMap[res.UID] = &kubemodel.DaemonSet{
			UID:          res.UID,
			Name:         res.DaemonSet,
			NamespaceUID: res.NamespaceUID,
		}
	}

	daemonSetUptimeResult, _ := daemonSetUptimeResultFuture.Await()
	for _, res := range daemonSetUptimeResult {
		daemonSet, ok := daemonSetMap[res.UID]
		if !ok {
			log.Warnf("daemonset with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		daemonSet.Start = s
		daemonSet.End = e
	}

	daemonSetLabelsResult, _ := daemonSetLabelsResultFuture.Await()
	for _, res := range daemonSetLabelsResult {
		daemonSet, ok := daemonSetMap[res.UID]
		if !ok {
			log.Warnf("daemonset with UID '%s' has not been initialized to add labels", res.UID)
			continue
		}
		daemonSet.Labels = res.Labels
	}

	daemonSetAnnotationsResult, _ := daemonSetAnnotationsResultFuture.Await()
	for _, res := range daemonSetAnnotationsResult {
		daemonSet, ok := daemonSetMap[res.UID]
		if !ok {
			log.Warnf("daemonset with UID '%s' has not been initialized to add annotations", res.UID)
			continue
		}
		daemonSet.Annotations = res.Annotations
	}

	for _, daemonSet := range daemonSetMap {
		err := kms.RegisterDaemonSet(daemonSet)
		if err != nil {
			log.Warnf("Failed to register daemonset: %s", err.Error())
		}
	}

	return nil
}

func (km *KubeModel) computeJobs(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	jobInfoResultFuture := source.WithGroup(grp, metrics.QueryJobInfo(start, end))
	jobUptimeResultFuture := source.WithGroup(grp, metrics.QueryJobUptime(start, end))
	jobLabelsResultFuture := source.WithGroup(grp, metrics.QueryJobLabels(start, end))
	jobAnnotationsResultFuture := source.WithGroup(grp, metrics.QueryJobAnnotations(start, end))

	jobMap := make(map[string]*kubemodel.Job)

	jobInfoResult, _ := jobInfoResultFuture.Await()
	for _, res := range jobInfoResult {
		jobMap[res.UID] = &kubemodel.Job{
			UID:          res.UID,
			Name:         res.Job,
			NamespaceUID: res.NamespaceUID,
		}
	}

	jobUptimeResult, _ := jobUptimeResultFuture.Await()
	for _, res := range jobUptimeResult {
		job, ok := jobMap[res.UID]
		if !ok {
			log.Warnf("job with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		job.Start = s
		job.End = e
	}

	jobLabelsResult, _ := jobLabelsResultFuture.Await()
	for _, res := range jobLabelsResult {
		job, ok := jobMap[res.UID]
		if !ok {
			log.Warnf("job with UID '%s' has not been initialized to add labels", res.UID)
			continue
		}
		job.Labels = res.Labels
	}

	jobAnnotationsResult, _ := jobAnnotationsResultFuture.Await()
	for _, res := range jobAnnotationsResult {
		job, ok := jobMap[res.UID]
		if !ok {
			log.Warnf("job with UID '%s' has not been initialized to add annotations", res.UID)
			continue
		}
		job.Annotations = res.Annotations
	}

	for _, job := range jobMap {
		err := kms.RegisterJob(job)
		if err != nil {
			log.Warnf("Failed to register job: %s", err.Error())
		}
	}

	return nil
}

func (km *KubeModel) computeCronJobs(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	cronJobInfoResultFuture := source.WithGroup(grp, metrics.QueryCronJobInfo(start, end))
	cronJobUptimeResultFuture := source.WithGroup(grp, metrics.QueryCronJobUptime(start, end))
	cronJobLabelsResultFuture := source.WithGroup(grp, metrics.QueryCronJobLabels(start, end))
	cronJobAnnotationsResultFuture := source.WithGroup(grp, metrics.QueryCronJobAnnotations(start, end))

	cronJobMap := make(map[string]*kubemodel.CronJob)

	cronJobInfoResult, _ := cronJobInfoResultFuture.Await()
	for _, res := range cronJobInfoResult {
		cronJobMap[res.UID] = &kubemodel.CronJob{
			UID:          res.UID,
			Name:         res.CronJob,
			NamespaceUID: res.NamespaceUID,
		}
	}

	cronJobUptimeResult, _ := cronJobUptimeResultFuture.Await()
	for _, res := range cronJobUptimeResult {
		cronJob, ok := cronJobMap[res.UID]
		if !ok {
			log.Warnf("cronjob with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		cronJob.Start = s
		cronJob.End = e
	}

	cronJobLabelsResult, _ := cronJobLabelsResultFuture.Await()
	for _, res := range cronJobLabelsResult {
		cronJob, ok := cronJobMap[res.UID]
		if !ok {
			log.Warnf("cronjob with UID '%s' has not been initialized to add labels", res.UID)
			continue
		}
		cronJob.Labels = res.Labels
	}

	cronJobAnnotationsResult, _ := cronJobAnnotationsResultFuture.Await()
	for _, res := range cronJobAnnotationsResult {
		cronJob, ok := cronJobMap[res.UID]
		if !ok {
			log.Warnf("cronjob with UID '%s' has not been initialized to add annotations", res.UID)
			continue
		}
		cronJob.Annotations = res.Annotations
	}

	for _, cronJob := range cronJobMap {
		err := kms.RegisterCronJob(cronJob)
		if err != nil {
			log.Warnf("Failed to register cronjob: %s", err.Error())
		}
	}

	return nil
}

func (km *KubeModel) computeReplicaSets(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	replicaSetInfoResultFuture := source.WithGroup(grp, metrics.QueryReplicaSetInfo(start, end))
	replicaSetUptimeResultFuture := source.WithGroup(grp, metrics.QueryReplicaSetUptime(start, end))
	replicaSetOwnerResultFuture := source.WithGroup(grp, metrics.QueryReplicaSetOwners(start, end))
	replicaSetLabelsResultFuture := source.WithGroup(grp, metrics.QueryReplicaSetLabels(start, end))
	replicaSetAnnotationsResultFuture := source.WithGroup(grp, metrics.QueryReplicaSetAnnotations(start, end))

	replicaSetMap := make(map[string]*kubemodel.ReplicaSet)

	replicaSetInfoResult, _ := replicaSetInfoResultFuture.Await()
	for _, res := range replicaSetInfoResult {
		replicaSetMap[res.UID] = &kubemodel.ReplicaSet{
			UID:          res.UID,
			Name:         res.ReplicaSet,
			NamespaceUID: res.NamespaceUID,
		}
	}

	replicaSetUptimeResult, _ := replicaSetUptimeResultFuture.Await()
	for _, res := range replicaSetUptimeResult {
		replicaSet, ok := replicaSetMap[res.UID]
		if !ok {
			log.Warnf("replicaset with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		replicaSet.Start = s
		replicaSet.End = e
	}

	replicaSetOwnersResult, _ := replicaSetOwnerResultFuture.Await()
	for _, res := range replicaSetOwnersResult {
		replicaSet, ok := replicaSetMap[res.UID]
		if !ok {
			log.Warnf("replicaset with UID '%s' has not been initialized to add owner", res.UID)
			continue
		}
		replicaSet.Owners = append(replicaSet.Owners, kubemodel.Owner{
			UID:        res.OwnerUID,
			Kind:       kubemodel.ParseOwnerKind(res.OwnerKind),
			Controller: res.Controller,
		})
	}

	replicaSetLabelsResult, _ := replicaSetLabelsResultFuture.Await()
	for _, res := range replicaSetLabelsResult {
		replicaSet, ok := replicaSetMap[res.UID]
		if !ok {
			log.Warnf("replicaset with UID '%s' has not been initialized to add labels", res.UID)
			continue
		}
		replicaSet.Labels = res.Labels
	}

	replicaSetAnnotationsResult, _ := replicaSetAnnotationsResultFuture.Await()
	for _, res := range replicaSetAnnotationsResult {
		replicaSet, ok := replicaSetMap[res.UID]
		if !ok {
			log.Warnf("replicaset with UID '%s' has not been initialized to add annotations", res.UID)
			continue
		}
		replicaSet.Annotations = res.Annotations
	}

	for _, replicaSet := range replicaSetMap {
		err := kms.RegisterReplicaSet(replicaSet)
		if err != nil {
			log.Warnf("Failed to register replicaset: %s", err.Error())
		}
	}

	return nil
}

func (km *KubeModel) computeContainers(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	containerUptimeFuture := source.WithGroup(grp, metrics.QueryContainerUptime(start, end))

	containerResourceRequestsFuture := source.WithGroup(grp, metrics.QueryContainerResourceRequests(start, end))
	containerResourceLimitsFuture := source.WithGroup(grp, metrics.QueryContainerResourceLimits(start, end))

	cpuUsageAvgFuture := source.WithGroup(grp, metrics.QueryCPUUsageAvg(start, end))
	cpuUsageMaxFuture := source.WithGroup(grp, metrics.QueryCPUUsageMax(start, end))

	ramUsageAvgFuture := source.WithGroup(grp, metrics.QueryRAMUsageAvg(start, end))
	ramUsageMaxFuture := source.WithGroup(grp, metrics.QueryRAMUsageMax(start, end))

	type containerKey struct {
		podUID string
		name   string
	}

	containerMap := make(map[containerKey]*kubemodel.Container)

	containerUptimeResult, _ := containerUptimeFuture.Await()
	for _, res := range containerUptimeResult {
		key := containerKey{podUID: res.UID, name: res.Container}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		containerMap[key] = &kubemodel.Container{
			PodUID:           res.UID,
			Name:             res.Container,
			ResourceRequests: make(kubemodel.ResourceQuantities),
			ResourceLimits:   make(kubemodel.ResourceQuantities),
			Start:            s,
			End:              e,
		}
	}

	containerResourceRequestsResult, _ := containerResourceRequestsFuture.Await()
	for _, res := range containerResourceRequestsResult {
		key := containerKey{podUID: res.UID, name: res.Container}
		container, ok := containerMap[key]
		if !ok {
			log.Warnf("container %s/%s has not been initialized to add resource requests", res.UID, res.Container)
			continue
		}
		resource, unit, value := resourceUnitValue(res.Resource, res.Unit, res.Value)
		container.ResourceRequests.Set(resource, unit, kubemodel.StatAvg, value)
	}

	containerResourceLimitsResult, _ := containerResourceLimitsFuture.Await()
	for _, res := range containerResourceLimitsResult {
		key := containerKey{podUID: res.UID, name: res.Container}
		container, ok := containerMap[key]
		if !ok {
			log.Warnf("container %s/%s has not been initialized to add resource limits", res.UID, res.Container)
			continue
		}
		resource, unit, value := resourceUnitValue(res.Resource, res.Unit, res.Value)
		container.ResourceLimits.Set(resource, unit, kubemodel.StatAvg, value)
	}

	cpuUsageAvgResult, _ := cpuUsageAvgFuture.Await()
	for _, res := range cpuUsageAvgResult {
		key := containerKey{podUID: res.UID, name: res.Container}
		container, ok := containerMap[key]
		if !ok {
			log.Warnf("container %s/%s has not been initialized to add CPU usage avg", res.UID, res.Container)
			continue
		}
		if len(res.Data) > 0 {
			container.CPUCoreUsageAvg = res.Data[0].Value
		}
	}

	cpuUsageMaxResult, _ := cpuUsageMaxFuture.Await()
	for _, res := range cpuUsageMaxResult {
		key := containerKey{podUID: res.UID, name: res.Container}
		container, ok := containerMap[key]
		if !ok {
			log.Warnf("container %s/%s has not been initialized to add CPU usage max", res.UID, res.Container)
			continue
		}
		if len(res.Data) > 0 {
			container.CPUCoreUsageMax = res.Data[0].Value
		}
	}

	ramUsageAvgResult, _ := ramUsageAvgFuture.Await()
	for _, res := range ramUsageAvgResult {
		key := containerKey{podUID: res.UID, name: res.Container}
		container, ok := containerMap[key]
		if !ok {
			log.Warnf("container %s/%s has not been initialized to add RAM usage avg", res.UID, res.Container)
			continue
		}
		if len(res.Data) > 0 {
			container.RAMBytesUsageAvg = res.Data[0].Value
		}
	}

	ramUsageMaxResult, _ := ramUsageMaxFuture.Await()
	for _, res := range ramUsageMaxResult {
		key := containerKey{podUID: res.UID, name: res.Container}
		container, ok := containerMap[key]
		if !ok {
			log.Warnf("container %s/%s has not been initialized to add RAM usage max", res.UID, res.Container)
			continue
		}
		if len(res.Data) > 0 {
			container.RAMBytesUsageMax = res.Data[0].Value
		}
	}

	for _, container := range containerMap {
		err := kms.RegisterContainer(container)
		if err != nil {
			log.Warnf("Failed to register container: %s", err.Error())
		}
	}

	return nil
}

func (km *KubeModel) computeResourceQuotas(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	rqInfoResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaInfo(start, end))
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

	rqMap := make(map[string]*kubemodel.ResourceQuota)

	// Initialize resource quotas from info
	rqInfoResult, _ := rqInfoResultFuture.Await()
	for _, res := range rqInfoResult {
		rqMap[res.UID] = &kubemodel.ResourceQuota{
			UID:          res.UID,
			Name:         res.ResourceQuota,
			NamespaceUID: res.NamespaceUID,
			Spec:         &kubemodel.ResourceQuotaSpec{Hard: &kubemodel.ResourceQuotaSpecHard{}},
			Status:       &kubemodel.ResourceQuotaStatus{Used: &kubemodel.ResourceQuotaStatusUsed{}},
		}
	}

	rqUptimeResult, _ := rqUptimeResultFuture.Await()
	for _, res := range rqUptimeResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		rq.Start = s
		rq.End = e
	}

	rqSpecCPURequestAverageResult, _ := rqSpecCPURequestAverageResultFuture.Await()
	for _, res := range rqSpecCPURequestAverageResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add spec CPU request average", res.UID)
			continue
		}

		mcpu := res.Value * 1000
		rq.Spec.Hard.SetRequest(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatAvg, mcpu)

	}

	rqSpecCPURequestMaxResult, _ := rqSpecCPURequestMaxResultFuture.Await()
	for _, res := range rqSpecCPURequestMaxResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add spec CPU request max", res.UID)
			continue
		}

		mcpu := res.Value * 1000
		rq.Spec.Hard.SetRequest(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatMax, mcpu)
	}

	rqSpecRAMRequestAverageResult, _ := rqSpecRAMRequestAverageResultFuture.Await()
	for _, res := range rqSpecRAMRequestAverageResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add spec RAM request average", res.UID)
			continue
		}

		rq.Spec.Hard.SetRequest(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatAvg, res.Value)
	}

	rqSpecRAMRequestMaxResult, _ := rqSpecRAMRequestMaxResultFuture.Await()
	for _, res := range rqSpecRAMRequestMaxResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add spec RAM request max", res.UID)
			continue
		}

		rq.Spec.Hard.SetRequest(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatMax, res.Value)
	}

	rqSpecCPULimitAverageResult, _ := rqSpecCPULimitAverageResultFuture.Await()
	for _, res := range rqSpecCPULimitAverageResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add spec CPU limit average", res.UID)
			continue
		}

		mcpu := res.Value * 1000
		rq.Spec.Hard.SetLimit(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatAvg, mcpu)

	}

	rqSpecCPULimitMaxResult, _ := rqSpecCPULimitMaxResultFuture.Await()
	for _, res := range rqSpecCPULimitMaxResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add spec CPU limit max", res.UID)
			continue
		}

		mcpu := res.Value * 1000
		rq.Spec.Hard.SetLimit(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatMax, mcpu)
	}

	rqSpecRAMLimitAverageResult, _ := rqSpecRAMLimitAverageResultFuture.Await()
	for _, res := range rqSpecRAMLimitAverageResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add spec RAM limit average", res.UID)
			continue
		}

		rq.Spec.Hard.SetLimit(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatAvg, res.Value)
	}

	rqSpecRAMLimitMaxResult, _ := rqSpecRAMLimitMaxResultFuture.Await()
	for _, res := range rqSpecRAMLimitMaxResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add spec RAM limit max", res.UID)
			continue
		}

		rq.Spec.Hard.SetLimit(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatMax, res.Value)
	}

	rqStatusUsedCPURequestAverageResult, _ := rqStatusUsedCPURequestAverageResultFuture.Await()
	for _, res := range rqStatusUsedCPURequestAverageResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add status CPU request average", res.UID)
			continue
		}

		mcpu := res.Value * 1000
		rq.Status.Used.SetRequest(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatAvg, mcpu)
	}

	rqStatusUsedCPURequestMaxResult, _ := rqStatusUsedCPURequestMaxResultFuture.Await()
	for _, res := range rqStatusUsedCPURequestMaxResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add status CPU request max", res.UID)
			continue
		}

		mcpu := res.Value * 1000
		rq.Status.Used.SetRequest(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatMax, mcpu)
	}

	rqStatusUsedRAMRequestAverageResult, _ := rqStatusUsedRAMRequestAverageResultFuture.Await()
	for _, res := range rqStatusUsedRAMRequestAverageResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add status RAM request average", res.UID)
			continue
		}

		rq.Status.Used.SetRequest(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatAvg, res.Value)
	}

	rqStatusUsedRAMRequestMaxResult, _ := rqStatusUsedRAMRequestMaxResultFuture.Await()
	for _, res := range rqStatusUsedRAMRequestMaxResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add status RAM request max", res.UID)
			continue
		}

		rq.Status.Used.SetRequest(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatMax, res.Value)
	}

	rqStatusUsedCPULimitAverageResult, _ := rqStatusUsedCPULimitAverageResultFuture.Await()
	for _, res := range rqStatusUsedCPULimitAverageResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add status CPU limit average", res.UID)
			continue
		}

		mcpu := res.Value * 1000
		rq.Status.Used.SetLimit(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatAvg, mcpu)
	}

	rqStatusUsedCPULimitMaxResult, _ := rqStatusUsedCPULimitMaxResultFuture.Await()
	for _, res := range rqStatusUsedCPULimitMaxResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add status CPU limit max", res.UID)
			continue
		}

		mcpu := res.Value * 1000
		rq.Status.Used.SetLimit(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatMax, mcpu)
	}

	rqStatusUsedRAMLimitAverageResult, _ := rqStatusUsedRAMLimitAverageResultFuture.Await()
	for _, res := range rqStatusUsedRAMLimitAverageResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add status RAM limit average", res.UID)
			continue
		}

		rq.Status.Used.SetLimit(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatAvg, res.Value)
	}

	rqStatusUsedRAMLimitMaxResult, _ := rqStatusUsedRAMLimitMaxResultFuture.Await()
	for _, res := range rqStatusUsedRAMLimitMaxResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add status RAM limit max", res.UID)
			continue
		}

		rq.Status.Used.SetLimit(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatMax, res.Value)
	}

	for _, resourceQuota := range rqMap {
		err := kms.RegisterResourceQuota(resourceQuota)
		if err != nil {
			log.Warnf("Failed to register resource quota: %s", err.Error())
		}
	}

	return nil
}

func (km *KubeModel) computeServices(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	serviceInfoResultFuture := source.WithGroup(grp, metrics.QueryServiceInfo(start, end))
	serviceUptimeResultFuture := source.WithGroup(grp, metrics.QueryServiceUptime(start, end))
	serviceSelectorLabelsResultFuture := source.WithGroup(grp, metrics.QueryServiceSelectorLabels(start, end))

	serviceMap := make(map[string]*kubemodel.Service)

	// Initialize services from info
	serviceInfoResult, _ := serviceInfoResultFuture.Await()
	for _, res := range serviceInfoResult {
		serviceMap[res.UID] = &kubemodel.Service{
			UID:          res.UID,
			NamespaceUID: res.NamespaceUID,
			Name:         res.Service,
			Type:         kubemodel.ParseServiceType(res.ServiceType),
		}
	}

	serviceUptimeResult, _ := serviceUptimeResultFuture.Await()
	for _, res := range serviceUptimeResult {
		service, ok := serviceMap[res.UID]
		if !ok {
			log.Warnf("service with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		service.Start = s
		service.End = e
	}

	serviceSelectorLabelsResult, _ := serviceSelectorLabelsResultFuture.Await()
	for _, res := range serviceSelectorLabelsResult {
		service, ok := serviceMap[res.UID]
		if !ok {
			log.Warnf("service with UID '%s' has not been initialized to add selector labels", res.UID)
			continue
		}
		service.Selector = res.Labels
	}

	for _, service := range serviceMap {
		err := kms.RegisterService(service)
		if err != nil {
			log.Warnf("Failed to register service: %s", err.Error())
		}
	}

	return nil
}

func (km *KubeModel) computePersistentVolumes(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	pvInfoResultFuture := source.WithGroup(grp, metrics.QueryPVInfo(start, end))
	pvUptimeResultFuture := source.WithGroup(grp, metrics.QueryPVUptime(start, end))
	pvBytesResultFuture := source.WithGroup(grp, metrics.QueryPVBytes(start, end))

	pvMap := make(map[string]*kubemodel.PersistentVolume)

	pvInfoResult, _ := pvInfoResultFuture.Await()
	for _, res := range pvInfoResult {
		pvMap[res.UID] = &kubemodel.PersistentVolume{
			UID:             res.UID,
			Name:            res.PersistentVolume,
			StorageClass:    res.StorageClass,
			CSIVolumeHandle: res.CSIVolumeHandle,
		}
	}

	pvUptimeResult, _ := pvUptimeResultFuture.Await()
	for _, res := range pvUptimeResult {
		pv, ok := pvMap[res.UID]
		if !ok {
			log.Warnf("persistent volume with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		pv.Start = s
		pv.End = e
	}

	pvBytesResult, _ := pvBytesResultFuture.Await()
	for _, res := range pvBytesResult {
		pv, ok := pvMap[res.UID]
		if !ok {
			log.Warnf("persistent volume with UID '%s' has not been initialized to add bytes", res.UID)
			continue
		}

		pv.SizeBytes = res.Value

	}

	for _, pv := range pvMap {
		err := kms.RegisterPersistentVolume(pv)
		if err != nil {
			log.Warnf("Failed to register persistent volume: %s", err.Error())
		}
	}

	return nil
}

func (km *KubeModel) computePersistentVolumeClaims(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	pvcInfoResultFuture := source.WithGroup(grp, metrics.QueryPVCInfo(start, end))
	pvcUptimeResultFuture := source.WithGroup(grp, metrics.QueryPVCUptime(start, end))
	pvcBytesRequestedResultFuture := source.WithGroup(grp, metrics.QueryPVCBytesRequested(start, end))

	pvcMap := make(map[string]*kubemodel.PersistentVolumeClaim)

	pvcInfoResult, _ := pvcInfoResultFuture.Await()
	for _, res := range pvcInfoResult {
		pvcMap[res.UID] = &kubemodel.PersistentVolumeClaim{
			UID:                 res.UID,
			Name:                res.PersistentVolumeClaim,
			NamespaceUID:        res.NamespaceUID,
			PersistentVolumeUID: res.PVUID,
			StorageClass:        res.StorageClass,
		}
	}

	pvcUptimeResult, _ := pvcUptimeResultFuture.Await()
	for _, res := range pvcUptimeResult {
		pvc, ok := pvcMap[res.UID]
		if !ok {
			log.Warnf("persistent volume claim with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		pvc.Start = s
		pvc.End = e
	}

	pvcBytesRequestedResult, _ := pvcBytesRequestedResultFuture.Await()
	for _, res := range pvcBytesRequestedResult {
		pvc, ok := pvcMap[res.UID]
		if !ok {
			log.Warnf("persistent volume claim with UID '%s' has not been initialized to add requested bytes", res.UID)
			continue
		}
		if len(res.Data) > 0 {
			pvc.RequestedBytes = res.Data[0].Value
		}
	}

	for _, pvc := range pvcMap {
		err := kms.RegisterPVC(pvc)
		if err != nil {
			log.Warnf("Failed to register persistent volume claim: %s", err.Error())
		}
	}

	return nil
}

func (km *KubeModel) computeDCGMDevices(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	dcgmInfoFuture := source.WithGroup(grp, metrics.QueryDCGMDeviceInfo(start, end))
	dcgmUptimeFuture := source.WithGroup(grp, metrics.QueryDCGMDeviceUptime(start, end))
	dcgmUsageAvgFuture := source.WithGroup(grp, metrics.QueryDCGMContainerUsageAvg(start, end))
	dcgmUsageMaxFuture := source.WithGroup(grp, metrics.QueryDCGMContainerUsageMax(start, end))

	deviceMap := make(map[string]*kubemodel.DCGMDevice)

	dcgmInfoResult, _ := dcgmInfoFuture.Await()
	for _, res := range dcgmInfoResult {
		if res.UUID == "" {
			continue
		}
		if _, ok := deviceMap[res.UUID]; ok {
			continue
		}
		deviceMap[res.UUID] = &kubemodel.DCGMDevice{
			UUID:      res.UUID,
			Device:    res.Device,
			ModelName: res.ModelName,
			PodUsage:  make(map[string]kubemodel.DCGMPod),
		}
	}

	dcgmUptimeResult, _ := dcgmUptimeFuture.Await()
	for _, res := range dcgmUptimeResult {
		d, ok := deviceMap[res.UUID]
		if !ok {
			log.Warnf("DCGM uptime result for unknown device UUID '%s'", res.UUID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		d.Start = s
		d.End = e
	}

	dcgmUsageAvgResult, _ := dcgmUsageAvgFuture.Await()
	for _, res := range dcgmUsageAvgResult {
		device, ok := deviceMap[res.UUID]
		if !ok || res.PodUID == "" || res.Container == "" {
			continue
		}
		pod, ok := device.PodUsage[res.PodUID]
		if !ok {
			pod = kubemodel.DCGMPod{ContainerUsage: make(map[string]kubemodel.DCGMContainer)}
		}
		c := pod.ContainerUsage[res.Container]
		c.UsageAvg = res.Value
		pod.ContainerUsage[res.Container] = c
		device.PodUsage[res.PodUID] = pod
	}

	dcgmUsageMaxResult, _ := dcgmUsageMaxFuture.Await()
	for _, res := range dcgmUsageMaxResult {
		device, ok := deviceMap[res.UUID]
		if !ok || res.PodUID == "" || res.Container == "" {
			continue
		}
		pod, ok := device.PodUsage[res.PodUID]
		if !ok {
			pod = kubemodel.DCGMPod{ContainerUsage: make(map[string]kubemodel.DCGMContainer)}
		}
		c := pod.ContainerUsage[res.Container]
		c.UsageMax = res.Value
		pod.ContainerUsage[res.Container] = c
		device.PodUsage[res.PodUID] = pod
	}

	for _, device := range deviceMap {
		if err := kms.RegisterDCGMDevice(device); err != nil {
			log.Warnf("Failed to register DCGM device: %s", err.Error())
		}
	}

	return nil
}
