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

	// 2.6 Compute StatefulSets
	err = km.computeStatefulSets(kms, start, end)
	if err != nil {
		kms.Error(err)
	}

	// 2.7 Compute DaemonSets
	err = km.computeDaemonSets(kms, start, end)
	if err != nil {
		kms.Error(err)
	}

	// 2.8 Compute Jobs
	err = km.computeJobs(kms, start, end)
	if err != nil {
		kms.Error(err)
	}

	// 2.9 Compute CronJobs
	err = km.computeCronJobs(kms, start, end)
	if err != nil {
		kms.Error(err)
	}

	// 2.10 Compute ReplicaSets
	err = km.computeReplicaSets(kms, start, end)
	if err != nil {
		kms.Error(err)
	}

	// 2.11 Compute Containers
	err = km.computeContainers(kms, start, end)
	if err != nil {
		kms.Error(err)
	}

	// 2.12 Compute ResourceQuotas
	err = km.computeResourceQuotas(kms, start, end)
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
			UID:         res.UID,
			Provider:    kubemodel.ParseProvider(res.Provider),
			Account:     res.AccountID,
			Name:        res.Cluster,
			Provisioner: res.Provisioner,
			Region:      res.Region,
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
			NamespaceUID: res.NameSpaceUID,
		}
	}

	statefulSetUptimeResult, _ := statefulSetUptimeResultFuture.Await()
	for _, res := range statefulSetUptimeResult {
		statefulSet, ok := statefulSetMap[res.UID]
		if !ok {
			log.Warnf("statefulset with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}

		statefulSet.Start = res.First
		statefulSet.End = res.Last
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
			NamespaceUID: res.NameSpaceUID,
		}
	}

	daemonSetUptimeResult, _ := daemonSetUptimeResultFuture.Await()
	for _, res := range daemonSetUptimeResult {
		daemonSet, ok := daemonSetMap[res.UID]
		if !ok {
			log.Warnf("daemonset with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}

		daemonSet.Start = res.First
		daemonSet.End = res.Last
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
			NamespaceUID: res.NameSpaceUID,
		}
	}

	jobUptimeResult, _ := jobUptimeResultFuture.Await()
	for _, res := range jobUptimeResult {
		job, ok := jobMap[res.UID]
		if !ok {
			log.Warnf("job with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}

		job.Start = res.First
		job.End = res.Last
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
			NamespaceUID: res.NameSpaceUID,
		}
	}

	cronJobUptimeResult, _ := cronJobUptimeResultFuture.Await()
	for _, res := range cronJobUptimeResult {
		cronJob, ok := cronJobMap[res.UID]
		if !ok {
			log.Warnf("cronjob with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}

		cronJob.Start = res.First
		cronJob.End = res.Last
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
	replicaSetLabelsResultFuture := source.WithGroup(grp, metrics.QueryReplicaSetLabels(start, end))
	replicaSetAnnotationsResultFuture := source.WithGroup(grp, metrics.QueryReplicaSetAnnotations(start, end))

	replicaSetMap := make(map[string]*kubemodel.ReplicaSet)

	replicaSetInfoResult, _ := replicaSetInfoResultFuture.Await()
	for _, res := range replicaSetInfoResult {
		replicaSetMap[res.UID] = &kubemodel.ReplicaSet{
			UID:          res.UID,
			Name:         res.ReplicaSet,
			NamespaceUID: res.NameSpaceUID,
		}
	}

	replicaSetUptimeResult, _ := replicaSetUptimeResultFuture.Await()
	for _, res := range replicaSetUptimeResult {
		replicaSet, ok := replicaSetMap[res.UID]
		if !ok {
			log.Warnf("replicaset with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}

		replicaSet.Start = res.First
		replicaSet.End = res.Last
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

	cpuAllocatedFuture := source.WithGroup(grp, metrics.QueryCPUCoresAllocated(start, end))
	cpuUsageAvgFuture := source.WithGroup(grp, metrics.QueryCPUUsageAvg(start, end))
	cpuUsageMaxFuture := source.WithGroup(grp, metrics.QueryCPUUsageMax(start, end))
	cpuRequestsFuture := source.WithGroup(grp, metrics.QueryCPURequests(start, end))
	cpuLimitsFuture := source.WithGroup(grp, metrics.QueryCPULimits(start, end))

	ramAllocatedFuture := source.WithGroup(grp, metrics.QueryRAMBytesAllocated(start, end))
	ramUsageAvgFuture := source.WithGroup(grp, metrics.QueryRAMUsageAvg(start, end))
	ramUsageMaxFuture := source.WithGroup(grp, metrics.QueryRAMUsageMax(start, end))
	ramRequestsFuture := source.WithGroup(grp, metrics.QueryRAMRequests(start, end))
	ramLimitsFuture := source.WithGroup(grp, metrics.QueryRAMLimits(start, end))

	gpuAllocatedFuture := source.WithGroup(grp, metrics.QueryGPUsAllocated(start, end))
	gpuUsageAvgFuture := source.WithGroup(grp, metrics.QueryGPUsUsageAvg(start, end))
	gpuUsageMaxFuture := source.WithGroup(grp, metrics.QueryGPUsUsageMax(start, end))
	gpuRequestedFuture := source.WithGroup(grp, metrics.QueryGPUsRequested(start, end))

	type containerKey struct {
		podUID string
		name   string
	}

	containerMap := make(map[containerKey]*kubemodel.Container)

	containerUptimeResult, _ := containerUptimeFuture.Await()
	for _, res := range containerUptimeResult {
		key := containerKey{podUID: res.UID, name: res.Container}
		containerMap[key] = &kubemodel.Container{
			PodUID: res.UID,
			Name:   res.Container,
			Start:  res.First,
			End:    res.Last,
		}
	}

	cpuAllocatedResult, _ := cpuAllocatedFuture.Await()
	for _, res := range cpuAllocatedResult {
		key := containerKey{podUID: res.UID, name: res.Container}
		container, ok := containerMap[key]
		if !ok {
			log.Warnf("container %s/%s has not been initialized to add CPU allocated", res.UID, res.Container)
			continue
		}
		if len(res.Data) > 0 {
			container.CPUAllocated = kubemodel.Measurement(res.Data[0].Value)
		}
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
			container.CPUUsageAvg = kubemodel.Measurement(res.Data[0].Value)
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
			container.CPUUsageMax = kubemodel.Measurement(res.Data[0].Value)
		}
	}

	cpuRequestsResult, _ := cpuRequestsFuture.Await()
	for _, res := range cpuRequestsResult {
		key := containerKey{podUID: res.UID, name: res.Container}
		container, ok := containerMap[key]
		if !ok {
			log.Warnf("container %s/%s has not been initialized to add CPU requests", res.UID, res.Container)
			continue
		}
		if len(res.Data) > 0 {
			container.CPURequest = kubemodel.Measurement(res.Data[0].Value)
		}
	}

	cpuLimitsResult, _ := cpuLimitsFuture.Await()
	for _, res := range cpuLimitsResult {
		key := containerKey{podUID: res.UID, name: res.Container}
		container, ok := containerMap[key]
		if !ok {
			log.Warnf("container %s/%s has not been initialized to add CPU limits", res.UID, res.Container)
			continue
		}
		if len(res.Data) > 0 {
			container.CPULimit = kubemodel.Measurement(res.Data[0].Value)
		}
	}

	ramAllocatedResult, _ := ramAllocatedFuture.Await()
	for _, res := range ramAllocatedResult {
		key := containerKey{podUID: res.UID, name: res.Container}
		container, ok := containerMap[key]
		if !ok {
			log.Warnf("container %s/%s has not been initialized to add RAM allocated", res.UID, res.Container)
			continue
		}
		if len(res.Data) > 0 {
			container.RAMAllocated = kubemodel.Measurement(res.Data[0].Value)
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
			container.RAMUsageAvg = kubemodel.Measurement(res.Data[0].Value)
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
			container.RAMUsageMax = kubemodel.Measurement(res.Data[0].Value)
		}
	}

	ramRequestsResult, _ := ramRequestsFuture.Await()
	for _, res := range ramRequestsResult {
		key := containerKey{podUID: res.UID, name: res.Container}
		container, ok := containerMap[key]
		if !ok {
			log.Warnf("container %s/%s has not been initialized to add RAM requests", res.UID, res.Container)
			continue
		}
		if len(res.Data) > 0 {
			container.RAMRequest = kubemodel.Measurement(res.Data[0].Value)
		}
	}

	ramLimitsResult, _ := ramLimitsFuture.Await()
	for _, res := range ramLimitsResult {
		key := containerKey{podUID: res.UID, name: res.Container}
		container, ok := containerMap[key]
		if !ok {
			log.Warnf("container %s/%s has not been initialized to add RAM limits", res.UID, res.Container)
			continue
		}
		if len(res.Data) > 0 {
			container.RAMLimit = kubemodel.Measurement(res.Data[0].Value)
		}
	}

	gpuAllocatedResult, _ := gpuAllocatedFuture.Await()
	for _, res := range gpuAllocatedResult {
		key := containerKey{podUID: res.UID, name: res.Container}
		container, ok := containerMap[key]
		if !ok {
			log.Warnf("container %s/%s has not been initialized to add GPU allocated", res.UID, res.Container)
			continue
		}
		if len(res.Data) > 0 {
			container.GPUAllocated = kubemodel.Measurement(res.Data[0].Value)
		}
	}

	gpuUsageAvgResult, _ := gpuUsageAvgFuture.Await()
	for _, res := range gpuUsageAvgResult {
		key := containerKey{podUID: res.UID, name: res.Container}
		container, ok := containerMap[key]
		if !ok {
			log.Warnf("container %s/%s has not been initialized to add GPU usage avg", res.UID, res.Container)
			continue
		}
		if len(res.Data) > 0 {
			container.GPUUsageAvg = kubemodel.Measurement(res.Data[0].Value)
		}
	}

	gpuUsageMaxResult, _ := gpuUsageMaxFuture.Await()
	for _, res := range gpuUsageMaxResult {
		key := containerKey{podUID: res.UID, name: res.Container}
		container, ok := containerMap[key]
		if !ok {
			log.Warnf("container %s/%s has not been initialized to add GPU usage max", res.UID, res.Container)
			continue
		}
		if len(res.Data) > 0 {
			container.GPUUsageMax = kubemodel.Measurement(res.Data[0].Value)
		}
	}

	gpuRequestedResult, _ := gpuRequestedFuture.Await()
	for _, res := range gpuRequestedResult {
		key := containerKey{podUID: res.UID, name: res.Container}
		container, ok := containerMap[key]
		if !ok {
			log.Warnf("container %s/%s has not been initialized to add GPU requested", res.UID, res.Container)
			continue
		}
		if len(res.Data) > 0 {
			container.GPURequest = kubemodel.Measurement(res.Data[0].Value)
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
