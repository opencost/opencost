package prom

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	promsource "github.com/opencost/opencost/modules/prometheus-source/pkg/prom"
)

// QueryNodeLabels — kube_node_labels is empty in our cluster (OTel-scraped KSM does not
// emit label_* fields). Return empty result; node labels are unavailable.
func (pds *PrometheusMetricsQuerier) QueryNodeLabels(start, end time.Time) *source.Future[source.NodeLabelsResult] {
	log.Warnf("QueryNodeLabels: kube_node_labels has no label_* fields in OTel-scraped KSM; returning empty result")
	return source.NewFutureFrom([]*source.NodeLabelsResult{})
}

// QueryNamespaceLabels — kube_namespace_labels is empty in our cluster (OTel-scraped KSM).
func (pds *PrometheusMetricsQuerier) QueryNamespaceLabels(start, end time.Time) *source.Future[source.NamespaceLabelsResult] {
	log.Warnf("QueryNamespaceLabels: kube_namespace_labels has no data in OTel-scraped KSM; returning empty result")
	return source.NewFutureFrom([]*source.NamespaceLabelsResult{})
}

// QueryNamespaceAnnotations — kube_namespace_annotations is empty in our cluster.
func (pds *PrometheusMetricsQuerier) QueryNamespaceAnnotations(start, end time.Time) *source.Future[source.NamespaceAnnotationsResult] {
	log.Warnf("QueryNamespaceAnnotations: kube_namespace_annotations has no data in OTel-scraped KSM; returning empty result")
	return source.NewFutureFrom([]*source.NamespaceAnnotationsResult{})
}

// QueryPodLabels queries for pod labels from KSM kube_pod_labels.
// NOTE: In OTel-scraped KSM, kube_pod_labels exists but has no label_* fields.
// Labels present: k8s_cluster_name, k8s_namespace_name, k8s_pod_name, k8s_pod_uid.
// The metric is queried anyway so the decoder can extract whatever is available.
func (pds *PrometheusMetricsQuerier) QueryPodLabels(start, end time.Time) *source.Future[source.PodLabelsResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	// kube_pod_labels exists in our cluster but has no label_* fields.
	// We still need to emit k8s_namespace_name + k8s_pod_name so the decoder can build the key.
	q := fmt.Sprintf(`avg(avg_over_time(kube_pod_labels{%s}[%s])) by (k8s_namespace_name, k8s_pod_name, %s)`, pds.clusterFilterSuffix(), d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryPodLabels", end.Unix(), q)
	return source.NewFuture(source.DecodePodLabelsResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryPodAnnotations — kube_pod_annotations not available in OTel-scraped KSM.
func (pds *PrometheusMetricsQuerier) QueryPodAnnotations(start, end time.Time) *source.Future[source.PodAnnotationsResult] {
	log.Warnf("QueryPodAnnotations: kube_pod_annotations not available in OTel-scraped KSM; returning empty result")
	return source.NewFutureFrom([]*source.PodAnnotationsResult{})
}

// QueryServiceLabels — kube_service_labels not available in OTel-scraped KSM.
func (pds *PrometheusMetricsQuerier) QueryServiceLabels(start, end time.Time) *source.Future[source.ServiceLabelsResult] {
	log.Warnf("QueryServiceLabels: kube_service_labels not available in OTel-scraped KSM; returning empty result")
	return source.NewFutureFrom([]*source.ServiceLabelsResult{})
}

// QueryDeploymentLabels — kube_deployment_labels not available in OTel-scraped KSM.
func (pds *PrometheusMetricsQuerier) QueryDeploymentLabels(start, end time.Time) *source.Future[source.LabelsResult] {
	log.Warnf("QueryDeploymentLabels: kube_deployment_labels not available in OTel-scraped KSM; returning empty result")
	return source.NewFutureFrom([]*source.LabelsResult{})
}

// QueryStatefulSetLabels — kube_statefulset_labels not available in OTel-scraped KSM.
func (pds *PrometheusMetricsQuerier) QueryStatefulSetLabels(start, end time.Time) *source.Future[source.LabelsResult] {
	log.Warnf("QueryStatefulSetLabels: kube_statefulset_labels not available in OTel-scraped KSM; returning empty result")
	return source.NewFutureFrom([]*source.LabelsResult{})
}

// QueryDaemonSetLabels — kube_daemonset_labels not available in OTel-scraped KSM.
func (pds *PrometheusMetricsQuerier) QueryDaemonSetLabels(start, end time.Time) *source.Future[source.LabelsResult] {
	log.Warnf("QueryDaemonSetLabels: kube_daemonset_labels not available in OTel-scraped KSM; returning empty result")
	return source.NewFutureFrom([]*source.LabelsResult{})
}

// QueryJobLabels — kube_job_labels not available in OTel-scraped KSM.
func (pds *PrometheusMetricsQuerier) QueryJobLabels(start, end time.Time) *source.Future[source.LabelsResult] {
	log.Warnf("QueryJobLabels: kube_job_labels not available in OTel-scraped KSM; returning empty result")
	return source.NewFutureFrom([]*source.LabelsResult{})
}

// QueryCronJobLabels — kube_cronjob_labels not available in OTel-scraped KSM.
func (pds *PrometheusMetricsQuerier) QueryCronJobLabels(start, end time.Time) *source.Future[source.LabelsResult] {
	log.Warnf("QueryCronJobLabels: kube_cronjob_labels not available in OTel-scraped KSM; returning empty result")
	return source.NewFutureFrom([]*source.LabelsResult{})
}

// QueryClusterInfo — cluster_info not emitted by OTel receivers.
func (pds *PrometheusMetricsQuerier) QueryClusterInfo(start, end time.Time) *source.Future[source.ClusterInfoResult] {
	log.Warnf("QueryClusterInfo: cluster_info not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.ClusterInfoResult{})
}

// QueryClusterKubeModelVersion — not emitted by OTel receivers.
func (pds *PrometheusMetricsQuerier) QueryClusterKubeModelVersion(start, end time.Time) *source.Future[source.ClusterKubeModelVersionResult] {
	log.Warnf("QueryClusterKubeModelVersion: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.ClusterKubeModelVersionResult{})
}

// QueryPodsWithReplicaSetOwner queries for pods with ReplicaSet owners via KSM kube_pod_owner.
// Labels (OTel-native): k8s_cluster_name, k8s_namespace_name, k8s_pod_name, k8s_pod_uid,
// owner_is_controller, owner_kind, owner_name.
func (pds *PrometheusMetricsQuerier) QueryPodsWithReplicaSetOwner(start, end time.Time) *source.Future[source.PodsWithReplicaSetOwnerResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg(avg_over_time(kube_pod_owner{owner_kind="ReplicaSet"%s}[%s])) by (k8s_namespace_name, k8s_pod_name, k8s_pod_uid, owner_name, owner_kind, owner_is_controller, %s)`, pds.clusterFilterSuffix(), d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryPodsWithReplicaSetOwner", end.Unix(), q)
	return source.NewFuture(source.DecodePodsWithReplicaSetOwnerResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryReplicaSetsWithoutOwners queries for ReplicaSets not referenced by any owner.
// Uses kube_replicaset_created minus kube_replicaset_owner (both OTel-labeled with k8s_replicaset_name).
func (pds *PrometheusMetricsQuerier) QueryReplicaSetsWithoutOwners(start, end time.Time) *source.Future[source.ReplicaSetsWithoutOwnersResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(
		`(avg(avg_over_time(kube_replicaset_created{%s}[%s])) by (k8s_replicaset_name, k8s_namespace_name, %s) unless on(k8s_replicaset_name, k8s_namespace_name, %s) avg(avg_over_time(kube_replicaset_owner{%s}[%s])) by (k8s_replicaset_name, k8s_namespace_name, %s)) * on(k8s_replicaset_name, k8s_namespace_name, %s) group_left() avg(avg_over_time(kube_replicaset_created{%s}[%s])) by (k8s_replicaset_name, k8s_namespace_name, %s)`,
		pds.clusterFilterSuffix(), d, cfg.ClusterLabel, cfg.ClusterLabel,
		pds.clusterFilterSuffix(), d, cfg.ClusterLabel, cfg.ClusterLabel,
		pds.clusterFilterSuffix(), d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryReplicaSetsWithoutOwners", end.Unix(), q)
	return source.NewFuture(source.DecodeReplicaSetsWithoutOwnersResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryReplicaSetsWithRollout queries for ReplicaSets owned by Argo Rollouts via KSM kube_replicaset_owner.
// Labels (OTel-native): k8s_cluster_name, k8s_namespace_name, k8s_replicaset_name,
// owner_is_controller, owner_kind, owner_name.
func (pds *PrometheusMetricsQuerier) QueryReplicaSetsWithRollout(start, end time.Time) *source.Future[source.ReplicaSetsWithRolloutResult] {
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg_over_time(kube_replicaset_owner{owner_kind="Rollout"%s}[%s])`, pds.clusterFilterSuffix(), d)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryReplicaSetsWithRollout", end.Unix(), q)
	return source.NewFuture(source.DecodeReplicaSetsWithRolloutResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryCronJobAnnotations(start, end time.Time) *source.Future[source.AnnotationsResult] {
	log.Warnf("QueryCronJobAnnotations: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.AnnotationsResult{})
}

func (pds *PrometheusMetricsQuerier) QueryCronJobInfo(start, end time.Time) *source.Future[source.CronJobInfoResult] {
	log.Warnf("QueryCronJobInfo: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.CronJobInfoResult{})
}

func (pds *PrometheusMetricsQuerier) QueryCronJobUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	log.Warnf("QueryCronJobUptime: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.UptimeResult{})
}

func (pds *PrometheusMetricsQuerier) QueryDCGMContainerUsageAvg(start, end time.Time) *source.Future[source.DCGMDeviceContainerUsageResult] {
	log.Warnf("QueryDCGMContainerUsageAvg: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.DCGMDeviceContainerUsageResult{})
}

func (pds *PrometheusMetricsQuerier) QueryDCGMContainerUsageMax(start, end time.Time) *source.Future[source.DCGMDeviceContainerUsageResult] {
	log.Warnf("QueryDCGMContainerUsageMax: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.DCGMDeviceContainerUsageResult{})
}

func (pds *PrometheusMetricsQuerier) QueryDCGMDeviceInfo(start, end time.Time) *source.Future[source.DCGMDeviceInfoResult] {
	log.Warnf("QueryDCGMDeviceInfo: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.DCGMDeviceInfoResult{})
}

func (pds *PrometheusMetricsQuerier) QueryDCGMDeviceUptime(start, end time.Time) *source.Future[source.DCGMDeviceUptimeResult] {
	log.Warnf("QueryDCGMDeviceUptime: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.DCGMDeviceUptimeResult{})
}

func (pds *PrometheusMetricsQuerier) QueryDaemonSetAnnotations(start, end time.Time) *source.Future[source.AnnotationsResult] {
	log.Warnf("QueryDaemonSetAnnotations: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.AnnotationsResult{})
}

func (pds *PrometheusMetricsQuerier) QueryDaemonSetArguments(start, end time.Time) *source.Future[source.DaemonSetArgumentResult] {
	log.Warnf("QueryDaemonSetArguments: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.DaemonSetArgumentResult{})
}

func (pds *PrometheusMetricsQuerier) QueryDaemonSetInfo(start, end time.Time) *source.Future[source.DaemonSetInfoResult] {
	log.Warnf("QueryDaemonSetInfo: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.DaemonSetInfoResult{})
}

func (pds *PrometheusMetricsQuerier) QueryDaemonSetUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	log.Warnf("QueryDaemonSetUptime: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.UptimeResult{})
}

func (pds *PrometheusMetricsQuerier) QueryDeploymentAnnotations(start, end time.Time) *source.Future[source.AnnotationsResult] {
	log.Warnf("QueryDeploymentAnnotations: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.AnnotationsResult{})
}

func (pds *PrometheusMetricsQuerier) QueryDeploymentInfo(start, end time.Time) *source.Future[source.DeploymentInfoResult] {
	log.Warnf("QueryDeploymentInfo: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.DeploymentInfoResult{})
}

func (pds *PrometheusMetricsQuerier) QueryDeploymentMatchLabels(start, end time.Time) *source.Future[source.DeploymentLabelsResult] {
	log.Warnf("QueryDeploymentMatchLabels: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.DeploymentLabelsResult{})
}

func (pds *PrometheusMetricsQuerier) QueryDeploymentUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	log.Warnf("QueryDeploymentUptime: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.UptimeResult{})
}

func (pds *PrometheusMetricsQuerier) QueryInferenceCacheConfig(t time.Time) *source.Future[source.InferenceCacheConfigResult] {
	log.Warnf("QueryInferenceCacheConfig: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.InferenceCacheConfigResult{})
}

func (pds *PrometheusMetricsQuerier) QueryInferenceCachedTokens(start, end time.Time) *source.Future[source.InferenceTokensResult] {
	log.Warnf("QueryInferenceCachedTokens: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.InferenceTokensResult{})
}

func (pds *PrometheusMetricsQuerier) QueryInferenceGenerationTokens(start, end time.Time) *source.Future[source.InferenceTokensResult] {
	log.Warnf("QueryInferenceGenerationTokens: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.InferenceTokensResult{})
}

func (pds *PrometheusMetricsQuerier) QueryInferenceInputProcessingTime(start, end time.Time) *source.Future[source.InferenceProcessingTimeResult] {
	log.Warnf("QueryInferenceInputProcessingTime: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.InferenceProcessingTimeResult{})
}

func (pds *PrometheusMetricsQuerier) QueryInferenceOutputProcessingTime(start, end time.Time) *source.Future[source.InferenceProcessingTimeResult] {
	log.Warnf("QueryInferenceOutputProcessingTime: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.InferenceProcessingTimeResult{})
}

func (pds *PrometheusMetricsQuerier) QueryInferencePromptTokens(start, end time.Time) *source.Future[source.InferenceTokensResult] {
	log.Warnf("QueryInferencePromptTokens: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.InferenceTokensResult{})
}

func (pds *PrometheusMetricsQuerier) QueryJobAnnotations(start, end time.Time) *source.Future[source.AnnotationsResult] {
	log.Warnf("QueryJobAnnotations: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.AnnotationsResult{})
}

func (pds *PrometheusMetricsQuerier) QueryJobInfo(start, end time.Time) *source.Future[source.JobInfoResult] {
	log.Warnf("QueryJobInfo: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.JobInfoResult{})
}

func (pds *PrometheusMetricsQuerier) QueryJobUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	log.Warnf("QueryJobUptime: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.UptimeResult{})
}

func (pds *PrometheusMetricsQuerier) QueryKMLocalStorageBytes(start, end time.Time) *source.Future[source.UIDValueResult] {
	log.Warnf("QueryKMLocalStorageBytes: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.UIDValueResult{})
}

func (pds *PrometheusMetricsQuerier) QueryKMLocalStorageUsedAvg(start, end time.Time) *source.Future[source.NodeUIDValueResult] {
	log.Warnf("QueryKMLocalStorageUsedAvg: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.NodeUIDValueResult{})
}

func (pds *PrometheusMetricsQuerier) QueryKMLocalStorageUsedMax(start, end time.Time) *source.Future[source.NodeUIDValueResult] {
	log.Warnf("QueryKMLocalStorageUsedMax: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.NodeUIDValueResult{})
}

func (pds *PrometheusMetricsQuerier) QueryKMPVCInfo(start, end time.Time) *source.Future[source.PVCInfoResult] {
	log.Warnf("QueryKMPVCInfo: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.PVCInfoResult{})
}

func (pds *PrometheusMetricsQuerier) QueryKMPVInfo(start, end time.Time) *source.Future[source.PVInfoResult] {
	log.Warnf("QueryKMPVInfo: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.PVInfoResult{})
}

func (pds *PrometheusMetricsQuerier) QueryNamespaceInfo(start, end time.Time) *source.Future[source.NamespaceInfoResult] {
	log.Warnf("QueryNamespaceInfo: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.NamespaceInfoResult{})
}

func (pds *PrometheusMetricsQuerier) QueryNodeInfo(start, end time.Time) *source.Future[source.NodeInfoResult] {
	log.Warnf("QueryNodeInfo: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.NodeInfoResult{})
}

func (pds *PrometheusMetricsQuerier) QueryNodeResourceCapacities(start, end time.Time) *source.Future[source.ResourceResult] {
	log.Warnf("QueryNodeResourceCapacities: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.ResourceResult{})
}

func (pds *PrometheusMetricsQuerier) QueryNodeResourcesAllocatable(start, end time.Time) *source.Future[source.ResourceResult] {
	log.Warnf("QueryNodeResourcesAllocatable: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.ResourceResult{})
}

func (pds *PrometheusMetricsQuerier) QueryNodeUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	log.Warnf("QueryNodeUptime: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.UptimeResult{})
}

func (pds *PrometheusMetricsQuerier) QueryPVCBytesUsedAverage(start, end time.Time) *source.Future[source.PVCUIDValueResult] {
	log.Warnf("QueryPVCBytesUsedAverage: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.PVCUIDValueResult{})
}

func (pds *PrometheusMetricsQuerier) QueryPVCBytesUsedMax(start, end time.Time) *source.Future[source.PVCUIDValueResult] {
	log.Warnf("QueryPVCBytesUsedMax: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.PVCUIDValueResult{})
}

func (pds *PrometheusMetricsQuerier) QueryPVCUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	log.Warnf("QueryPVCUptime: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.UptimeResult{})
}

func (pds *PrometheusMetricsQuerier) QueryPVUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	log.Warnf("QueryPVUptime: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.UptimeResult{})
}

func (pds *PrometheusMetricsQuerier) QueryPodInfo(start, end time.Time) *source.Future[source.PodInfoResult] {
	log.Warnf("QueryPodInfo: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.PodInfoResult{})
}

func (pds *PrometheusMetricsQuerier) QueryPodNetworkEgressBytes(start, end time.Time) *source.Future[source.PodNetworkBytesResult] {
	log.Warnf("QueryPodNetworkEgressBytes: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.PodNetworkBytesResult{})
}

func (pds *PrometheusMetricsQuerier) QueryPodNetworkIngressBytes(start, end time.Time) *source.Future[source.PodNetworkBytesResult] {
	log.Warnf("QueryPodNetworkIngressBytes: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.PodNetworkBytesResult{})
}

func (pds *PrometheusMetricsQuerier) QueryPodOwners(start, end time.Time) *source.Future[source.OwnerResult] {
	log.Warnf("QueryPodOwners: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.OwnerResult{})
}

func (pds *PrometheusMetricsQuerier) QueryPodPVCVolumes(start, end time.Time) *source.Future[source.PodPVCVolumeResult] {
	log.Warnf("QueryPodPVCVolumes: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.PodPVCVolumeResult{})
}

func (pds *PrometheusMetricsQuerier) QueryPodUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	log.Warnf("QueryPodUptime: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.UptimeResult{})
}

func (pds *PrometheusMetricsQuerier) QueryPodsWithDaemonSetOwner(start, end time.Time) *source.Future[source.PodsWithDaemonSetOwnerResult] {
	// kube_pod_owner{owner_kind="DaemonSet"} is available from OTel-scraped KSM.
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg(avg_over_time(kube_pod_owner{owner_kind="DaemonSet"%s}[%s])) by (k8s_namespace_name, k8s_pod_name, k8s_pod_uid, owner_name, owner_kind, owner_is_controller, %s)`, pds.clusterFilterSuffix(), d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryPodsWithDaemonSetOwner", end.Unix(), q)
	return source.NewFuture(source.DecodePodsWithDaemonSetOwnerResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryPodsWithJobOwner(start, end time.Time) *source.Future[source.PodsWithJobOwnerResult] {
	// kube_pod_owner{owner_kind="Job"} is available from OTel-scraped KSM.
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg(avg_over_time(kube_pod_owner{owner_kind="Job"%s}[%s])) by (k8s_namespace_name, k8s_pod_name, k8s_pod_uid, owner_name, owner_kind, owner_is_controller, %s)`, pds.clusterFilterSuffix(), d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryPodsWithJobOwner", end.Unix(), q)
	return source.NewFuture(source.DecodePodsWithJobOwnerResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryReplicaSetAnnotations(start, end time.Time) *source.Future[source.AnnotationsResult] {
	log.Warnf("QueryReplicaSetAnnotations: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.AnnotationsResult{})
}

func (pds *PrometheusMetricsQuerier) QueryReplicaSetInfo(start, end time.Time) *source.Future[source.ReplicaSetInfoResult] {
	log.Warnf("QueryReplicaSetInfo: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.ReplicaSetInfoResult{})
}

func (pds *PrometheusMetricsQuerier) QueryReplicaSetLabels(start, end time.Time) *source.Future[source.LabelsResult] {
	log.Warnf("QueryReplicaSetLabels: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.LabelsResult{})
}

func (pds *PrometheusMetricsQuerier) QueryReplicaSetOwners(start, end time.Time) *source.Future[source.OwnerResult] {
	log.Warnf("QueryReplicaSetOwners: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.OwnerResult{})
}

func (pds *PrometheusMetricsQuerier) QueryReplicaSetUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	log.Warnf("QueryReplicaSetUptime: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.UptimeResult{})
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaInfo(start, end time.Time) *source.Future[source.ResourceQuotaInfoResult] {
	log.Warnf("QueryResourceQuotaInfo: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.ResourceQuotaInfoResult{})
}

func (pds *PrometheusMetricsQuerier) QueryServiceInfo(start, end time.Time) *source.Future[source.ServiceInfoResult] {
	log.Warnf("QueryServiceInfo: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.ServiceInfoResult{})
}

func (pds *PrometheusMetricsQuerier) QueryServiceSelectorLabels(start, end time.Time) *source.Future[source.ServiceLabelsResult] {
	log.Warnf("QueryServiceSelectorLabels: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.ServiceLabelsResult{})
}

func (pds *PrometheusMetricsQuerier) QueryServiceUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	log.Warnf("QueryServiceUptime: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.UptimeResult{})
}

func (pds *PrometheusMetricsQuerier) QueryStatefulSetAnnotations(start, end time.Time) *source.Future[source.AnnotationsResult] {
	log.Warnf("QueryStatefulSetAnnotations: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.AnnotationsResult{})
}

func (pds *PrometheusMetricsQuerier) QueryStatefulSetInfo(start, end time.Time) *source.Future[source.StatefulSetInfoResult] {
	log.Warnf("QueryStatefulSetInfo: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.StatefulSetInfoResult{})
}

func (pds *PrometheusMetricsQuerier) QueryStatefulSetMatchLabels(start, end time.Time) *source.Future[source.StatefulSetLabelsResult] {
	log.Warnf("QueryStatefulSetMatchLabels: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.StatefulSetLabelsResult{})
}

func (pds *PrometheusMetricsQuerier) QueryStatefulSetUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	log.Warnf("QueryStatefulSetUptime: not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.UptimeResult{})
}
