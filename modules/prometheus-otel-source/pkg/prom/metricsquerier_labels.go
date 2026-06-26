package prom

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	promsource "github.com/opencost/opencost/modules/prometheus-source/pkg/prom"
)

// QueryNodeLabels queries for node labels from kube-state-metrics
func (pds *PrometheusMetricsQuerier) QueryNodeLabels(start, end time.Time) *source.Future[source.NodeLabelsResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg_over_time(kube_node_labels{%s}[%s])`, cfg.ClusterFilter, d)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNodeLabels", end.Unix(), q)
	return source.NewFuture(source.DecodeNodeLabelsResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryNamespaceLabels queries for namespace labels from kube-state-metrics
func (pds *PrometheusMetricsQuerier) QueryNamespaceLabels(start, end time.Time) *source.Future[source.NamespaceLabelsResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg_over_time(kube_namespace_labels{%s}[%s])`, cfg.ClusterFilter, d)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNamespaceLabels", end.Unix(), q)
	return source.NewFuture(source.DecodeNamespaceLabelsResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryNamespaceAnnotations queries for namespace annotations from kube-state-metrics
func (pds *PrometheusMetricsQuerier) QueryNamespaceAnnotations(start, end time.Time) *source.Future[source.NamespaceAnnotationsResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg_over_time(kube_namespace_annotations{%s}[%s])`, cfg.ClusterFilter, d)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNamespaceAnnotations", end.Unix(), q)
	return source.NewFuture(source.DecodeNamespaceAnnotationsResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryPodLabels queries for pod labels from kube-state-metrics
func (pds *PrometheusMetricsQuerier) QueryPodLabels(start, end time.Time) *source.Future[source.PodLabelsResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg_over_time(kube_pod_labels{%s}[%s])`, cfg.ClusterFilter, d)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryPodLabels", end.Unix(), q)
	return source.NewFuture(source.DecodePodLabelsResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryPodAnnotations queries for pod annotations from kube-state-metrics
func (pds *PrometheusMetricsQuerier) QueryPodAnnotations(start, end time.Time) *source.Future[source.PodAnnotationsResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg_over_time(kube_pod_annotations{%s}[%s])`, cfg.ClusterFilter, d)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryPodAnnotations", end.Unix(), q)
	return source.NewFuture(source.DecodePodAnnotationsResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryServiceLabels queries for service selector labels from kube-state-metrics
func (pds *PrometheusMetricsQuerier) QueryServiceLabels(start, end time.Time) *source.Future[source.ServiceLabelsResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg_over_time(kube_service_labels{%s}[%s])`, cfg.ClusterFilter, d)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryServiceLabels", end.Unix(), q)
	return source.NewFuture(source.DecodeServiceLabelsResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryDeploymentLabels queries for deployment match labels from kube-state-metrics
func (pds *PrometheusMetricsQuerier) QueryDeploymentLabels(start, end time.Time) *source.Future[source.DeploymentLabelsResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg_over_time(kube_deployment_labels{%s}[%s])`, cfg.ClusterFilter, d)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryDeploymentLabels", end.Unix(), q)
	return source.NewFuture(source.DecodeDeploymentLabelsResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryStatefulSetLabels queries for statefulset labels from kube-state-metrics
func (pds *PrometheusMetricsQuerier) QueryStatefulSetLabels(start, end time.Time) *source.Future[source.StatefulSetLabelsResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg_over_time(kube_statefulset_labels{%s}[%s])`, cfg.ClusterFilter, d)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryStatefulSetLabels", end.Unix(), q)
	return source.NewFuture(source.DecodeStatefulSetLabelsResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryDaemonSetLabels queries for daemonset labels from kube-state-metrics
func (pds *PrometheusMetricsQuerier) QueryDaemonSetLabels(start, end time.Time) *source.Future[source.DaemonSetLabelsResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg_over_time(kube_daemonset_labels{%s}[%s])`, cfg.ClusterFilter, d)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryDaemonSetLabels", end.Unix(), q)
	return source.NewFuture(source.DecodeDaemonSetLabelsResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryJobLabels queries for job labels from kube-state-metrics
func (pds *PrometheusMetricsQuerier) QueryJobLabels(start, end time.Time) *source.Future[source.JobLabelsResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg_over_time(kube_job_labels{%s}[%s])`, cfg.ClusterFilter, d)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryJobLabels", end.Unix(), q)
	return source.NewFuture(source.DecodeJobLabelsResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryPodsWithReplicaSetOwner queries for pods that have ReplicaSet owners
func (pds *PrometheusMetricsQuerier) QueryPodsWithReplicaSetOwner(start, end time.Time) *source.Future[source.PodsWithReplicaSetOwnerResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg_over_time(kube_pod_owner{owner_kind="ReplicaSet",%s}[%s])`, cfg.ClusterFilter, d)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryPodsWithReplicaSetOwner", end.Unix(), q)
	return source.NewFuture(source.DecodePodsWithReplicaSetOwnerResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryReplicaSetsWithoutOwners queries for ReplicaSets without owner references
func (pds *PrometheusMetricsQuerier) QueryReplicaSetsWithoutOwners(start, end time.Time) *source.Future[source.ReplicaSetsWithoutOwnersResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	// ReplicaSets without owners will not have owner_name or owner_kind labels
	q := fmt.Sprintf(`avg_over_time(kube_replicaset_created{%s}[%s]) unless on(replicaset, namespace) avg_over_time(kube_replicaset_owner{%s}[%s])`,
		cfg.ClusterFilter, d, cfg.ClusterFilter, d)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryReplicaSetsWithoutOwners", end.Unix(), q)
	return source.NewFuture(source.DecodeReplicaSetsWithoutOwnersResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryReplicaSetsWithRollout queries for ReplicaSets associated with Argo Rollouts
func (pds *PrometheusMetricsQuerier) QueryReplicaSetsWithRollout(start, end time.Time) *source.Future[source.ReplicaSetsWithRolloutResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg_over_time(kube_replicaset_owner{owner_kind="Rollout",%s}[%s])`, cfg.ClusterFilter, d)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryReplicaSetsWithRollout", end.Unix(), q)
	return source.NewFuture(source.DecodeReplicaSetsWithRolloutResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}
