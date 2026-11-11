package prom

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	promsource "github.com/opencost/opencost/modules/prometheus-source/pkg/prom"
)

// QueryNodeCPUCoresCapacity returns node CPU capacity via KSM kube_node_status_capacity.
// Labels: k8s_cluster_name, k8s_node_name, resource, unit.
// There is no separate OTel "capacity" metric; allocatable is used as approximation
// when capacity is not available, but KSM provides both — prefer capacity here.
func (pds *PrometheusMetricsQuerier) QueryNodeCPUCoresCapacity(start, end time.Time) *source.Future[source.NodeCPUCoresCapacityResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(
		`avg(avg_over_time(kube_node_status_capacity{resource="cpu"%s}[%s])) by (%s, k8s_node_name)`,
		pds.clusterFilterSuffix(), d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNodeCPUCoresCapacity", end.Unix(), q)
	return source.NewFuture(source.DecodeNodeCPUCoresCapacityResult, pds.NewNamedContext(promsource.ClusterContextName).QueryAtTime(q, end))
}

// QueryNodeCPUCoresAllocatable returns node CPU allocatable via KSM kube_node_status_allocatable.
// Labels: k8s_cluster_name, k8s_node_name, resource, unit.
func (pds *PrometheusMetricsQuerier) QueryNodeCPUCoresAllocatable(start, end time.Time) *source.Future[source.NodeCPUCoresAllocatableResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(
		`avg(avg_over_time(kube_node_status_allocatable{resource="cpu"%s}[%s])) by (%s, k8s_node_name)`,
		pds.clusterFilterSuffix(), d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNodeCPUCoresAllocatable", end.Unix(), q)
	return source.NewFuture(source.DecodeNodeCPUCoresAllocatableResult, pds.NewNamedContext(promsource.ClusterContextName).QueryAtTime(q, end))
}

// QueryNodeRAMBytesCapacity returns node memory capacity via KSM kube_node_status_capacity.
// Labels: k8s_cluster_name, k8s_node_name, resource, unit.
func (pds *PrometheusMetricsQuerier) QueryNodeRAMBytesCapacity(start, end time.Time) *source.Future[source.NodeRAMBytesCapacityResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(
		`avg(avg_over_time(kube_node_status_capacity{resource="memory"%s}[%s])) by (%s, k8s_node_name)`,
		pds.clusterFilterSuffix(), d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNodeRAMBytesCapacity", end.Unix(), q)
	return source.NewFuture(source.DecodeNodeRAMBytesCapacityResult, pds.NewNamedContext(promsource.ClusterContextName).QueryAtTime(q, end))
}

// QueryNodeRAMBytesAllocatable returns node memory allocatable via KSM kube_node_status_allocatable.
// Labels: k8s_cluster_name, k8s_node_name, resource, unit.
func (pds *PrometheusMetricsQuerier) QueryNodeRAMBytesAllocatable(start, end time.Time) *source.Future[source.NodeRAMBytesAllocatableResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(
		`avg(avg_over_time(kube_node_status_allocatable{resource="memory"%s}[%s])) by (%s, k8s_node_name)`,
		pds.clusterFilterSuffix(), d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNodeRAMBytesAllocatable", end.Unix(), q)
	return source.NewFuture(source.DecodeNodeRAMBytesAllocatableResult, pds.NewNamedContext(promsource.ClusterContextName).QueryAtTime(q, end))
}

// QueryNodeGPUCount — no GPU node metrics available via OTel receivers.
func (pds *PrometheusMetricsQuerier) QueryNodeGPUCount(start, end time.Time) *source.Future[source.NodeGPUCountResult] {
	log.Warnf("QueryNodeGPUCount: no GPU node metrics available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.NodeGPUCountResult{})
}

// QueryNodeActiveMinutes returns node uptime intervals via OTel k8s_node_uptime.
// k8s_node_uptime has label k8s_node_name natively.
// Note: k8s_node_uptime also carries daemonset/pod context labels which are dropped by the by() clause.
func (pds *PrometheusMetricsQuerier) QueryNodeActiveMinutes(start, end time.Time) *source.Future[source.NodeActiveMinutesResult] {
	cfg := pds.promConfig
	m := cfg.DataResolutionMinutes
	d := pds.durationStringFor(start, end, m, false)
	q := fmt.Sprintf(
		`avg(k8s_node_uptime{%s}) by (k8s_node_name, %s)[%s:%dm]`,
		pds.clusterFilterSuffix(), cfg.ClusterLabel, d, m)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNodeActiveMinutes", end.Unix(), q)
	return source.NewFuture(source.DecodeNodeActiveMinutesResult, pds.NewNamedContext(promsource.ClusterContextName).QueryAtTime(q, end))
}

// QueryNodeCPUModeTotal returns per-mode CPU time via OTel system_cpu_time.
// OTel uses label "state"; classic node_exporter uses "mode". We rename state→mode
// via label_replace so the downstream DecodeNodeCPUModeTotalResult (which reads "mode")
// works correctly. Plain range vector used instead of subquery for accuracy.
func (pds *PrometheusMetricsQuerier) QueryNodeCPUModeTotal(start, end time.Time) *source.Future[source.NodeCPUModeTotalResult] {
	cfg := pds.promConfig
	m := cfg.DataResolutionMinutes
	d := pds.durationStringFor(start, end, m, false)
	q := fmt.Sprintf(
		`sum(label_replace(rate(system_cpu_time{%s}[%s]), "mode", "$1", "state", "(.*)")) by (k8s_node_name, %s, mode)`,
		pds.clusterFilterSuffix(), d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNodeCPUModeTotal", end.Unix(), q)
	return source.NewFuture(source.DecodeNodeCPUModeTotalResult, pds.NewNamedContext(promsource.ClusterContextName).QueryAtTime(q, end))
}

// QueryNodeRAMSystemPercent returns the fraction of node RAM used by kube-system containers.
// Uses OTel kubeletstats container_memory_working_set and KSM kube_node_status_allocatable.
func (pds *PrometheusMetricsQuerier) QueryNodeRAMSystemPercent(start, end time.Time) *source.Future[source.NodeRAMSystemPercentResult] {
	cfg := pds.promConfig
	m := cfg.DataResolutionMinutes
	d := pds.durationStringFor(start, end, m, false)
	q := fmt.Sprintf(
		`sum(sum_over_time(container_memory_working_set{k8s_container_name!="POD",k8s_container_name!="",k8s_namespace_name="kube-system"%s}[%s:%dm])) by (k8s_node_name,%s)`+
			` / sum(sum_over_time(kube_node_status_allocatable{resource="memory"%s}[%s:%dm])) by (k8s_node_name,%s)`,
		pds.clusterFilterSuffix(), d, m, cfg.ClusterLabel,
		pds.clusterFilterSuffix(), d, m, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNodeRAMSystemPercent", end.Unix(), q)
	return source.NewFuture(source.DecodeNodeRAMSystemPercentResult, pds.NewNamedContext(promsource.ClusterContextName).QueryAtTime(q, end))
}

// QueryNodeRAMUserPercent returns the fraction of node RAM used by non-kube-system containers.
func (pds *PrometheusMetricsQuerier) QueryNodeRAMUserPercent(start, end time.Time) *source.Future[source.NodeRAMUserPercentResult] {
	cfg := pds.promConfig
	m := cfg.DataResolutionMinutes
	d := pds.durationStringFor(start, end, m, false)
	q := fmt.Sprintf(
		`sum(sum_over_time(container_memory_working_set{k8s_container_name!="POD",k8s_container_name!="",k8s_namespace_name!="kube-system"%s}[%s:%dm])) by (k8s_node_name,%s)`+
			` / sum(sum_over_time(kube_node_status_allocatable{resource="memory"%s}[%s:%dm])) by (k8s_node_name,%s)`,
		pds.clusterFilterSuffix(), d, m, cfg.ClusterLabel,
		pds.clusterFilterSuffix(), d, m, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNodeRAMUserPercent", end.Unix(), q)
	return source.NewFuture(source.DecodeNodeRAMUserPercentResult, pds.NewNamedContext(promsource.ClusterContextName).QueryAtTime(q, end))
}

// QueryLBPricePerHr — no kubecost_load_balancer_cost metric available via OTel receivers.
func (pds *PrometheusMetricsQuerier) QueryLBPricePerHr(start, end time.Time) *source.Future[source.LBPricePerHrResult] {
	log.Warnf("QueryLBPricePerHr: kubecost_load_balancer_cost not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.LBPricePerHrResult{})
}

// QueryLBActiveMinutes — no kubecost_load_balancer_cost metric available via OTel receivers.
func (pds *PrometheusMetricsQuerier) QueryLBActiveMinutes(start, end time.Time) *source.Future[source.LBActiveMinutesResult] {
	log.Warnf("QueryLBActiveMinutes: kubecost_load_balancer_cost not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.LBActiveMinutesResult{})
}

// QueryClusterManagementDuration — no kubecost_cluster_management_cost metric available via OTel receivers.
func (pds *PrometheusMetricsQuerier) QueryClusterManagementDuration(start, end time.Time) *source.Future[source.ClusterManagementDurationResult] {
	log.Warnf("QueryClusterManagementDuration: kubecost_cluster_management_cost not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.ClusterManagementDurationResult{})
}

// QueryClusterManagementPricePerHr — no kubecost_cluster_management_cost metric available via OTel receivers.
func (pds *PrometheusMetricsQuerier) QueryClusterManagementPricePerHr(start, end time.Time) *source.Future[source.ClusterManagementPricePerHrResult] {
	log.Warnf("QueryClusterManagementPricePerHr: kubecost_cluster_management_cost not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.ClusterManagementPricePerHrResult{})
}
