package prom

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	promsource "github.com/opencost/opencost/modules/prometheus-source/pkg/prom"
)

// --- Node capacity/allocatable (OTel k8sclusterreceiver) ---

func (pds *PrometheusMetricsQuerier) QueryNodeCPUCoresCapacity(start, end time.Time) *source.Future[source.NodeCPUCoresCapacityResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	// Use kube_node_status_capacity from kube-state-metrics with resource="cpu"
	q := fmt.Sprintf(`avg(avg_over_time(kube_node_status_capacity{resource="cpu",%s}[%s])) by (%s, node)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNodeCPUCoresCapacity", end.Unix(), q)
	return source.NewFuture(source.DecodeNodeCPUCoresCapacityResult, pds.NewNamedContext(promsource.ClusterContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeCPUCoresAllocatable(start, end time.Time) *source.Future[source.NodeCPUCoresAllocatableResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	// OTel k8scluster receiver uses k8s_node_allocatable_cpu with k8s_node_name label
	q := fmt.Sprintf(`avg(avg_over_time(k8s_node_allocatable_cpu{%s}[%s])) by (%s, k8s_node_name)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNodeCPUCoresAllocatable", end.Unix(), q)
	return source.NewFuture(source.DecodeNodeCPUCoresAllocatableResult, pds.NewNamedContext(promsource.ClusterContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeRAMBytesCapacity(start, end time.Time) *source.Future[source.NodeRAMBytesCapacityResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	// Use kube_node_status_capacity from kube-state-metrics with resource="memory"
	q := fmt.Sprintf(`avg(avg_over_time(kube_node_status_capacity{resource="memory",%s}[%s])) by (%s, node)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNodeRAMBytesCapacity", end.Unix(), q)
	return source.NewFuture(source.DecodeNodeRAMBytesCapacityResult, pds.NewNamedContext(promsource.ClusterContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeRAMBytesAllocatable(start, end time.Time) *source.Future[source.NodeRAMBytesAllocatableResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	// OTel k8scluster receiver uses k8s_node_allocatable_memory with k8s_node_name label
	q := fmt.Sprintf(`avg(avg_over_time(k8s_node_allocatable_memory{%s}[%s])) by (%s, k8s_node_name)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNodeRAMBytesAllocatable", end.Unix(), q)
	return source.NewFuture(source.DecodeNodeRAMBytesAllocatableResult, pds.NewNamedContext(promsource.ClusterContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeGPUCount(start, end time.Time) *source.Future[source.NodeGPUCountResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg(avg_over_time(node_gpu_count{%s}[%s])) by (%s, k8s_node_name, provider_id)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNodeGPUCount", end.Unix(), q)
	return source.NewFuture(source.DecodeNodeGPUCountResult, pds.NewNamedContext(promsource.ClusterContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeActiveMinutes(start, end time.Time) *source.Future[source.NodeActiveMinutesResult] {
	cfg := pds.promConfig
	m := cfg.DataResolutionMinutes
	d := pds.durationStringFor(start, end, m, false)
	q := fmt.Sprintf(`avg(node_total_hourly_cost{%s}) by (node, %s, provider_id)[%s:%dm]`, cfg.ClusterFilter, cfg.ClusterLabel, d, m)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNodeActiveMinutes", end.Unix(), q)
	return source.NewFuture(source.DecodeNodeActiveMinutesResult, pds.NewNamedContext(promsource.ClusterContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeCPUModeTotal(start, end time.Time) *source.Future[source.NodeCPUModeTotalResult] {
	cfg := pds.promConfig
	m := cfg.DataResolutionMinutes
	d := pds.durationStringFor(start, end, m, false)
	q := fmt.Sprintf(`sum(rate(system_cpu_time{%s}[%s:%dm])) by (kubernetes_node, %s, state)`, cfg.ClusterFilter, d, m, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNodeCPUModeTotal", end.Unix(), q)
	return source.NewFuture(source.DecodeNodeCPUModeTotalResult, pds.NewNamedContext(promsource.ClusterContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeRAMSystemPercent(start, end time.Time) *source.Future[source.NodeRAMSystemPercentResult] {
	cfg := pds.promConfig
	m := cfg.DataResolutionMinutes
	d := pds.durationStringFor(start, end, m, false)
	// OTel kubeletstats uses container_memory_working_set, OTel k8scluster uses k8s_node_allocatable_memory
	// Both use k8s_node_name label for consistency
	q := fmt.Sprintf(`sum(sum_over_time(container_memory_working_set{k8s_container_name!="POD",k8s_container_name!="",k8s_namespace_name="kube-system",%s}[%s:%dm])) by (k8s_node_name,%s) / sum(sum_over_time(k8s_node_allocatable_memory{%s}[%s:%dm])) by (k8s_node_name,%s)`, cfg.ClusterFilter, d, m, cfg.ClusterLabel, cfg.ClusterFilter, d, m, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNodeRAMSystemPercent", end.Unix(), q)
	return source.NewFuture(source.DecodeNodeRAMSystemPercentResult, pds.NewNamedContext(promsource.ClusterContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeRAMUserPercent(start, end time.Time) *source.Future[source.NodeRAMUserPercentResult] {
	cfg := pds.promConfig
	m := cfg.DataResolutionMinutes
	d := pds.durationStringFor(start, end, m, false)
	// OTel kubeletstats uses container_memory_working_set, OTel k8scluster uses k8s_node_allocatable_memory
	// Both use k8s_node_name label for consistency
	q := fmt.Sprintf(`sum(sum_over_time(container_memory_working_set{k8s_container_name!="POD",k8s_container_name!="",k8s_namespace_name!="kube-system",%s}[%s:%dm])) by (k8s_node_name,%s) / sum(sum_over_time(k8s_node_allocatable_memory{%s}[%s:%dm])) by (k8s_node_name,%s)`, cfg.ClusterFilter, d, m, cfg.ClusterLabel, cfg.ClusterFilter, d, m, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNodeRAMUserPercent", end.Unix(), q)
	return source.NewFuture(source.DecodeNodeRAMUserPercentResult, pds.NewNamedContext(promsource.ClusterContextName).QueryAtTime(q, end))
}

// --- LB / Cluster Management ---

func (pds *PrometheusMetricsQuerier) QueryLBPricePerHr(start, end time.Time) *source.Future[source.LBPricePerHrResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg(avg_over_time(kubecost_load_balancer_cost{%s}[%s])) by (namespace, service_name, ingress_ip, %s)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryLBPricePerHr", end.Unix(), q)
	return source.NewFuture(source.DecodeLBPricePerHrResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryLBActiveMinutes(start, end time.Time) *source.Future[source.LBActiveMinutesResult] {
	cfg := pds.promConfig
	m := cfg.DataResolutionMinutes
	d := pds.durationStringFor(start, end, m, false)
	q := fmt.Sprintf(`avg(kubecost_load_balancer_cost{%s}) by (namespace, service_name, %s, ingress_ip)[%s:%dm]`, cfg.ClusterFilter, cfg.ClusterLabel, d, m)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryLBActiveMinutes", end.Unix(), q)
	return source.NewFuture(source.DecodeLBActiveMinutesResult, pds.NewNamedContext(promsource.ClusterContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryClusterManagementDuration(start, end time.Time) *source.Future[source.ClusterManagementDurationResult] {
	cfg := pds.promConfig
	m := cfg.DataResolutionMinutes
	d := pds.durationStringFor(start, end, m, false)
	q := fmt.Sprintf(`max(kubecost_cluster_management_cost{%s}) by (%s)[%s:%dm]`, cfg.ClusterFilter, cfg.ClusterLabel, d, m)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryClusterManagementDuration", end.Unix(), q)
	return source.NewFuture(source.DecodeClusterManagementDurationResult, pds.NewNamedContext(promsource.ClusterContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryClusterManagementPricePerHr(start, end time.Time) *source.Future[source.ClusterManagementPricePerHrResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg(avg_over_time(kubecost_cluster_management_cost{%s}[%s])) by (%s)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryClusterManagementPricePerHr", end.Unix(), q)
	return source.NewFuture(source.DecodeClusterManagementPricePerHrResult, pds.NewNamedContext(promsource.ClusterContextName).QueryAtTime(q, end))
}
