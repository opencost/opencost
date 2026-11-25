package prom

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	promsource "github.com/opencost/opencost/modules/prometheus-source/pkg/prom"
)

// QueryPods returns running pods via kube_pod_container_status_running (KSM, OTel-labeled).
// Labels: k8s_cluster_name, k8s_container_name, k8s_namespace_name, k8s_pod_name, k8s_pod_uid.
// We group by pod/namespace/cluster to avoid per-container duplicates.
func (pds *PrometheusMetricsQuerier) QueryPods(start, end time.Time) *source.Future[source.PodsResult] {
	cfg := pds.promConfig
	m := cfg.DataResolutionMinutes
	d := pds.durationStringFor(start, end, m, false)
	q := fmt.Sprintf(
		`avg(kube_pod_container_status_running{%s} == 1) by (k8s_pod_name, k8s_namespace_name, %s)[%s:%dm]`,
		pds.clusterFilterSuffix(), cfg.ClusterLabel, d, m)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryPods", end.Unix(), q)
	return source.NewFuture(source.DecodePodsResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryPodsUID is called when INGEST_POD_UID=true is set.
// Pod UID ingestion is not supported by the prometheus-otel-source module because
// it was designed for Kubecost's replicated metric setup which does not apply to
// OTel/KSM deployments. Log a clear warning and fall back to QueryPods so the
// allocation pipeline continues to work (without pod deduplication by UID).
func (pds *PrometheusMetricsQuerier) QueryPodsUID(start, end time.Time) *source.Future[source.PodsResult] {
	log.Warnf("QueryPodsUID: INGEST_POD_UID is not supported by the prometheus-otel-source " +
		"module. Pod deduplication by UID requires Kubecost's replicated metric setup which " +
		"is not available in OTel/KSM deployments. Falling back to QueryPods (no UID). " +
		"Disable INGEST_POD_UID to suppress this warning.")
	return pds.QueryPods(start, end)
}

// QueryRAMBytesAllocated returns memory requests via KSM kube_pod_container_resource_requests.
// Uses max(request, usage) semantics: request is the allocation floor in OpenCost.
// Labels: k8s_cluster_name, k8s_container_name, k8s_namespace_name, k8s_node_name, k8s_pod_name.
func (pds *PrometheusMetricsQuerier) QueryRAMBytesAllocated(start, end time.Time) *source.Future[source.RAMBytesAllocatedResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(
		`avg(avg_over_time(kube_pod_container_resource_requests{resource="memory",k8s_container_name!="",k8s_container_name!="POD",k8s_node_name!=""%s}[%s])) by (k8s_container_name, k8s_pod_name, k8s_namespace_name, k8s_node_name, %s)`,
		pds.clusterFilterSuffix(), d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryRAMBytesAllocated", end.Unix(), q)
	return source.NewFuture(source.DecodeRAMBytesAllocatedResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryRAMRequests returns container memory requests via KSM kube_pod_container_resource_requests.
// Labels: k8s_cluster_name, k8s_container_name, k8s_namespace_name, k8s_node_name, k8s_pod_name, resource, unit.
func (pds *PrometheusMetricsQuerier) QueryRAMRequests(start, end time.Time) *source.Future[source.RAMRequestsResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(
		`avg(avg_over_time(kube_pod_container_resource_requests{resource="memory",k8s_container_name!="",k8s_container_name!="POD",k8s_node_name!=""%s}[%s])) by (k8s_container_name, k8s_pod_name, k8s_namespace_name, k8s_node_name, %s)`,
		pds.clusterFilterSuffix(), d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryRAMRequests", end.Unix(), q)
	return source.NewFuture(source.DecodeRAMRequestsResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryRAMLimits returns container memory limits via KSM kube_pod_container_resource_limits.
// Labels: k8s_cluster_name, k8s_container_name, k8s_namespace_name, k8s_node_name, k8s_pod_name, resource, unit.
func (pds *PrometheusMetricsQuerier) QueryRAMLimits(start, end time.Time) *source.Future[source.RAMLimitsResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(
		`avg(avg_over_time(kube_pod_container_resource_limits{resource="memory",k8s_container_name!="",k8s_container_name!="POD",k8s_node_name!=""%s}[%s])) by (k8s_container_name, k8s_pod_name, k8s_namespace_name, k8s_node_name, %s)`,
		pds.clusterFilterSuffix(), d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryRAMLimits", end.Unix(), q)
	return source.NewFuture(source.DecodeRAMLimitsResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryRAMUsageAvg returns average memory working set via OTel kubeletstats container_memory_working_set.
// Replaces cAdvisor container_memory_working_set_bytes.
// Labels: k8s_cluster_name, k8s_container_name, k8s_namespace_name, k8s_node_name, k8s_pod_name.
func (pds *PrometheusMetricsQuerier) QueryRAMUsageAvg(start, end time.Time) *source.Future[source.RAMUsageAvgResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(
		`avg(avg_over_time(container_memory_working_set{k8s_container_name!="",k8s_container_name!="POD"%s}[%s])) by (k8s_container_name, k8s_pod_name, k8s_namespace_name, k8s_node_name, %s)`,
		pds.clusterFilterSuffix(), d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryRAMUsageAvg", end.Unix(), q)
	return source.NewFuture(source.DecodeRAMUsageAvgResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryRAMUsageMax returns peak memory working set via OTel kubeletstats container_memory_working_set.
func (pds *PrometheusMetricsQuerier) QueryRAMUsageMax(start, end time.Time) *source.Future[source.RAMUsageMaxResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(
		`max(max_over_time(container_memory_working_set{k8s_container_name!="",k8s_container_name!="POD"%s}[%s])) by (k8s_container_name, k8s_pod_name, k8s_namespace_name, k8s_node_name, %s)`,
		pds.clusterFilterSuffix(), d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryRAMUsageMax", end.Unix(), q)
	return source.NewFuture(source.DecodeRAMUsageMaxResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryCPUCoresAllocated returns CPU requests via KSM kube_pod_container_resource_requests.
// CPU request is the allocation floor in OpenCost (max(request, usage)).
func (pds *PrometheusMetricsQuerier) QueryCPUCoresAllocated(start, end time.Time) *source.Future[source.CPUCoresAllocatedResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(
		`avg(avg_over_time(kube_pod_container_resource_requests{resource="cpu",k8s_container_name!="",k8s_container_name!="POD",k8s_node_name!=""%s}[%s])) by (k8s_container_name, k8s_pod_name, k8s_namespace_name, k8s_node_name, %s)`,
		pds.clusterFilterSuffix(), d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryCPUCoresAllocated", end.Unix(), q)
	return source.NewFuture(source.DecodeCPUCoresAllocatedResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryCPURequests returns container CPU requests via KSM kube_pod_container_resource_requests.
// Labels: k8s_cluster_name, k8s_container_name, k8s_namespace_name, k8s_node_name, k8s_pod_name, resource, unit.
func (pds *PrometheusMetricsQuerier) QueryCPURequests(start, end time.Time) *source.Future[source.CPURequestsResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(
		`avg(avg_over_time(kube_pod_container_resource_requests{resource="cpu",k8s_container_name!="",k8s_container_name!="POD",k8s_node_name!=""%s}[%s])) by (k8s_container_name, k8s_pod_name, k8s_namespace_name, k8s_node_name, %s)`,
		pds.clusterFilterSuffix(), d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryCPURequests", end.Unix(), q)
	return source.NewFuture(source.DecodeCPURequestsResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryCPULimits returns container CPU limits via KSM kube_pod_container_resource_limits.
// Labels: k8s_cluster_name, k8s_container_name, k8s_namespace_name, k8s_node_name, k8s_pod_name, resource, unit.
func (pds *PrometheusMetricsQuerier) QueryCPULimits(start, end time.Time) *source.Future[source.CPULimitsResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(
		`avg(avg_over_time(kube_pod_container_resource_limits{resource="cpu",k8s_container_name!="",k8s_container_name!="POD",k8s_node_name!=""%s}[%s])) by (k8s_container_name, k8s_pod_name, k8s_namespace_name, k8s_node_name, %s)`,
		pds.clusterFilterSuffix(), d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryCPULimits", end.Unix(), q)
	return source.NewFuture(source.DecodeCPULimitsResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryCPUUsageAvg returns average CPU usage via OTel kubeletstats container_cpu_usage.
// container_cpu_usage is a gauge in cores, pre-computed by the OTel collector.
// Replaces rate(container_cpu_usage_seconds_total) from cAdvisor.
func (pds *PrometheusMetricsQuerier) QueryCPUUsageAvg(start, end time.Time) *source.Future[source.CPUUsageAvgResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(
		`avg(avg_over_time(container_cpu_usage{k8s_container_name!="",k8s_container_name!="POD"%s}[%s])) by (k8s_container_name, k8s_pod_name, k8s_namespace_name, k8s_node_name, %s)`,
		pds.clusterFilterSuffix(), d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryCPUUsageAvg", end.Unix(), q)
	return source.NewFuture(source.DecodeCPUUsageAvgResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryCPUUsageMax returns peak CPU usage via OTel kubeletstats.
// Uses a subquery over container_cpu_time (cumulative CPU seconds, OTel equivalent of
// container_cpu_usage_seconds_total from cAdvisor).
func (pds *PrometheusMetricsQuerier) QueryCPUUsageMax(start, end time.Time) *source.Future[source.CPUUsageMaxResult] {
	cfg := pds.promConfig
	m := cfg.DataResolutionMinutes
	d := pds.durationStringFor(start, end, m, false)
	q := fmt.Sprintf(
		`max(max_over_time(irate(container_cpu_time{k8s_container_name!="",k8s_container_name!="POD"%s}[%dm])[%s:%dm])) by (k8s_container_name, k8s_pod_name, k8s_namespace_name, k8s_node_name, %s)`,
		pds.clusterFilterSuffix(), 2*m, d, m, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryCPUUsageMax", end.Unix(), q)
	return source.NewFuture(source.DecodeCPUUsageMaxResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryGPUsRequested — no GPU resource request metrics available via OTel receivers.
func (pds *PrometheusMetricsQuerier) QueryGPUsRequested(start, end time.Time) *source.Future[source.GPUsRequestedResult] {
	log.Warnf("QueryGPUsRequested: no GPU metrics available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.GPUsRequestedResult{})
}

// QueryGPUsAllocated — no GPU allocation metrics available via OTel receivers.
func (pds *PrometheusMetricsQuerier) QueryGPUsAllocated(start, end time.Time) *source.Future[source.GPUsAllocatedResult] {
	log.Warnf("QueryGPUsAllocated: no GPU metrics available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.GPUsAllocatedResult{})
}

// QueryGPUsUsageAvg — DCGM metrics not available via standard OTel receivers.
func (pds *PrometheusMetricsQuerier) QueryGPUsUsageAvg(start, end time.Time) *source.Future[source.GPUsUsageAvgResult] {
	log.Warnf("QueryGPUsUsageAvg: DCGM metrics not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.GPUsUsageAvgResult{})
}

// QueryGPUsUsageMax — DCGM metrics not available via standard OTel receivers.
func (pds *PrometheusMetricsQuerier) QueryGPUsUsageMax(start, end time.Time) *source.Future[source.GPUsUsageMaxResult] {
	log.Warnf("QueryGPUsUsageMax: DCGM metrics not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.GPUsUsageMaxResult{})
}

// QueryContainerResourceLimits returns per-resource limits via KSM kube_pod_container_resource_limits.
// Labels (OTel-native): k8s_cluster_name, k8s_container_name, k8s_namespace_name, k8s_node_name,
// k8s_pod_name, resource, unit.
func (pds *PrometheusMetricsQuerier) QueryContainerResourceLimits(start, end time.Time) *source.Future[source.ContainerResourceResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(
		`avg(avg_over_time(kube_pod_container_resource_limits{k8s_container_name!="",k8s_container_name!="POD",k8s_node_name!=""%s}[%s])) by (k8s_container_name, k8s_pod_name, resource, unit, %s)`,
		pds.clusterFilterSuffix(), d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryContainerResourceLimits", end.Unix(), q)
	return source.NewFuture(source.DecodeContainerResourceResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryContainerResourceRequests returns per-resource requests via KSM kube_pod_container_resource_requests.
// Labels (OTel-native): k8s_cluster_name, k8s_container_name, k8s_namespace_name, k8s_node_name,
// k8s_pod_name, resource, unit.
func (pds *PrometheusMetricsQuerier) QueryContainerResourceRequests(start, end time.Time) *source.Future[source.ContainerResourceResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(
		`avg(avg_over_time(kube_pod_container_resource_requests{k8s_container_name!="",k8s_container_name!="POD",k8s_node_name!=""%s}[%s])) by (k8s_container_name, k8s_pod_name, resource, unit, %s)`,
		pds.clusterFilterSuffix(), d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryContainerResourceRequests", end.Unix(), q)
	return source.NewFuture(source.DecodeContainerResourceResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryContainerUptime returns container uptime intervals via KSM kube_pod_container_status_running.
// Labels (OTel-native): k8s_cluster_name, k8s_container_name, k8s_namespace_name, k8s_pod_name, k8s_pod_uid.
func (pds *PrometheusMetricsQuerier) QueryContainerUptime(start, end time.Time) *source.Future[source.ContainerUptimeResult] {
	cfg := pds.promConfig
	m := cfg.DataResolutionMinutes
	d := pds.durationStringFor(start, end, m, false)
	q := fmt.Sprintf(
		`avg(kube_pod_container_status_running{k8s_container_name!=""%s} != 0) by (k8s_container_name, k8s_pod_name, k8s_namespace_name, %s)[%s:%dm]`,
		pds.clusterFilterSuffix(), cfg.ClusterLabel, d, m)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryContainerUptime", end.Unix(), q)
	return source.NewFuture(source.DecodeContainerUptimeResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}
