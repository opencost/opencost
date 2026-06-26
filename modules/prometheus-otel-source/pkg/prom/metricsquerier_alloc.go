package prom

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	promsource "github.com/opencost/opencost/modules/prometheus-source/pkg/prom"
)

func (pds *PrometheusMetricsQuerier) QueryPods(start, end time.Time) *source.Future[source.PodsResult] {
	cfg := pds.promConfig
	m := cfg.DataResolutionMinutes
	d := pds.durationStringFor(start, end, m, false)
	// When kube-state-metrics already emits OTel-style labels (k8s_pod_name, k8s_namespace_name),
	// no label_replace transformation is needed
	q := fmt.Sprintf(`avg(kube_pod_container_status_running{%s} != 0) by (k8s_pod_name, k8s_namespace_name, %s)[%s:%dm]`, cfg.ClusterFilter, cfg.ClusterLabel, d, m)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryPods", end.Unix(), q)
	return source.NewFuture(source.DecodePodsResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryPodsUID(start, end time.Time) *source.Future[source.PodsResult] {
	cfg := pds.promConfig
	m := cfg.DataResolutionMinutes
	d := pds.durationStringFor(start, end, m, false)
	// When kube-state-metrics already emits OTel-style labels (k8s_pod_name, k8s_namespace_name),
	// no label_replace transformation is needed
	q := fmt.Sprintf(`avg(kube_pod_container_status_running{%s} != 0) by (k8s_pod_name, k8s_namespace_name, uid, %s)[%s:%dm]`, cfg.ClusterFilter, cfg.ClusterLabel, d, m)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryPodsUID", end.Unix(), q)
	return source.NewFuture(source.DecodePodsResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryRAMBytesAllocated(start, end time.Time) *source.Future[source.RAMBytesAllocatedResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	// OTel kubeletstats receiver uses container_memory_usage (not k8s_container_memory_usage)
	q := fmt.Sprintf(`avg(avg_over_time(container_memory_usage{k8s_container_name!="",k8s_container_name!="POD",k8s_node_name!="",%s}[%s])) by (k8s_container_name,k8s_pod_name,k8s_namespace_name,k8s_node_name,%s,provider_id)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryRAMBytesAllocated", end.Unix(), q)
	return source.NewFuture(source.DecodeRAMBytesAllocatedResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryRAMRequests(start, end time.Time) *source.Future[source.RAMRequestsResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg(avg_over_time(kube_pod_container_resource_requests{resource="memory",unit="byte",k8s_container_name!="",k8s_container_name!="POD",k8s_node_name!="",%s}[%s])) by (k8s_container_name,k8s_pod_name,k8s_namespace_name,k8s_node_name,%s)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryRAMRequests", end.Unix(), q)
	return source.NewFuture(source.DecodeRAMRequestsResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryRAMUsageAvg(start, end time.Time) *source.Future[source.RAMUsageAvgResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	// OTel kubeletstats receiver uses container_memory_working_set (not k8s_container_memory_working_set)
	q := fmt.Sprintf(`avg(avg_over_time(container_memory_working_set{k8s_container_name!="",k8s_container_name!="POD",%s}[%s])) by (k8s_container_name,k8s_pod_name,k8s_namespace_name,k8s_node_name,instance,%s)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryRAMUsageAvg", end.Unix(), q)
	return source.NewFuture(source.DecodeRAMUsageAvgResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryRAMUsageMax(start, end time.Time) *source.Future[source.RAMUsageMaxResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	// OTel kubeletstats receiver uses container_memory_working_set (not k8s_container_memory_working_set)
	q := fmt.Sprintf(`max(max_over_time(container_memory_working_set{k8s_container_name!="",k8s_container_name!="POD",%s}[%s])) by (k8s_container_name,k8s_pod_name,k8s_namespace_name,k8s_node_name,instance,%s)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryRAMUsageMax", end.Unix(), q)
	return source.NewFuture(source.DecodeRAMUsageMaxResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryCPUCoresAllocated(start, end time.Time) *source.Future[source.CPUCoresAllocatedResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg(avg_over_time(container_cpu_allocation{k8s_container_name!="",k8s_container_name!="POD",k8s_node_name!="",%s}[%s])) by (k8s_container_name,k8s_pod_name,k8s_namespace_name,k8s_node_name,%s)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryCPUCoresAllocated", end.Unix(), q)
	return source.NewFuture(source.DecodeCPUCoresAllocatedResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryCPURequests(start, end time.Time) *source.Future[source.CPURequestsResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg(avg_over_time(kube_pod_container_resource_requests{resource="cpu",unit="core",k8s_container_name!="",k8s_container_name!="POD",k8s_node_name!="",%s}[%s])) by (k8s_container_name,k8s_pod_name,k8s_namespace_name,k8s_node_name,%s)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryCPURequests", end.Unix(), q)
	return source.NewFuture(source.DecodeCPURequestsResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryCPUUsageAvg(start, end time.Time) *source.Future[source.CPUUsageAvgResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	// OTel kubeletstats receiver uses container_cpu_time (not k8s_container_cpu_time)
	q := fmt.Sprintf(`avg(rate(container_cpu_time{k8s_container_name!="",k8s_container_name!="POD",%s}[%s])) by (k8s_container_name,k8s_pod_name,k8s_namespace_name,k8s_node_name,instance,%s)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryCPUUsageAvg", end.Unix(), q)
	return source.NewFuture(source.DecodeCPUUsageAvgResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryCPUUsageMax(start, end time.Time) *source.Future[source.CPUUsageMaxResult] {
	const queryName = "QueryCPUUsageMax"
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	// Try OpenCost recording rule first
	qRR := fmt.Sprintf(`max(max_over_time(kubecost_container_cpu_usage_irate{%s}[%s])) by (k8s_container_name,k8s_pod_name,k8s_namespace_name,k8s_node_name,instance,%s)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), qRR)
	ctx := pds.NewNamedContext(promsource.AllocationContextName)
	resRR, errRR := ctx.QueryAtTime(qRR, end).Await()
	if errRR != nil {
		log.Debugf("Recording rule query failed, falling back to subquery: %s", errRR)
	}
	if len(resRR) > 0 {
		return source.NewFutureFrom(source.DecodeAll(resRR, source.DecodeCPUUsageMaxResult))
	}
	// Fallback to subquery using OTel container_cpu_time (not k8s_container_cpu_time)
	m := cfg.DataResolutionMinutes
	d2 := pds.durationStringFor(start, end, m, false)
	qSub := fmt.Sprintf(`max(max_over_time(irate(container_cpu_time{k8s_container_name!="POD",k8s_container_name!="",%s}[%dm])[%s:%dm])) by (k8s_container_name,k8s_pod_name,k8s_namespace_name,k8s_node_name,instance,%s)`, cfg.ClusterFilter, 2*m, d2, m, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), qSub)
	return source.NewFuture(source.DecodeCPUUsageMaxResult, ctx.QueryAtTime(qSub, end))
}
