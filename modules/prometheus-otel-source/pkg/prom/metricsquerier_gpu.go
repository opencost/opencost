package prom

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

func (pds *PrometheusMetricsQuerier) QueryGPUsRequested(start, end time.Time) *source.Future[source.GPUsRequestedResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg(avg_over_time(kube_pod_container_resource_requests{resource="nvidia_com_gpu",k8s_container_name!="",container!="POD",k8s_node_name!="",%s}[%s])) by (k8s_container_name,k8s_pod_name,k8s_namespace_name,k8s_node_name,%s)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryGPUsRequested", end.Unix(), q)
	return source.NewFuture(source.DecodeGPUsRequestedResult, pds.NewNamedContext(AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryGPUsUsageAvg(start, end time.Time) *source.Future[source.GPUsUsageAvgResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg(avg_over_time(DCGM_FI_PROF_GR_ENGINE_ACTIVE{k8s_container_name!=""}[%s])) by (k8s_container_name,k8s_pod_name,k8s_namespace_name,%s)`, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryGPUsUsageAvg", end.Unix(), q)
	return source.NewFuture(source.DecodeGPUsUsageAvgResult, pds.NewNamedContext(AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryGPUsUsageMax(start, end time.Time) *source.Future[source.GPUsUsageMaxResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`max(max_over_time(DCGM_FI_PROF_GR_ENGINE_ACTIVE{k8s_container_name!=""}[%s])) by (k8s_container_name,k8s_pod_name,k8s_namespace_name,%s)`, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryGPUsUsageMax", end.Unix(), q)
	return source.NewFuture(source.DecodeGPUsUsageMaxResult, pds.NewNamedContext(AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryGPUsAllocated(start, end time.Time) *source.Future[source.GPUsAllocatedResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg(avg_over_time(container_gpu_allocation{k8s_container_name!="",k8s_container_name!="POD",k8s_node_name!="",%s}[%s])) by (k8s_container_name,k8s_pod_name,k8s_namespace_name,k8s_node_name,%s)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryGPUsAllocated", end.Unix(), q)
	return source.NewFuture(source.DecodeGPUsAllocatedResult, pds.NewNamedContext(AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryIsGPUShared(start, end time.Time) *source.Future[source.IsGPUSharedResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg(avg_over_time(kube_pod_container_resource_requests{k8s_container_name!="",node!="",pod!="",k8s_container_name!="",unit="integer",%s}[%s])) by (k8s_container_name,k8s_pod_name,k8s_namespace_name,k8s_node_name,resource,%s)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryIsGPUShared", end.Unix(), q)
	return source.NewFuture(source.DecodeIsGPUSharedResult, pds.NewNamedContext(AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryGPUInfo(start, end time.Time) *source.Future[source.GPUInfoResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg(avg_over_time(DCGM_FI_DEV_DEC_UTIL{k8s_container_name!="",%s}[%s])) by (k8s_container_name,k8s_pod_name,k8s_namespace_name,device,modelName,UUID,%s)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryGPUInfo", end.Unix(), q)
	return source.NewFuture(source.DecodeGPUInfoResult, pds.NewNamedContext(AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeCPUPricePerHr(start, end time.Time) *source.Future[source.NodeCPUPricePerHrResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg(avg_over_time(node_cpu_hourly_cost{%s}[%s])) by (node,%s,instance_type,provider_id)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNodeCPUPricePerHr", end.Unix(), q)
	return source.NewFuture(source.DecodeNodeCPUPricePerHrResult, pds.NewNamedContext(AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeRAMPricePerGiBHr(start, end time.Time) *source.Future[source.NodeRAMPricePerGiBHrResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg(avg_over_time(node_ram_hourly_cost{%s}[%s])) by (node,%s,instance_type,provider_id)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNodeRAMPricePerGiBHr", end.Unix(), q)
	return source.NewFuture(source.DecodeNodeRAMPricePerGiBHrResult, pds.NewNamedContext(AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeGPUPricePerHr(start, end time.Time) *source.Future[source.NodeGPUPricePerHrResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg(avg_over_time(node_gpu_hourly_cost{%s}[%s])) by (node,%s,instance_type,provider_id)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNodeGPUPricePerHr", end.Unix(), q)
	return source.NewFuture(source.DecodeNodeGPUPricePerHrResult, pds.NewNamedContext(AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeIsSpot(start, end time.Time) *source.Future[source.NodeIsSpotResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg_over_time(kubecost_node_is_spot{%s}[%s])`, cfg.ClusterFilter, d)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNodeIsSpot", end.Unix(), q)
	return source.NewFuture(source.DecodeNodeIsSpotResult, pds.NewNamedContext(AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryPodPVCAllocation(start, end time.Time) *source.Future[source.PodPVCAllocationResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg(avg_over_time(pod_pvc_allocation{%s}[%s])) by (persistentvolume,persistentvolumeclaim,k8s_pod_name,k8s_namespace_name,%s)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryPodPVCAllocation", end.Unix(), q)
	return source.NewFuture(source.DecodePodPVCAllocationResult, pds.NewNamedContext(AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryPVCBytesRequested(start, end time.Time) *source.Future[source.PVCBytesRequestedResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg(avg_over_time(kube_persistentvolumeclaim_resource_requests_storage_bytes{%s}[%s])) by (persistentvolumeclaim,k8s_namespace_name,%s)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryPVCBytesRequested", end.Unix(), q)
	return source.NewFuture(source.DecodePVCBytesRequestedResult, pds.NewNamedContext(AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryPVBytes(start, end time.Time) *source.Future[source.PVBytesResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg(avg_over_time(kube_persistentvolume_capacity_bytes{%s}[%s])) by (persistentvolume,%s)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryPVBytes", end.Unix(), q)
	return source.NewFuture(source.DecodePVBytesResult, pds.NewNamedContext(AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryPVInfo(start, end time.Time) *source.Future[source.PVInfoResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg(avg_over_time(kubecost_pv_info{%s}[%s])) by (%s,storageclass,persistentvolume,provider_id)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryPVInfo", end.Unix(), q)
	return source.NewFuture(source.DecodePVInfoResult, pds.NewNamedContext(AllocationContextName).QueryAtTime(q, end))
}
