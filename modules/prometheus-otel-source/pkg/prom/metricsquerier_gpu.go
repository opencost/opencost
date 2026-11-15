package prom

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	promsource "github.com/opencost/opencost/modules/prometheus-source/pkg/prom"
)

// QueryIsGPUShared queries for integer resource requests to determine GPU sharing.
// Uses KSM kube_pod_container_resource_requests with OTel-native labels.
func (pds *PrometheusMetricsQuerier) QueryIsGPUShared(start, end time.Time) *source.Future[source.IsGPUSharedResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg(avg_over_time(kube_pod_container_resource_requests{k8s_container_name!="",k8s_node_name!="",k8s_pod_name!="",k8s_container_name!="",unit="integer"%s}[%s])) by (k8s_container_name,k8s_pod_name,k8s_namespace_name,k8s_node_name,resource,%s)`, pds.clusterFilterSuffix(), d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryIsGPUShared", end.Unix(), q)
	return source.NewFuture(source.DecodeIsGPUSharedResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryGPUInfo(start, end time.Time) *source.Future[source.GPUInfoResult] {
	// DCGM metrics not available via OTel receivers in this cluster.
	log.Warnf("QueryGPUInfo: DCGM_FI_DEV_DEC_UTIL not available via OTel receivers; returning empty result")
	return source.NewFutureFrom([]*source.GPUInfoResult{})
}

func (pds *PrometheusMetricsQuerier) QueryNodeCPUPricePerHr(start, end time.Time) *source.Future[source.NodeCPUPricePerHrResult] {
	// node_cpu_hourly_cost is an OpenCost output metric, not available as input.
	// Pricing falls back to cloud provider / CSV defaults.
	log.Warnf("QueryNodeCPUPricePerHr: node_cpu_hourly_cost not available (OpenCost output metric); returning empty")
	return source.NewFutureFrom([]*source.NodeCPUPricePerHrResult{})
}

func (pds *PrometheusMetricsQuerier) QueryNodeRAMPricePerGiBHr(start, end time.Time) *source.Future[source.NodeRAMPricePerGiBHrResult] {
	// node_ram_hourly_cost is an OpenCost output metric, not available as input.
	log.Warnf("QueryNodeRAMPricePerGiBHr: node_ram_hourly_cost not available (OpenCost output metric); returning empty")
	return source.NewFutureFrom([]*source.NodeRAMPricePerGiBHrResult{})
}

func (pds *PrometheusMetricsQuerier) QueryNodeGPUPricePerHr(start, end time.Time) *source.Future[source.NodeGPUPricePerHrResult] {
	// node_gpu_hourly_cost is an OpenCost output metric, not available as input.
	log.Warnf("QueryNodeGPUPricePerHr: node_gpu_hourly_cost not available (OpenCost output metric); returning empty")
	return source.NewFutureFrom([]*source.NodeGPUPricePerHrResult{})
}

func (pds *PrometheusMetricsQuerier) QueryNodeIsSpot(start, end time.Time) *source.Future[source.NodeIsSpotResult] {
	// kubecost_node_is_spot is an OpenCost output metric, not available as input.
	log.Warnf("QueryNodeIsSpot: kubecost_node_is_spot not available (OpenCost output metric); returning empty")
	return source.NewFutureFrom([]*source.NodeIsSpotResult{})
}

func (pds *PrometheusMetricsQuerier) QueryPodPVCAllocation(start, end time.Time) *source.Future[source.PodPVCAllocationResult] {
	// pod_pvc_allocation is an OpenCost output metric, not available as input.
	// PVC-to-Pod allocation is derived from kube_persistentvolumeclaim_info + kube_pod_* instead.
	log.Warnf("QueryPodPVCAllocation: pod_pvc_allocation is an OpenCost output metric, not available as input; returning empty")
	return source.NewFutureFrom([]*source.PodPVCAllocationResult{})
}

// QueryPVCBytesRequested returns PVC storage requests via KSM kube_persistentvolumeclaim_resource_requests_storage_bytes.
// Labels (OTel-native): k8s_cluster_name, k8s_namespace_name, k8s_persistentvolumeclaim_name.
func (pds *PrometheusMetricsQuerier) QueryPVCBytesRequested(start, end time.Time) *source.Future[source.PVCBytesRequestedResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(
		`avg(avg_over_time(kube_persistentvolumeclaim_resource_requests_storage_bytes{%s}[%s])) by (k8s_persistentvolumeclaim_name, k8s_namespace_name, %s)`,
		pds.clusterFilterSuffix(), d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryPVCBytesRequested", end.Unix(), q)
	return source.NewFuture(source.DecodePVCBytesRequestedResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryPVBytes returns PV capacity via KSM kube_persistentvolume_capacity_bytes.
// Labels (OTel-native): k8s_cluster_name, k8s_persistentvolume_name.
func (pds *PrometheusMetricsQuerier) QueryPVBytes(start, end time.Time) *source.Future[source.PVBytesResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(
		`avg(avg_over_time(kube_persistentvolume_capacity_bytes{%s}[%s])) by (k8s_persistentvolume_name, %s)`,
		pds.clusterFilterSuffix(), d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryPVBytes", end.Unix(), q)
	return source.NewFuture(source.DecodePVBytesResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryPVInfo returns PV metadata via KSM kube_persistentvolume_info.
// Labels (OTel-native): k8s_cluster_name, k8s_persistentvolume_name, k8s_storageclass_name,
// csi_driver, csi_volume_handle, reclaim_policy.
// provider_id is not present; omitted from by() clause.
func (pds *PrometheusMetricsQuerier) QueryPVInfo(start, end time.Time) *source.Future[source.PVInfoResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(
		`avg(avg_over_time(kube_persistentvolume_info{%s}[%s])) by (%s, k8s_storageclass_name, k8s_persistentvolume_name)`,
		pds.clusterFilterSuffix(), d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryPVInfo", end.Unix(), q)
	return source.NewFuture(source.DecodePVInfoResult, pds.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}
