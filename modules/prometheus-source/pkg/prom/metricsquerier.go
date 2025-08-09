package prom

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	prometheus "github.com/prometheus/client_golang/api"
)

//--------------------------------------------------------------------------
//  PrometheusMetricsQuerier
//--------------------------------------------------------------------------

// PrometheusMetricsQuerier is the implementation of the data source's MetricsQuerier interface for Prometheus.
type PrometheusMetricsQuerier struct {
	promConfig   *OpenCostPrometheusConfig
	promClient   prometheus.Client
	promContexts *ContextFactory
}

func newPrometheusMetricsQuerier(
	promConfig *OpenCostPrometheusConfig,
	promClient prometheus.Client,
	promContexts *ContextFactory,
) *PrometheusMetricsQuerier {
	return &PrometheusMetricsQuerier{
		promConfig:   promConfig,
		promClient:   promClient,
		promContexts: promContexts,
	}
}

func (pds *PrometheusMetricsQuerier) QueryPVPricePerGiBHour(start, end time.Time) *source.Future[source.PVPricePerGiBHourResult] {
	const pvCostQuery = `avg(avg_over_time(pv_hourly_cost{%s}[%s])) by (%s, persistentvolume, volumename, provider_id)`

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPVCost")
	}

	queryPVCost := fmt.Sprintf(pvCostQuery, pds.promConfig.ClusterFilter, durStr, pds.promConfig.ClusterLabel)

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodePVPricePerGiBHourResult, ctx.QueryAtTime(queryPVCost, end))
}

func (pds *PrometheusMetricsQuerier) QueryPVUsedAverage(start, end time.Time) *source.Future[source.PVUsedAvgResult] {
	// `avg(avg_over_time(kubelet_volume_stats_used_bytes{%s}[%s])) by (%s, persistentvolumeclaim, namespace)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	const pvUsedAverageQuery = `avg(avg_over_time(kubelet_volume_stats_used_bytes{%s}[%s])) by (%s, persistentvolumeclaim, namespace)`
	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPVUsedAverage")
	}

	queryPVUsedAvg := fmt.Sprintf(pvUsedAverageQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodePVUsedAvgResult, ctx.QueryAtTime(queryPVUsedAvg, end))
}

func (pds *PrometheusMetricsQuerier) QueryPVUsedMax(start, end time.Time) *source.Future[source.PVUsedMaxResult] {
	// `max(max_over_time(kubelet_volume_stats_used_bytes{%s}[%s])) by (%s, persistentvolumeclaim, namespace)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	const pvUsedMaxQuery = `max(max_over_time(kubelet_volume_stats_used_bytes{%s}[%s])) by (%s, persistentvolumeclaim, namespace)`
	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPVUsedMax")
	}

	queryPVUsedMax := fmt.Sprintf(pvUsedMaxQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodePVUsedMaxResult, ctx.QueryAtTime(queryPVUsedMax, end))
}

func (pds *PrometheusMetricsQuerier) QueryPVCInfo(start, end time.Time) *source.Future[source.PVCInfoResult] {
	const queryFmtPVCInfo = `avg(kube_persistentvolumeclaim_info{volumename != "", %s}) by (persistentvolumeclaim, storageclass, volumename, namespace, %s)[%s:%dm]`
	// env.GetPromClusterFilter(), env.GetPromClusterLabel(), durStr, resStr)

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution)
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPVCInfo")
	}

	queryPVCInfo := fmt.Sprintf(queryFmtPVCInfo, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodePVCInfoResult, ctx.QueryAtTime(queryPVCInfo, end))
}

func (pds *PrometheusMetricsQuerier) QueryPVActiveMinutes(start, end time.Time) *source.Future[source.PVActiveMinutesResult] {
	const pvActiveMinsQuery = `avg(kube_persistentvolume_capacity_bytes{%s}) by (%s, persistentvolume)[%s:%dm]`
	// env.GetPromClusterFilter(), env.GetPromClusterLabel(), durStr, minsPerResolution)

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution)
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPVActiveMinutes")
	}

	queryPVActiveMins := fmt.Sprintf(pvActiveMinsQuery, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodePVActiveMinutesResult, ctx.QueryAtTime(queryPVActiveMins, end))
}

func (pds *PrometheusMetricsQuerier) QueryLocalStorageCost(start, end time.Time) *source.Future[source.LocalStorageCostResult] {
	// `sum_over_time(sum(container_fs_limit_bytes{device=~"/dev/(nvme|sda).*", id="/", %s}) by (instance, device, %s)[%s:%dm]) / 1024 / 1024 / 1024 * %f * %f`
	// env.GetPromClusterFilter(), env.GetPromClusterLabel(), durStr, minsPerResolution, hourlyToCumulative, costPerGBHr)

	const localStorageCostQuery = `sum_over_time(sum(container_fs_limit_bytes{device=~"/dev/(nvme|sda).*", id="/", %s}) by (instance, device, %s)[%s:%dm]) / 1024 / 1024 / 1024 * %f * %f`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution)
	if durStr == "" {
		panic("failed to parse duration string passed to QueryLocalStorageCost")
	}

	// hourlyToCumulative is a scaling factor that, when multiplied by an
	// hourly value, converts it to a cumulative value; i.e. [$/hr] *
	// [min/res]*[hr/min] = [$/res]
	hourlyToCumulative := float64(minsPerResolution) * (1.0 / 60.0)
	costPerGBHr := 0.04 / 730.0

	queryLocalStorageCost := fmt.Sprintf(localStorageCostQuery, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution, hourlyToCumulative, costPerGBHr)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeLocalStorageCostResult, ctx.QueryAtTime(queryLocalStorageCost, end))
}

func (pds *PrometheusMetricsQuerier) QueryLocalStorageUsedCost(start, end time.Time) *source.Future[source.LocalStorageUsedCostResult] {
	// `sum_over_time(sum(container_fs_usage_bytes{device=~"/dev/(nvme|sda).*", id="/", %s}) by (instance, device, %s)[%s:%dm]) / 1024 / 1024 / 1024 * %f * %f`
	// env.GetPromClusterFilter(), env.GetPromClusterLabel(), durStr, minsPerResolution, hourlyToCumulative, costPerGBHr)

	const localStorageUsedCostQuery = `sum_over_time(sum(container_fs_usage_bytes{device=~"/dev/(nvme|sda).*", id="/", %s}) by (instance, device, %s)[%s:%dm]) / 1024 / 1024 / 1024 * %f * %f`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution)
	if durStr == "" {
		panic("failed to parse duration string passed to QueryLocalStorageUsedCost")
	}

	// hourlyToCumulative is a scaling factor that, when multiplied by an
	// hourly value, converts it to a cumulative value; i.e. [$/hr] *
	// [min/res]*[hr/min] = [$/res]
	hourlyToCumulative := float64(minsPerResolution) * (1.0 / 60.0)
	costPerGBHr := 0.04 / 730.0

	queryLocalStorageUsedCost := fmt.Sprintf(localStorageUsedCostQuery, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution, hourlyToCumulative, costPerGBHr)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeLocalStorageUsedCostResult, ctx.QueryAtTime(queryLocalStorageUsedCost, end))
}

func (pds *PrometheusMetricsQuerier) QueryLocalStorageUsedAvg(start, end time.Time) *source.Future[source.LocalStorageUsedAvgResult] {
	// `avg(sum(avg_over_time(container_fs_usage_bytes{device=~"/dev/(nvme|sda).*", id="/", %s}[%s])) by (instance, device, %s, job)) by (instance, device, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel(), env.GetPromClusterLabel())

	const localStorageUsedAvgQuery = `avg(sum(avg_over_time(container_fs_usage_bytes{device=~"/dev/(nvme|sda).*", id="/", %s}[%s])) by (instance, device, %s, job)) by (instance, device, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryLocalStorageUsedAvg")
	}

	queryLocalStorageUsedAvg := fmt.Sprintf(localStorageUsedAvgQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeLocalStorageUsedAvgResult, ctx.QueryAtTime(queryLocalStorageUsedAvg, end))
}

func (pds *PrometheusMetricsQuerier) QueryLocalStorageUsedMax(start, end time.Time) *source.Future[source.LocalStorageUsedMaxResult] {
	// `max(sum(max_over_time(container_fs_usage_bytes{device=~"/dev/(nvme|sda).*", id="/", %s}[%s])) by (instance, device, %s, job)) by (instance, device, %s)`
	//  env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel(), env.GetPromClusterLabel())
	const localStorageUsedMaxQuery = `max(sum(max_over_time(container_fs_usage_bytes{device=~"/dev/(nvme|sda).*", id="/", %s}[%s])) by (instance, device, %s, job)) by (instance, device, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryLocalStorageUsedMax")
	}

	queryLocalStorageUsedMax := fmt.Sprintf(localStorageUsedMaxQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeLocalStorageUsedMaxResult, ctx.QueryAtTime(queryLocalStorageUsedMax, end))
}

func (pds *PrometheusMetricsQuerier) QueryLocalStorageBytes(start, end time.Time) *source.Future[source.LocalStorageBytesResult] {
	// `avg_over_time(sum(container_fs_limit_bytes{device=~"/dev/(nvme|sda).*", id="/", %s}) by (instance, device, %s)[%s:%dm])`
	// env.GetPromClusterFilter(), env.GetPromClusterLabel(), durStr, minsPerResolution)

	const localStorageBytesQuery = `avg_over_time(sum(container_fs_limit_bytes{device=~"/dev/(nvme|sda).*", id="/", %s}) by (instance, device, %s)[%s:%dm])`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution)
	if durStr == "" {
		panic("failed to parse duration string passed to QueryLocalStorageBytes")
	}

	queryLocalStorageBytes := fmt.Sprintf(localStorageBytesQuery, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeLocalStorageBytesResult, ctx.QueryAtTime(queryLocalStorageBytes, end))
}

func (pds *PrometheusMetricsQuerier) QueryLocalStorageActiveMinutes(start, end time.Time) *source.Future[source.LocalStorageActiveMinutesResult] {
	// `count(node_total_hourly_cost{%s}) by (%s, node)[%s:%dm]`
	// env.GetPromClusterFilter(), env.GetPromClusterLabel(), durStr, minsPerResolution)

	const localStorageActiveMinutesQuery = `count(node_total_hourly_cost{%s}) by (%s, node, instance, provider_id)[%s:%dm]`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution)
	if durStr == "" {
		panic("failed to parse duration string passed to QueryLocalStorageActiveMinutes")
	}

	queryLocalStorageActiveMins := fmt.Sprintf(localStorageActiveMinutesQuery, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeLocalStorageActiveMinutesResult, ctx.QueryAtTime(queryLocalStorageActiveMins, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeCPUCoresCapacity(start, end time.Time) *source.Future[source.NodeCPUCoresCapacityResult] {
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())
	const nodeCPUCoresCapacityQuery = `avg(avg_over_time(kube_node_status_capacity_cpu_cores{%s}[%s])) by (%s, node)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeCPUCoresCapacity")
	}

	queryNodeCPUCoresCapacity := fmt.Sprintf(nodeCPUCoresCapacityQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeNodeCPUCoresCapacityResult, ctx.QueryAtTime(queryNodeCPUCoresCapacity, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeCPUCoresAllocatable(start, end time.Time) *source.Future[source.NodeCPUCoresAllocatableResult] {
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	const nodeCPUCoresAllocatableQuery = `avg(avg_over_time(kube_node_status_allocatable_cpu_cores{%s}[%s])) by (%s, node)`
	// `avg(avg_over_time(container_cpu_allocation{container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeCPUCoresAllocatable")
	}

	queryNodeCPUCoresAllocatable := fmt.Sprintf(nodeCPUCoresAllocatableQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeNodeCPUCoresAllocatableResult, ctx.QueryAtTime(queryNodeCPUCoresAllocatable, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeRAMBytesCapacity(start, end time.Time) *source.Future[source.NodeRAMBytesCapacityResult] {
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	const nodeRAMBytesCapacityQuery = `avg(avg_over_time(kube_node_status_capacity_memory_bytes{%s}[%s])) by (%s, node)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeRAMBytesCapacity")
	}

	queryNodeRAMBytesCapacity := fmt.Sprintf(nodeRAMBytesCapacityQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeNodeRAMBytesCapacityResult, ctx.QueryAtTime(queryNodeRAMBytesCapacity, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeRAMBytesAllocatable(start, end time.Time) *source.Future[source.NodeRAMBytesAllocatableResult] {
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	const nodeRAMBytesAllocatableQuery = `avg(avg_over_time(kube_node_status_allocatable_memory_bytes{%s}[%s])) by (%s, node)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeRAMBytesAllocatable")
	}

	queryNodeRAMBytesAllocatable := fmt.Sprintf(nodeRAMBytesAllocatableQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeNodeRAMBytesAllocatableResult, ctx.QueryAtTime(queryNodeRAMBytesAllocatable, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeGPUCount(start, end time.Time) *source.Future[source.NodeGPUCountResult] {
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	const nodeGPUCountQuery = `avg(avg_over_time(node_gpu_count{%s}[%s])) by (%s, node, provider_id)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeGPUCount")
	}

	queryNodeGPUCount := fmt.Sprintf(nodeGPUCountQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeNodeGPUCountResult, ctx.QueryAtTime(queryNodeGPUCount, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeLabels(start, end time.Time) *source.Future[source.NodeLabelsResult] {
	const labelsQuery = `avg_over_time(kube_node_labels{%s}[%s])`
	// env.GetPromClusterFilter(), durStr

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeLabels")
	}

	queryLabels := fmt.Sprintf(labelsQuery, cfg.ClusterFilter, durStr)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeNodeLabelsResult, ctx.QueryAtTime(queryLabels, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeActiveMinutes(start, end time.Time) *source.Future[source.NodeActiveMinutesResult] {
	// env.GetPromClusterFilter(), env.GetPromClusterLabel(), durStr, minsPerResolution)

	const activeMinsQuery = `avg(node_total_hourly_cost{%s}) by (node, %s, provider_id)[%s:%dm]`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution)
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeActiveMinutes")
	}

	queryActiveMins := fmt.Sprintf(activeMinsQuery, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeNodeActiveMinutesResult, ctx.QueryAtTime(queryActiveMins, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeCPUModeTotal(start, end time.Time) *source.Future[source.NodeCPUModeTotalResult] {
	// env.GetPromClusterFilter(), durStr, minsPerResolution, env.GetPromClusterLabel())

	const nodeCPUModeTotalQuery = `sum(rate(node_cpu_seconds_total{%s}[%s:%dm])) by (kubernetes_node, %s, mode)`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution)
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeCPUModeTotal")
	}

	queryCPUModeTotal := fmt.Sprintf(nodeCPUModeTotalQuery, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeNodeCPUModeTotalResult, ctx.QueryAtTime(queryCPUModeTotal, end))
}
func (pds *PrometheusMetricsQuerier) QueryNodeRAMSystemPercent(start, end time.Time) *source.Future[source.NodeRAMSystemPercentResult] {
	// env.GetPromClusterFilter(), durStr, minsPerResolution, env.GetPromClusterLabel(), env.GetPromClusterFilter(), durStr, minsPerResolution, env.GetPromClusterLabel(), env.GetPromClusterLabel())

	const nodeRAMSystemPctQuery = `sum(sum_over_time(container_memory_working_set_bytes{container_name!="POD",container_name!="",namespace="kube-system", %s}[%s:%dm])) by (instance, %s) / avg(label_replace(sum(sum_over_time(kube_node_status_capacity_memory_bytes{%s}[%s:%dm])) by (node, %s), "instance", "$1", "node", "(.*)")) by (instance, %s)`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution)
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeRAMSystemPercent")
	}

	queryRAMSystemPct := fmt.Sprintf(nodeRAMSystemPctQuery, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeNodeRAMSystemPercentResult, ctx.QueryAtTime(queryRAMSystemPct, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeRAMUserPercent(start, end time.Time) *source.Future[source.NodeRAMUserPercentResult] {
	// env.GetPromClusterFilter(), durStr, minsPerResolution, env.GetPromClusterLabel(), env.GetPromClusterFilter(), durStr, minsPerResolution, env.GetPromClusterLabel(), env.GetPromClusterLabel())

	const nodeRAMUserPctQuery = `sum(sum_over_time(container_memory_working_set_bytes{container_name!="POD",container_name!="",namespace!="kube-system", %s}[%s:%dm])) by (instance, %s) / avg(label_replace(sum(sum_over_time(kube_node_status_capacity_memory_bytes{%s}[%s:%dm])) by (node, %s), "instance", "$1", "node", "(.*)")) by (instance, %s)`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution)
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeRAMUserPercent")
	}

	queryRAMUserPct := fmt.Sprintf(nodeRAMUserPctQuery, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeNodeRAMUserPercentResult, ctx.QueryAtTime(queryRAMUserPct, end))
}

func (pds *PrometheusMetricsQuerier) QueryLBPricePerHr(start, end time.Time) *source.Future[source.LBPricePerHrResult] {
	const queryFmtLBCostPerHr = `avg(avg_over_time(kubecost_load_balancer_cost{%s}[%s])) by (namespace, service_name, ingress_ip, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryLBPricePerHr")
	}

	queryLBCostPerHr := fmt.Sprintf(queryFmtLBCostPerHr, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeLBPricePerHrResult, ctx.QueryAtTime(queryLBCostPerHr, end))
}

func (pds *PrometheusMetricsQuerier) QueryLBActiveMinutes(start, end time.Time) *source.Future[source.LBActiveMinutesResult] {
	const lbActiveMinutesQuery = `avg(kubecost_load_balancer_cost{%s}) by (namespace, service_name, %s, ingress_ip)[%s:%dm]`
	// env.GetPromClusterFilter(), env.GetPromClusterLabel(), durStr, minsPerResolution)

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution)
	if durStr == "" {
		panic("failed to parse duration string passed to QueryLBActiveMinutes")
	}

	queryLBActiveMins := fmt.Sprintf(lbActiveMinutesQuery, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeLBActiveMinutesResult, ctx.QueryAtTime(queryLBActiveMins, end))
}

func (pds *PrometheusMetricsQuerier) QueryClusterManagementDuration(start, end time.Time) *source.Future[source.ClusterManagementDurationResult] {
	const clusterManagementDurationQuery = `avg(kubecost_cluster_management_cost{%s}) by (%s, provisioner_name)[%s:%dm]`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution)
	if durStr == "" {
		panic("failed to parse duration string passed to QueryClusterManagementDuration")
	}

	queryClusterManagementDuration := fmt.Sprintf(clusterManagementDurationQuery, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeClusterManagementDurationResult, ctx.QueryAtTime(queryClusterManagementDuration, end))
}

func (pds *PrometheusMetricsQuerier) QueryClusterManagementPricePerHr(start, end time.Time) *source.Future[source.ClusterManagementPricePerHrResult] {
	const clusterManagementCostQuery = `avg(avg_over_time(kubecost_cluster_management_cost{%s}[%s])) by (%s, provisioner_name)`
	// env.GetPromClusterFilter(), durationStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryClusterManagementCost")
	}

	queryClusterManagementCost := fmt.Sprintf(clusterManagementCostQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeClusterManagementPricePerHrResult, ctx.QueryAtTime(queryClusterManagementCost, end))
}

// AllocationMetricQuerier

func (pds *PrometheusMetricsQuerier) QueryPods(start, end time.Time) *source.Future[source.PodsResult] {
	const queryFmtPods = `avg(kube_pod_container_status_running{%s} != 0) by (pod, namespace, %s)[%s:%dm]`
	// env.GetPromClusterFilter(), env.GetPromClusterLabel(), durStr, resStr)

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution)
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPods")
	}

	queryPods := fmt.Sprintf(queryFmtPods, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodePodsResult, ctx.QueryAtTime(queryPods, end))
}

func (pds *PrometheusMetricsQuerier) QueryPodsUID(start, end time.Time) *source.Future[source.PodsResult] {
	const queryFmtPodsUID = `avg(kube_pod_container_status_running{%s} != 0) by (pod, namespace, uid, %s)[%s:%dm]`
	// env.GetPromClusterFilter(), env.GetPromClusterLabel(), durStr, resStr)

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution)
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPodsUID")
	}

	queryPodsUID := fmt.Sprintf(queryFmtPodsUID, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodePodsResult, ctx.QueryAtTime(queryPodsUID, end))
}

func (pds *PrometheusMetricsQuerier) QueryRAMBytesAllocated(start, end time.Time) *source.Future[source.RAMBytesAllocatedResult] {
	const queryFmtRAMBytesAllocated = `avg(avg_over_time(container_memory_allocation_bytes{container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, %s, provider_id)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryRAMBytesAllocated")
	}

	queryRAMBytesAllocated := fmt.Sprintf(queryFmtRAMBytesAllocated, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeRAMBytesAllocatedResult, ctx.QueryAtTime(queryRAMBytesAllocated, end))
}

func (pds *PrometheusMetricsQuerier) QueryRAMRequests(start, end time.Time) *source.Future[source.RAMRequestsResult] {
	const queryFmtRAMRequests = `avg(avg_over_time(kube_pod_container_resource_requests{resource="memory", unit="byte", container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryRAMRequests")
	}

	queryRAMRequests := fmt.Sprintf(queryFmtRAMRequests, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeRAMRequestsResult, ctx.QueryAtTime(queryRAMRequests, end))
}

func (pds *PrometheusMetricsQuerier) QueryRAMUsageAvg(start, end time.Time) *source.Future[source.RAMUsageAvgResult] {
	const queryFmtRAMUsageAvg = `avg(avg_over_time(container_memory_working_set_bytes{container!="", container_name!="POD", container!="POD", %s}[%s])) by (container_name, container, pod_name, pod, namespace, node, instance, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryRAMUsageAvg")
	}

	queryRAMUsageAvg := fmt.Sprintf(queryFmtRAMUsageAvg, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeRAMUsageAvgResult, ctx.QueryAtTime(queryRAMUsageAvg, end))
}

func (pds *PrometheusMetricsQuerier) QueryRAMUsageMax(start, end time.Time) *source.Future[source.RAMUsageMaxResult] {
	const queryFmtRAMUsageMax = `max(max_over_time(container_memory_working_set_bytes{container!="", container_name!="POD", container!="POD", %s}[%s])) by (container_name, container, pod_name, pod, namespace, node, instance, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryRAMUsageMax")
	}

	queryRAMUsageMax := fmt.Sprintf(queryFmtRAMUsageMax, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeRAMUsageMaxResult, ctx.QueryAtTime(queryRAMUsageMax, end))
}

func (pds *PrometheusMetricsQuerier) QueryCPUCoresAllocated(start, end time.Time) *source.Future[source.CPUCoresAllocatedResult] {
	const queryFmtCPUCoresAllocated = `avg(avg_over_time(container_cpu_allocation{container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryCPUCoresAllocated")
	}

	queryCPUCoresAllocated := fmt.Sprintf(queryFmtCPUCoresAllocated, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeCPUCoresAllocatedResult, ctx.QueryAtTime(queryCPUCoresAllocated, end))
}

func (pds *PrometheusMetricsQuerier) QueryCPURequests(start, end time.Time) *source.Future[source.CPURequestsResult] {
	const queryFmtCPURequests = `avg(avg_over_time(kube_pod_container_resource_requests{resource="cpu", unit="core", container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryCPURequests")
	}

	queryCPURequests := fmt.Sprintf(queryFmtCPURequests, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeCPURequestsResult, ctx.QueryAtTime(queryCPURequests, end))
}

func (pds *PrometheusMetricsQuerier) QueryCPUUsageAvg(start, end time.Time) *source.Future[source.CPUUsageAvgResult] {
	const queryFmtCPUUsageAvg = `avg(rate(container_cpu_usage_seconds_total{container!="", container_name!="POD", container!="POD", %s}[%s])) by (container_name, container, pod_name, pod, namespace, node, instance, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryCPUUsageAvg")
	}

	queryCPUUsageAvg := fmt.Sprintf(queryFmtCPUUsageAvg, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeCPUUsageAvgResult, ctx.QueryAtTime(queryCPUUsageAvg, end))
}

func (pds *PrometheusMetricsQuerier) QueryCPUUsageMax(start, end time.Time) *source.Future[source.CPUUsageMaxResult] {
	// Because we use container_cpu_usage_seconds_total to calculate CPU usage
	// at any given "instant" of time, we need to use an irate or rate. To then
	// calculate a max (or any aggregation) we have to perform an aggregation
	// query on top of an instant-by-instant maximum. Prometheus supports this
	// type of query with a "subquery" [1], however it is reportedly expensive
	// to make such a query. By default, Kubecost's Prometheus config includes
	// a recording rule that keeps track of the instant-by-instant irate for CPU
	// usage. The metric in this query is created by that recording rule.
	//
	// [1] https://prometheus.io/blog/2019/01/28/subquery-support/
	//
	// If changing the name of the recording rule, make sure to update the
	// corresponding diagnostic query to avoid confusion.
	const queryFmtCPUUsageMaxRecordingRule = `max(max_over_time(kubecost_container_cpu_usage_irate{%s}[%s])) by (container_name, container, pod_name, pod, namespace, node, instance, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	// This is the subquery equivalent of the above recording rule query. It is
	// more expensive, but does not require the recording rule. It should be
	// used as a fallback query if the recording rule data does not exist.
	//
	// The parameter after the colon [:<thisone>] in the subquery affects the
	// resolution of the subquery.
	// The parameter after the metric ...{}[<thisone>] should be set to 2x
	// the resolution, to make sure the irate always has two points to query
	// in case the Prom scrape duration has been reduced to be equal to the
	// query resolution.
	const queryFmtCPUUsageMaxSubquery = `max(max_over_time(irate(container_cpu_usage_seconds_total{container!="POD", container!="", %s}[%dm])[%s:%dm])) by (container, pod_name, pod, namespace, node, instance, %s)`
	// env.GetPromClusterFilter(), doubleResStr, durStr, resStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryCPUUsageMax")
	}

	queryCPUUsageMaxRecordingRule := fmt.Sprintf(queryFmtCPUUsageMaxRecordingRule, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	resCPUUsageMaxRR := ctx.QueryAtTime(queryCPUUsageMaxRecordingRule, end)
	resCPUUsageMax, _ := resCPUUsageMaxRR.Await()

	if len(resCPUUsageMax) > 0 {
		return wrapResults(queryCPUUsageMaxRecordingRule, source.DecodeCPUUsageMaxResult, resCPUUsageMax)
	}

	minsPerResolution := cfg.DataResolutionMinutes

	durStr = pds.durationStringFor(start, end, minsPerResolution)
	if durStr == "" {
		panic("failed to parse duration string passed to QueryCPUUsageMax")
	}

	queryCPUUsageMaxSubquery := fmt.Sprintf(queryFmtCPUUsageMaxSubquery, cfg.ClusterFilter, 2*minsPerResolution, durStr, minsPerResolution, cfg.ClusterLabel)
	return source.NewFuture(source.DecodeCPUUsageMaxResult, ctx.QueryAtTime(queryCPUUsageMaxSubquery, end))
}

func (pds *PrometheusMetricsQuerier) QueryGPUsRequested(start, end time.Time) *source.Future[source.GPUsRequestedResult] {
	const queryFmtGPUsRequested = `avg(avg_over_time(kube_pod_container_resource_requests{resource="nvidia_com_gpu", container!="",container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryGPUsRequested")
	}

	queryGPUsRequested := fmt.Sprintf(queryFmtGPUsRequested, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeGPUsRequestedResult, ctx.QueryAtTime(queryGPUsRequested, end))
}

func (pds *PrometheusMetricsQuerier) QueryGPUsUsageAvg(start, end time.Time) *source.Future[source.GPUsUsageAvgResult] {
	const queryFmtGPUsUsageAvg = `avg(avg_over_time(DCGM_FI_PROF_GR_ENGINE_ACTIVE{container!=""}[%s])) by (container, pod, namespace, %s)`
	// durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryGPUsUsageAvg")
	}

	queryGPUsUsageAvg := fmt.Sprintf(queryFmtGPUsUsageAvg, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeGPUsUsageAvgResult, ctx.QueryAtTime(queryGPUsUsageAvg, end))
}

func (pds *PrometheusMetricsQuerier) QueryGPUsUsageMax(start, end time.Time) *source.Future[source.GPUsUsageMaxResult] {
	const queryFmtGPUsUsageMax = `max(max_over_time(DCGM_FI_PROF_GR_ENGINE_ACTIVE{container!=""}[%s])) by (container, pod, namespace, %s)`
	// durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryGPUsUsageMax")
	}

	queryGPUsUsageMax := fmt.Sprintf(queryFmtGPUsUsageMax, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeGPUsUsageMaxResult, ctx.QueryAtTime(queryGPUsUsageMax, end))
}

func (pds *PrometheusMetricsQuerier) QueryGPUsAllocated(start, end time.Time) *source.Future[source.GPUsAllocatedResult] {
	const queryFmtGPUsAllocated = `avg(avg_over_time(container_gpu_allocation{container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryGPUsAllocated")
	}

	queryGPUsAllocated := fmt.Sprintf(queryFmtGPUsAllocated, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeGPUsAllocatedResult, ctx.QueryAtTime(queryGPUsAllocated, end))
}

func (pds *PrometheusMetricsQuerier) QueryIsGPUShared(start, end time.Time) *source.Future[source.IsGPUSharedResult] {
	const queryFmtIsGPUShared = `avg(avg_over_time(kube_pod_container_resource_requests{container!="", node != "", pod != "", container!= "", unit = "integer",  %s}[%s])) by (container, pod, namespace, node, resource, %s)`
	// env.GetPromClusterFilter(), durStr

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryIsGPUShared")
	}

	queryIsGPUShared := fmt.Sprintf(queryFmtIsGPUShared, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeIsGPUSharedResult, ctx.QueryAtTime(queryIsGPUShared, end))
}

func (pds *PrometheusMetricsQuerier) QueryGPUInfo(start, end time.Time) *source.Future[source.GPUInfoResult] {
	const queryFmtGetGPUInfo = `avg(avg_over_time(DCGM_FI_DEV_DEC_UTIL{container!="",%s}[%s])) by (container, pod, namespace, device, modelName, UUID, %s)`
	// env.GetPromClusterFilter(), durStr

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryGPUInfo")
	}

	queryGetGPUInfo := fmt.Sprintf(queryFmtGetGPUInfo, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeGPUInfoResult, ctx.QueryAtTime(queryGetGPUInfo, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeCPUPricePerHr(start, end time.Time) *source.Future[source.NodeCPUPricePerHrResult] {
	const queryFmtNodeCostPerCPUHr = `avg(avg_over_time(node_cpu_hourly_cost{%s}[%s])) by (node, %s, instance_type, provider_id)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeCPUPricePerHr")
	}

	queryNodeCostPerCPUHr := fmt.Sprintf(queryFmtNodeCostPerCPUHr, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeNodeCPUPricePerHrResult, ctx.QueryAtTime(queryNodeCostPerCPUHr, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeRAMPricePerGiBHr(start, end time.Time) *source.Future[source.NodeRAMPricePerGiBHrResult] {
	const queryFmtNodeCostPerRAMGiBHr = `avg(avg_over_time(node_ram_hourly_cost{%s}[%s])) by (node, %s, instance_type, provider_id)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeRAMPricePerGiBHr")
	}

	queryNodeCostPerRAMGiBHr := fmt.Sprintf(queryFmtNodeCostPerRAMGiBHr, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeNodeRAMPricePerGiBHrResult, ctx.QueryAtTime(queryNodeCostPerRAMGiBHr, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeGPUPricePerHr(start, end time.Time) *source.Future[source.NodeGPUPricePerHrResult] {
	const queryFmtNodeCostPerGPUHr = `avg(avg_over_time(node_gpu_hourly_cost{%s}[%s])) by (node, %s, instance_type, provider_id)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeGPUPricePerHr")
	}

	queryNodeCostPerGPUHr := fmt.Sprintf(queryFmtNodeCostPerGPUHr, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeNodeGPUPricePerHrResult, ctx.QueryAtTime(queryNodeCostPerGPUHr, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeIsSpot(start, end time.Time) *source.Future[source.NodeIsSpotResult] {
	const queryFmtNodeIsSpot = `avg_over_time(kubecost_node_is_spot{%s}[%s])`
	//`avg_over_time(kubecost_node_is_spot{%s}[%s:%dm])`
	// env.GetPromClusterFilter(), durStr)

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeIsSpot2")
	}

	queryNodeIsSpot := fmt.Sprintf(queryFmtNodeIsSpot, cfg.ClusterFilter, durStr)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeNodeIsSpotResult, ctx.QueryAtTime(queryNodeIsSpot, end))
}

func (pds *PrometheusMetricsQuerier) QueryPodPVCAllocation(start, end time.Time) *source.Future[source.PodPVCAllocationResult] {
	const queryFmtPodPVCAllocation = `avg(avg_over_time(pod_pvc_allocation{%s}[%s])) by (persistentvolume, persistentvolumeclaim, pod, namespace, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPodPVCAllocation")
	}

	queryPodPVCAllocation := fmt.Sprintf(queryFmtPodPVCAllocation, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodePodPVCAllocationResult, ctx.QueryAtTime(queryPodPVCAllocation, end))
}

func (pds *PrometheusMetricsQuerier) QueryPVCBytesRequested(start, end time.Time) *source.Future[source.PVCBytesRequestedResult] {
	const queryFmtPVCBytesRequested = `avg(avg_over_time(kube_persistentvolumeclaim_resource_requests_storage_bytes{%s}[%s])) by (persistentvolumeclaim, namespace, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPVCBytesRequested")
	}

	queryPVCBytesRequested := fmt.Sprintf(queryFmtPVCBytesRequested, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodePVCBytesRequestedResult, ctx.QueryAtTime(queryPVCBytesRequested, end))
}

func (pds *PrometheusMetricsQuerier) QueryPVBytes(start, end time.Time) *source.Future[source.PVBytesResult] {
	const queryFmtPVBytes = `avg(avg_over_time(kube_persistentvolume_capacity_bytes{%s}[%s])) by (persistentvolume, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPVBytes")
	}

	queryPVBytes := fmt.Sprintf(queryFmtPVBytes, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodePVBytesResult, ctx.QueryAtTime(queryPVBytes, end))
}

func (pds *PrometheusMetricsQuerier) QueryPVInfo(start, end time.Time) *source.Future[source.PVInfoResult] {
	const queryFmtPVMeta = `avg(avg_over_time(kubecost_pv_info{%s}[%s])) by (%s, storageclass, persistentvolume, provider_id)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPVMeta")
	}

	queryPVMeta := fmt.Sprintf(queryFmtPVMeta, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodePVInfoResult, ctx.QueryAtTime(queryPVMeta, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetZoneGiB(start, end time.Time) *source.Future[source.NetZoneGiBResult] {
	const queryFmtNetZoneGiB = `sum(increase(kubecost_pod_network_egress_bytes_total{internet="false", same_zone="false", same_region="true", %s}[%s:%dm])) by (pod_name, namespace, %s) / 1024 / 1024 / 1024`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution)
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNetZoneGiB")
	}

	queryNetZoneGiB := fmt.Sprintf(queryFmtNetZoneGiB, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeNetZoneGiBResult, ctx.QueryAtTime(queryNetZoneGiB, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetZonePricePerGiB(start, end time.Time) *source.Future[source.NetZonePricePerGiBResult] {
	const queryFmtNetZoneCostPerGiB = `avg(avg_over_time(kubecost_network_zone_egress_cost{%s}[%s])) by (%s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNetZonePricePerGiB")
	}

	queryNetZoneCostPerGiB := fmt.Sprintf(queryFmtNetZoneCostPerGiB, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeNetZonePricePerGiBResult, ctx.QueryAtTime(queryNetZoneCostPerGiB, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetRegionGiB(start, end time.Time) *source.Future[source.NetRegionGiBResult] {
	const queryFmtNetRegionGiB = `sum(increase(kubecost_pod_network_egress_bytes_total{internet="false", same_zone="false", same_region="false", %s}[%s:%dm])) by (pod_name, namespace, %s) / 1024 / 1024 / 1024`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution)
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNetRegionGiB")
	}

	queryNetRegionGiB := fmt.Sprintf(queryFmtNetRegionGiB, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeNetRegionGiBResult, ctx.QueryAtTime(queryNetRegionGiB, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetRegionPricePerGiB(start, end time.Time) *source.Future[source.NetRegionPricePerGiBResult] {
	const queryFmtNetRegionCostPerGiB = `avg(avg_over_time(kubecost_network_region_egress_cost{%s}[%s])) by (%s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNetRegionPricePerGiB")
	}

	queryNetRegionCostPerGiB := fmt.Sprintf(queryFmtNetRegionCostPerGiB, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeNetRegionPricePerGiBResult, ctx.QueryAtTime(queryNetRegionCostPerGiB, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetInternetGiB(start, end time.Time) *source.Future[source.NetInternetGiBResult] {
	const queryFmtNetInternetGiB = `sum(increase(kubecost_pod_network_egress_bytes_total{internet="true", %s}[%s:%dm])) by (pod_name, namespace, %s) / 1024 / 1024 / 1024`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution)
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNetInternetGiB")
	}

	queryNetInternetGiB := fmt.Sprintf(queryFmtNetInternetGiB, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeNetInternetGiBResult, ctx.QueryAtTime(queryNetInternetGiB, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetInternetPricePerGiB(start, end time.Time) *source.Future[source.NetInternetPricePerGiBResult] {
	const queryFmtNetInternetCostPerGiB = `avg(avg_over_time(kubecost_network_internet_egress_cost{%s}[%s])) by (%s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNetInternetPricePerGiB")
	}

	queryNetInternetCostPerGiB := fmt.Sprintf(queryFmtNetInternetCostPerGiB, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeNetInternetPricePerGiBResult, ctx.QueryAtTime(queryNetInternetCostPerGiB, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetInternetServiceGiB(start, end time.Time) *source.Future[source.NetInternetServiceGiBResult] {
	const queryFmtNetInternetGiB = `sum(increase(kubecost_pod_network_egress_bytes_total{internet="true", %s}[%s:%dm])) by (pod_name, namespace, service, %s) / 1024 / 1024 / 1024`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution)
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNetInternetGiB")
	}

	queryNetInternetGiB := fmt.Sprintf(queryFmtNetInternetGiB, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(NetworkInsightsContextName)
	return source.NewFuture(source.DecodeNetInternetServiceGiBResult, ctx.QueryAtTime(queryNetInternetGiB, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetTransferBytes(start, end time.Time) *source.Future[source.NetTransferBytesResult] {
	const queryFmtNetTransferBytes = `sum(increase(container_network_transmit_bytes_total{pod!="", %s}[%s:%dm])) by (pod_name, pod, namespace, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution)
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNetTransferBytes")
	}

	queryNetTransferBytes := fmt.Sprintf(queryFmtNetTransferBytes, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeNetTransferBytesResult, ctx.QueryAtTime(queryNetTransferBytes, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetZoneIngressGiB(start, end time.Time) *source.Future[source.NetZoneIngressGiBResult] {
	const queryFmtIngNetZoneGiB = `sum(increase(kubecost_pod_network_ingress_bytes_total{internet="false", same_zone="false", same_region="true", %s}[%s:%dm])) by (pod_name, namespace, %s) / 1024 / 1024 / 1024`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution)
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNetZoneIngressGiB")
	}

	queryNetZoneCostPerGiB := fmt.Sprintf(queryFmtIngNetZoneGiB, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(NetworkInsightsContextName)
	return source.NewFuture(source.DecodeNetZoneIngressGiBResult, ctx.QueryAtTime(queryNetZoneCostPerGiB, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetRegionIngressGiB(start, end time.Time) *source.Future[source.NetRegionIngressGiBResult] {
	const queryFmtIngNetRegionGiB = `sum(increase(kubecost_pod_network_ingress_bytes_total{internet="false", same_zone="false", same_region="false", %s}[%s:%dm])) by (pod_name, namespace, %s) / 1024 / 1024 / 1024`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution)
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNetRegionIngressGiB")
	}

	queryNetRegionIngGiB := fmt.Sprintf(queryFmtIngNetRegionGiB, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(NetworkInsightsContextName)
	return source.NewFuture(source.DecodeNetRegionIngressGiBResult, ctx.QueryAtTime(queryNetRegionIngGiB, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetInternetIngressGiB(start, end time.Time) *source.Future[source.NetInternetIngressGiBResult] {
	const queryFmtNetIngInternetGiB = `sum(increase(kubecost_pod_network_ingress_bytes_total{internet="true", %s}[%s:%dm])) by (pod_name, namespace, %s) / 1024 / 1024 / 1024`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution)
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNetInternetIngressGiB")
	}

	queryNetIngInternetGiB := fmt.Sprintf(queryFmtNetIngInternetGiB, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(NetworkInsightsContextName)
	return source.NewFuture(source.DecodeNetInternetIngressGiBResult, ctx.QueryAtTime(queryNetIngInternetGiB, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetInternetServiceIngressGiB(start, end time.Time) *source.Future[source.NetInternetServiceIngressGiBResult] {
	const queryFmtIngNetInternetGiB = `sum(increase(kubecost_pod_network_ingress_bytes_total{internet="true", %s}[%s:%dm])) by (pod_name, namespace, service, %s) / 1024 / 1024 / 1024`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel()

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution)
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNetInternetServiceIngressGiB")
	}

	queryNetIngInternetGiB := fmt.Sprintf(queryFmtIngNetInternetGiB, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(NetworkInsightsContextName)
	return source.NewFuture(source.DecodeNetInternetServiceIngressGiBResult, ctx.QueryAtTime(queryNetIngInternetGiB, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetReceiveBytes(start, end time.Time) *source.Future[source.NetReceiveBytesResult] {
	const queryFmtNetReceiveBytes = `sum(increase(container_network_receive_bytes_total{pod!="", %s}[%s:%dm])) by (pod_name, pod, namespace, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution)
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNetReceiveBytes")
	}

	queryNetReceiveBytes := fmt.Sprintf(queryFmtNetReceiveBytes, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeNetReceiveBytesResult, ctx.QueryAtTime(queryNetReceiveBytes, end))
}

func (pds *PrometheusMetricsQuerier) QueryNamespaceLabels(start, end time.Time) *source.Future[source.NamespaceLabelsResult] {
	const queryFmtNamespaceLabels = `avg_over_time(kube_namespace_labels{%s}[%s])`
	// env.GetPromClusterFilter(), durStr

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNamespaceLabels")
	}

	queryNamespaceLabels := fmt.Sprintf(queryFmtNamespaceLabels, cfg.ClusterFilter, durStr)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeNamespaceLabelsResult, ctx.QueryAtTime(queryNamespaceLabels, end))
}

func (pds *PrometheusMetricsQuerier) QueryNamespaceAnnotations(start, end time.Time) *source.Future[source.NamespaceAnnotationsResult] {
	const queryFmtNamespaceAnnotations = `avg_over_time(kube_namespace_annotations{%s}[%s])`
	// env.GetPromClusterFilter(), durStr

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNamespaceAnnotations")
	}

	queryNamespaceAnnotations := fmt.Sprintf(queryFmtNamespaceAnnotations, cfg.ClusterFilter, durStr)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeNamespaceAnnotationsResult, ctx.QueryAtTime(queryNamespaceAnnotations, end))
}

func (pds *PrometheusMetricsQuerier) QueryPodLabels(start, end time.Time) *source.Future[source.PodLabelsResult] {
	const queryFmtPodLabels = `avg_over_time(kube_pod_labels{%s}[%s])`
	// env.GetPromClusterFilter(), durStr

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPodLabels")
	}

	queryPodLabels := fmt.Sprintf(queryFmtPodLabels, cfg.ClusterFilter, durStr)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodePodLabelsResult, ctx.QueryAtTime(queryPodLabels, end))
}

func (pds *PrometheusMetricsQuerier) QueryPodAnnotations(start, end time.Time) *source.Future[source.PodAnnotationsResult] {
	const queryFmtPodAnnotations = `avg_over_time(kube_pod_annotations{%s}[%s])`
	// env.GetPromClusterFilter(), durStr

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPodAnnotations")
	}

	queryPodAnnotations := fmt.Sprintf(queryFmtPodAnnotations, cfg.ClusterFilter, durStr)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodePodAnnotationsResult, ctx.QueryAtTime(queryPodAnnotations, end))
}

func (pds *PrometheusMetricsQuerier) QueryServiceLabels(start, end time.Time) *source.Future[source.ServiceLabelsResult] {
	const queryFmtServiceLabels = `avg_over_time(service_selector_labels{%s}[%s])`
	// env.GetPromClusterFilter(), durStr

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryServiceLabels")
	}

	queryServiceLabels := fmt.Sprintf(queryFmtServiceLabels, cfg.ClusterFilter, durStr)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeServiceLabelsResult, ctx.QueryAtTime(queryServiceLabels, end))
}

func (pds *PrometheusMetricsQuerier) QueryDeploymentLabels(start, end time.Time) *source.Future[source.DeploymentLabelsResult] {
	const queryFmtDeploymentLabels = `avg_over_time(deployment_match_labels{%s}[%s])`
	// env.GetPromClusterFilter(), durStr

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNamespaceAnnotations")
	}

	queryDeploymentLabels := fmt.Sprintf(queryFmtDeploymentLabels, cfg.ClusterFilter, durStr)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeDeploymentLabelsResult, ctx.QueryAtTime(queryDeploymentLabels, end))
}

func (pds *PrometheusMetricsQuerier) QueryStatefulSetLabels(start, end time.Time) *source.Future[source.StatefulSetLabelsResult] {
	const queryFmtStatefulSetLabels = `avg_over_time(statefulSet_match_labels{%s}[%s])`
	// env.GetPromClusterFilter(), durStr

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryStatefulSetLabels")
	}

	queryStatefulSetLabels := fmt.Sprintf(queryFmtStatefulSetLabels, cfg.ClusterFilter, durStr)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeStatefulSetLabelsResult, ctx.QueryAtTime(queryStatefulSetLabels, end))
}

func (pds *PrometheusMetricsQuerier) QueryDaemonSetLabels(start, end time.Time) *source.Future[source.DaemonSetLabelsResult] {
	const queryFmtDaemonSetLabels = `sum(avg_over_time(kube_pod_owner{owner_kind="DaemonSet", %s}[%s])) by (pod, owner_name, namespace, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryDaemonSetLabels")
	}

	queryDaemonSetLabels := fmt.Sprintf(queryFmtDaemonSetLabels, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeDaemonSetLabelsResult, ctx.QueryAtTime(queryDaemonSetLabels, end))
}

func (pds *PrometheusMetricsQuerier) QueryJobLabels(start, end time.Time) *source.Future[source.JobLabelsResult] {
	const queryFmtJobLabels = `sum(avg_over_time(kube_pod_owner{owner_kind="Job", %s}[%s])) by (pod, owner_name, namespace ,%s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryJobLabels")
	}

	queryJobLabels := fmt.Sprintf(queryFmtJobLabels, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeJobLabelsResult, ctx.QueryAtTime(queryJobLabels, end))
}

func (pds *PrometheusMetricsQuerier) QueryPodsWithReplicaSetOwner(start, end time.Time) *source.Future[source.PodsWithReplicaSetOwnerResult] {
	const queryFmtPodsWithReplicaSetOwner = `sum(avg_over_time(kube_pod_owner{owner_kind="ReplicaSet", %s}[%s])) by (pod, owner_name, namespace ,%s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryPodsWithReplicaSetOwner")
	}

	queryPodsWithReplicaSetOwner := fmt.Sprintf(queryFmtPodsWithReplicaSetOwner, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodePodsWithReplicaSetOwnerResult, ctx.QueryAtTime(queryPodsWithReplicaSetOwner, end))
}

func (pds *PrometheusMetricsQuerier) QueryReplicaSetsWithoutOwners(start, end time.Time) *source.Future[source.ReplicaSetsWithoutOwnersResult] {
	const queryFmtReplicaSetsWithoutOwners = `avg(avg_over_time(kube_replicaset_owner{owner_kind="<none>", owner_name="<none>", %s}[%s])) by (replicaset, namespace, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryReplicaSetsWithoutOwners")
	}

	queryReplicaSetsWithoutOwners := fmt.Sprintf(queryFmtReplicaSetsWithoutOwners, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeReplicaSetsWithoutOwnersResult, ctx.QueryAtTime(queryReplicaSetsWithoutOwners, end))
}

func (pds *PrometheusMetricsQuerier) QueryReplicaSetsWithRollout(start, end time.Time) *source.Future[source.ReplicaSetsWithRolloutResult] {
	const queryFmtReplicaSetsWithRolloutOwner = `avg(avg_over_time(kube_replicaset_owner{owner_kind="Rollout", %s}[%s])) by (replicaset, namespace, owner_kind, owner_name, %s)`
	// env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryReplicaSetsWithRollout")
	}

	queryReplicaSetsWithRolloutOwner := fmt.Sprintf(queryFmtReplicaSetsWithRolloutOwner, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeReplicaSetsWithRolloutResult, ctx.QueryAtTime(queryReplicaSetsWithRolloutOwner, end))
}

func (pds *PrometheusMetricsQuerier) QueryDataCoverage(limitDays int) (time.Time, time.Time, error) {
	const (
		queryFmtOldestSample = `min_over_time(timestamp(group(node_cpu_hourly_cost{%s}))[%s:%s])`
		queryFmtNewestSample = `max_over_time(timestamp(group(node_cpu_hourly_cost{%s}))[%s:%s])`
	)

	cfg := pds.promConfig
	minutesPerDuration := 60
	dur := time.Duration(limitDays) * timeutil.Day
	end := time.Now().UTC().Truncate(timeutil.Day).Add(timeutil.Day)
	start := end.Add(-dur)

	durStr := pds.durationStringFor(start, end, minutesPerDuration)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	queryOldest := fmt.Sprintf(queryFmtOldestSample, cfg.ClusterFilter, durStr, "1h")
	resOldestFut := ctx.QueryAtTime(queryOldest, end)

	resOldest, err := resOldestFut.Await()
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("querying oldest sample: %w", err)
	}
	if len(resOldest) == 0 || len(resOldest[0].Values) == 0 {
		return time.Time{}, time.Time{}, fmt.Errorf("querying oldest sample: %w", err)
	}

	oldest := time.Unix(int64(resOldest[0].Values[0].Value), 0)

	queryNewest := fmt.Sprintf(queryFmtNewestSample, cfg.ClusterFilter, durStr, "1h")
	resNewestFut := ctx.QueryAtTime(queryNewest, end)

	resNewest, err := resNewestFut.Await()
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("querying newest sample: %w", err)
	}
	if len(resNewest) == 0 || len(resNewest[0].Values) == 0 {
		return time.Time{}, time.Time{}, fmt.Errorf("querying newest sample: %w", err)
	}

	newest := time.Unix(int64(resNewest[0].Values[0].Value), 0)

	return oldest, newest, nil
}

func (pds *PrometheusMetricsQuerier) durationStringFor(start, end time.Time, minsPerResolution int) string {
	dur := end.Sub(start)

	// If using a version of Prometheus where the resolution needs duration offset,
	// we need to apply that here.
	//
	// E.g. avg(node_total_hourly_cost{}) by (node, provider_id)[60m:5m] with
	// time=01:00:00 will return, for a node running the entire time, 12
	// timestamps where the first is 00:05:00 and the last is 01:00:00.
	// However, OpenCost expects for there to be 13 timestamps where the first
	// begins at 00:00:00. To achieve this, we must modify our query to
	// avg(node_total_hourly_cost{}) by (node, provider_id)[65m:5m]
	if pds.promConfig.IsOffsetResolution {
		// increase the query time by the resolution
		dur = dur + (time.Duration(minsPerResolution) * time.Minute)
	}

	return timeutil.DurationString(dur)
}

func newEmptyResult[T any](decoder source.ResultDecoder[T]) *source.Future[T] {
	ch := make(source.QueryResultsChan)
	go func() {
		results := source.NewQueryResults("")
		ch <- results
	}()

	return source.NewFuture(decoder, ch)
}

func wrapResults[T any](query string, decoder source.ResultDecoder[T], results []*source.QueryResult) *source.Future[T] {
	ch := make(source.QueryResultsChan)

	go func() {
		r := source.NewQueryResults(query)
		r.Results = results
		ch <- r
	}()

	return source.NewFuture(decoder, ch)
}
