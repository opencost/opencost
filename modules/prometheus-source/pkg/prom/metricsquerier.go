package prom

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	prometheus "github.com/prometheus/client_golang/api"
)

//--------------------------------------------------------------------------
//  PrometheusMetricsQuerier
//--------------------------------------------------------------------------

// PrometheusMetricsQueryLogFormat is the log format used to log metric queries before being sent to the prometheus
// instance
const PrometheusMetricsQueryLogFormat = `[PrometheusMetricsQuerier][%s][At Time: %d]: %s`

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
	const queryName = "QueryPVPricePerGiBHour"
	const pvCostQuery = `avg(avg_over_time(pv_hourly_cost{%s}[%s])) by (%s, persistentvolume, volumename, uid, provider_id)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryPVCost := fmt.Sprintf(pvCostQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryPVCost)

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodePVPricePerGiBHourResult, ctx.QueryAtTime(queryPVCost, end))
}

func (pds *PrometheusMetricsQuerier) QueryPVUsedAverage(start, end time.Time) *source.Future[source.PVUsedAvgResult] {
	const queryName = "QueryPVUsedAverage"
	const pvUsedAverageQuery = `avg(avg_over_time(kubelet_volume_stats_used_bytes{%s}[%s])) by (%s, persistentvolumeclaim, namespace, uid)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryPVUsedAvg := fmt.Sprintf(pvUsedAverageQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryPVUsedAvg)

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodePVUsedAvgResult, ctx.QueryAtTime(queryPVUsedAvg, end))
}

func (pds *PrometheusMetricsQuerier) QueryPVUsedMax(start, end time.Time) *source.Future[source.PVUsedMaxResult] {
	const queryName = "QueryPVUsedMax"
	const pvUsedMaxQuery = `max(max_over_time(kubelet_volume_stats_used_bytes{%s}[%s])) by (%s, persistentvolumeclaim, namespace, uid)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryPVUsedMax := fmt.Sprintf(pvUsedMaxQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryPVUsedMax)

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodePVUsedMaxResult, ctx.QueryAtTime(queryPVUsedMax, end))
}

func (pds *PrometheusMetricsQuerier) QueryPVCInfo(start, end time.Time) *source.Future[source.PVCInfoResult] {
	const queryName = "QueryPVCInfo"
	const queryFmtPVCInfo = `avg(kube_persistentvolumeclaim_info{volumename != "", %s}) by (persistentvolumeclaim, storageclass, volumename, namespace, uid, %s)[%s:%dm]`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, false)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryPVCInfo := fmt.Sprintf(queryFmtPVCInfo, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryPVCInfo)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodePVCInfoResult, ctx.QueryAtTime(queryPVCInfo, end))
}

func (pds *PrometheusMetricsQuerier) QueryPVActiveMinutes(start, end time.Time) *source.Future[source.PVActiveMinutesResult] {
	const queryName = "QueryPVActiveMinutes"
	const pvActiveMinsQuery = `avg(kube_persistentvolume_capacity_bytes{%s}) by (%s, persistentvolume, uid)[%s:%dm]`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, false)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryPVActiveMins := fmt.Sprintf(pvActiveMinsQuery, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryPVActiveMins)

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodePVActiveMinutesResult, ctx.QueryAtTime(queryPVActiveMins, end))
}

func (pds *PrometheusMetricsQuerier) QueryLocalStorageCost(start, end time.Time) *source.Future[source.LocalStorageCostResult] {
	const queryName = "QueryLocalStorageCost"
	const localStorageCostQuery = `sum_over_time(sum(container_fs_limit_bytes{device=~"/dev/(nvme|sda).*", id="/", %s}) by (instance, device, uid, %s)[%s:%dm]) / 1024 / 1024 / 1024 * %f * %f`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, false)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	// hourlyToCumulative is a scaling factor that, when multiplied by an
	// hourly value, converts it to a cumulative value; i.e. [$/hr] *
	// [min/res]*[hr/min] = [$/res]
	hourlyToCumulative := float64(minsPerResolution) * (1.0 / 60.0)
	costPerGBHr := 0.04 / 730.0

	queryLocalStorageCost := fmt.Sprintf(localStorageCostQuery, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution, hourlyToCumulative, costPerGBHr)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryLocalStorageCost)

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeLocalStorageCostResult, ctx.QueryAtTime(queryLocalStorageCost, end))
}

func (pds *PrometheusMetricsQuerier) QueryLocalStorageUsedCost(start, end time.Time) *source.Future[source.LocalStorageUsedCostResult] {
	const queryName = "QueryLocalStorageUsedCost"
	const localStorageUsedCostQuery = `sum_over_time(sum(container_fs_usage_bytes{device=~"/dev/(nvme|sda).*", id="/", %s}) by (instance, device, uid, %s)[%s:%dm]) / 1024 / 1024 / 1024 * %f * %f`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, false)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	// hourlyToCumulative is a scaling factor that, when multiplied by an
	// hourly value, converts it to a cumulative value; i.e. [$/hr] *
	// [min/res]*[hr/min] = [$/res]
	hourlyToCumulative := float64(minsPerResolution) * (1.0 / 60.0)
	costPerGBHr := 0.04 / 730.0

	queryLocalStorageUsedCost := fmt.Sprintf(localStorageUsedCostQuery, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution, hourlyToCumulative, costPerGBHr)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryLocalStorageUsedCost)

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeLocalStorageUsedCostResult, ctx.QueryAtTime(queryLocalStorageUsedCost, end))
}

func (pds *PrometheusMetricsQuerier) QueryLocalStorageUsedAvg(start, end time.Time) *source.Future[source.LocalStorageUsedAvgResult] {
	const queryName = "QueryLocalStorageUsedAvg"
	const localStorageUsedAvgQuery = `avg(sum(avg_over_time(container_fs_usage_bytes{device=~"/dev/(nvme|sda).*", id="/", %s}[%s])) by (instance, device, uid, %s, job)) by (instance, device, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryLocalStorageUsedAvg := fmt.Sprintf(localStorageUsedAvgQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryLocalStorageUsedAvg)

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeLocalStorageUsedAvgResult, ctx.QueryAtTime(queryLocalStorageUsedAvg, end))
}

func (pds *PrometheusMetricsQuerier) QueryLocalStorageUsedMax(start, end time.Time) *source.Future[source.LocalStorageUsedMaxResult] {
	const queryName = "QueryLocalStorageUsedMax"
	const localStorageUsedMaxQuery = `max(sum(max_over_time(container_fs_usage_bytes{device=~"/dev/(nvme|sda).*", id="/", %s}[%s])) by (instance, device, uid, %s, job)) by (instance, device, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryLocalStorageUsedMax")
	}

	queryLocalStorageUsedMax := fmt.Sprintf(localStorageUsedMaxQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryLocalStorageUsedMax)

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeLocalStorageUsedMaxResult, ctx.QueryAtTime(queryLocalStorageUsedMax, end))
}

func (pds *PrometheusMetricsQuerier) QueryLocalStorageBytes(start, end time.Time) *source.Future[source.LocalStorageBytesResult] {
	const queryName = "QueryLocalStorageBytes"
	const localStorageBytesQuery = `avg_over_time(sum(container_fs_limit_bytes{device=~"/dev/(nvme|sda).*", id="/", %s}) by (instance, device, uid, %s)[%s:%dm])`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, false)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryLocalStorageBytes := fmt.Sprintf(localStorageBytesQuery, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryLocalStorageBytes)

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeLocalStorageBytesResult, ctx.QueryAtTime(queryLocalStorageBytes, end))
}

func (pds *PrometheusMetricsQuerier) QueryLocalStorageActiveMinutes(start, end time.Time) *source.Future[source.LocalStorageActiveMinutesResult] {
	const queryName = "QueryLocalStorageActiveMinutes"
	const localStorageActiveMinutesQuery = `count(node_total_hourly_cost{%s}) by (%s, node, uid, instance, provider_id)[%s:%dm]`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, false)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryLocalStorageActiveMins := fmt.Sprintf(localStorageActiveMinutesQuery, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryLocalStorageActiveMins)

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeLocalStorageActiveMinutesResult, ctx.QueryAtTime(queryLocalStorageActiveMins, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeCPUCoresCapacity(start, end time.Time) *source.Future[source.NodeCPUCoresCapacityResult] {
	const queryName = "QueryNodeCPUCoresCapacity"
	const nodeCPUCoresCapacityQuery = `avg(avg_over_time(kube_node_status_capacity_cpu_cores{%s}[%s])) by (%s, node, uid)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryNodeCPUCoresCapacity := fmt.Sprintf(nodeCPUCoresCapacityQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryNodeCPUCoresCapacity)

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeNodeCPUCoresCapacityResult, ctx.QueryAtTime(queryNodeCPUCoresCapacity, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeCPUCoresAllocatable(start, end time.Time) *source.Future[source.NodeCPUCoresAllocatableResult] {
	const queryName = "QueryNodeCPUCoresAllocatable"
	const nodeCPUCoresAllocatableQuery = `avg(avg_over_time(kube_node_status_allocatable_cpu_cores{%s}[%s])) by (%s, node, uid)`
	// `avg(avg_over_time(container_cpu_allocation{container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryNodeCPUCoresAllocatable := fmt.Sprintf(nodeCPUCoresAllocatableQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryNodeCPUCoresAllocatable)

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeNodeCPUCoresAllocatableResult, ctx.QueryAtTime(queryNodeCPUCoresAllocatable, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeRAMBytesCapacity(start, end time.Time) *source.Future[source.NodeRAMBytesCapacityResult] {
	const queryName = "QueryNodeRAMBytesCapacity"
	const nodeRAMBytesCapacityQuery = `avg(avg_over_time(kube_node_status_capacity_memory_bytes{%s}[%s])) by (%s, node, uid)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryNodeRAMBytesCapacity := fmt.Sprintf(nodeRAMBytesCapacityQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryNodeRAMBytesCapacity)

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeNodeRAMBytesCapacityResult, ctx.QueryAtTime(queryNodeRAMBytesCapacity, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeRAMBytesAllocatable(start, end time.Time) *source.Future[source.NodeRAMBytesAllocatableResult] {
	const queryName = "QueryNodeRAMBytesAllocatable"
	const nodeRAMBytesAllocatableQuery = `avg(avg_over_time(kube_node_status_allocatable_memory_bytes{%s}[%s])) by (%s, node, uid)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryNodeRAMBytesAllocatable := fmt.Sprintf(nodeRAMBytesAllocatableQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryNodeRAMBytesAllocatable)

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeNodeRAMBytesAllocatableResult, ctx.QueryAtTime(queryNodeRAMBytesAllocatable, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeGPUCount(start, end time.Time) *source.Future[source.NodeGPUCountResult] {
	const queryName = "QueryNodeGPUCount"
	const nodeGPUCountQuery = `avg(avg_over_time(node_gpu_count{%s}[%s])) by (%s, node, uid, provider_id)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryNodeGPUCount := fmt.Sprintf(nodeGPUCountQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryNodeGPUCount)

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeNodeGPUCountResult, ctx.QueryAtTime(queryNodeGPUCount, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeLabels(start, end time.Time) *source.Future[source.NodeLabelsResult] {
	const queryName = "QueryNodeLabels"
	const labelsQuery = `avg_over_time(kube_node_labels{%s}[%s])`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryLabels := fmt.Sprintf(labelsQuery, cfg.ClusterFilter, durStr)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryLabels)

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeNodeLabelsResult, ctx.QueryAtTime(queryLabels, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeActiveMinutes(start, end time.Time) *source.Future[source.NodeActiveMinutesResult] {
	const queryName = "QueryNodeActiveMinutes"
	const activeMinsQuery = `avg(node_total_hourly_cost{%s}) by (node, uid, %s, provider_id)[%s:%dm]`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, false)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryActiveMins := fmt.Sprintf(activeMinsQuery, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryActiveMins)

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeNodeActiveMinutesResult, ctx.QueryAtTime(queryActiveMins, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeCPUModeTotal(start, end time.Time) *source.Future[source.NodeCPUModeTotalResult] {
	const queryName = "QueryNodeCPUModeTotal"
	const nodeCPUModeTotalQuery = `sum(rate(node_cpu_seconds_total{%s}[%s:%dm])) by (kubernetes_node, uid, %s, mode)`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, true)
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNodeCPUModeTotal")
	}

	queryCPUModeTotal := fmt.Sprintf(nodeCPUModeTotalQuery, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryCPUModeTotal)

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeNodeCPUModeTotalResult, ctx.QueryAtTime(queryCPUModeTotal, end))
}
func (pds *PrometheusMetricsQuerier) QueryNodeRAMSystemPercent(start, end time.Time) *source.Future[source.NodeRAMSystemPercentResult] {
	const queryName = "QueryNodeRAMSystemPercent"
	const nodeRAMSystemPctQuery = `sum(sum_over_time(container_memory_working_set_bytes{container_name!="POD",container_name!="",namespace="kube-system", %s}[%s:%dm])) by (instance, uid, %s) / avg(label_replace(sum(sum_over_time(kube_node_status_capacity_memory_bytes{%s}[%s:%dm])) by (node, uid, %s), "instance", "$1", "node", "(.*)")) by (instance, uid, %s)`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, false)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryRAMSystemPct := fmt.Sprintf(nodeRAMSystemPctQuery, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryRAMSystemPct)

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeNodeRAMSystemPercentResult, ctx.QueryAtTime(queryRAMSystemPct, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeRAMUserPercent(start, end time.Time) *source.Future[source.NodeRAMUserPercentResult] {
	const queryName = "QueryNodeRAMUserPercent"
	const nodeRAMUserPctQuery = `sum(sum_over_time(container_memory_working_set_bytes{container_name!="POD",container_name!="",namespace!="kube-system", %s}[%s:%dm])) by (instance, uid, %s) / avg(label_replace(sum(sum_over_time(kube_node_status_capacity_memory_bytes{%s}[%s:%dm])) by (node, uid, %s), "instance", "$1", "node", "(.*)")) by (instance, uid, %s)`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, false)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryRAMUserPct := fmt.Sprintf(nodeRAMUserPctQuery, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryRAMUserPct)

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeNodeRAMUserPercentResult, ctx.QueryAtTime(queryRAMUserPct, end))
}

func (pds *PrometheusMetricsQuerier) QueryLBPricePerHr(start, end time.Time) *source.Future[source.LBPricePerHrResult] {
	const queryName = "QueryLBPricePerHr"
	const queryFmtLBCostPerHr = `avg(avg_over_time(kubecost_load_balancer_cost{%s}[%s])) by (namespace, service_name, ingress_ip, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryLBCostPerHr := fmt.Sprintf(queryFmtLBCostPerHr, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryLBCostPerHr)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeLBPricePerHrResult, ctx.QueryAtTime(queryLBCostPerHr, end))
}

func (pds *PrometheusMetricsQuerier) QueryLBActiveMinutes(start, end time.Time) *source.Future[source.LBActiveMinutesResult] {
	const queryName = "QueryLBActiveMinutes"
	const lbActiveMinutesQuery = `avg(kubecost_load_balancer_cost{%s}) by (namespace, service_name, uid, %s, ingress_ip)[%s:%dm]`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, false)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryLBActiveMins := fmt.Sprintf(lbActiveMinutesQuery, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryLBActiveMins)

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeLBActiveMinutesResult, ctx.QueryAtTime(queryLBActiveMins, end))
}

// Note: cluster_info is not currently emitted
func (pds *PrometheusMetricsQuerier) QueryClusterUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	const queryName = "QueryClusterUptime"
	const queryFmtClusterUptime = `avg(cluster_info{%s}) by (%s, uid)[%s:%dm]`

	cfg := pds.promConfig

	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, false)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryClusterUptime := fmt.Sprintf(queryFmtClusterUptime, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryFmtClusterUptime)

	ctx := pds.promContexts.NewNamedContext(KubeModelContextName)
	return source.NewFuture(source.DecodeUptimeResult, ctx.QueryAtTime(queryClusterUptime, end))
}

func (pds *PrometheusMetricsQuerier) QueryClusterManagementDuration(start, end time.Time) *source.Future[source.ClusterManagementDurationResult] {
	const queryName = "QueryClusterManagementDuration"
	const clusterManagementDurationQuery = `avg(kubecost_cluster_management_cost{%s}) by (%s, provisioner_name)[%s:%dm]`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, false)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryClusterManagementDuration := fmt.Sprintf(clusterManagementDurationQuery, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryClusterManagementDuration)

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeClusterManagementDurationResult, ctx.QueryAtTime(queryClusterManagementDuration, end))
}

func (pds *PrometheusMetricsQuerier) QueryClusterManagementPricePerHr(start, end time.Time) *source.Future[source.ClusterManagementPricePerHrResult] {
	const queryName = "QueryClusterManagementPricePerHr"
	const clusterManagementCostQuery = `avg(avg_over_time(kubecost_cluster_management_cost{%s}[%s])) by (%s, provisioner_name)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryClusterManagementCost := fmt.Sprintf(clusterManagementCostQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryClusterManagementCost)

	ctx := pds.promContexts.NewNamedContext(ClusterContextName)
	return source.NewFuture(source.DecodeClusterManagementPricePerHrResult, ctx.QueryAtTime(queryClusterManagementCost, end))
}

// AllocationMetricQuerier

func (pds *PrometheusMetricsQuerier) QueryPods(start, end time.Time) *source.Future[source.PodsResult] {
	const queryName = "QueryPods"
	const queryFmtPods = `avg(kube_pod_container_status_running{%s} != 0) by (pod, namespace, uid, %s)[%s:%dm]`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, false)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryPods := fmt.Sprintf(queryFmtPods, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryPods)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodePodsResult, ctx.QueryAtTime(queryPods, end))
}

func (pds *PrometheusMetricsQuerier) QueryPodsUID(start, end time.Time) *source.Future[source.PodsResult] {
	const queryName = "QueryPodsUID"
	const queryFmtPodsUID = `avg(kube_pod_container_status_running{%s} != 0) by (pod, namespace, uid, %s)[%s:%dm]`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, false)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryPodsUID := fmt.Sprintf(queryFmtPodsUID, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryPodsUID)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodePodsResult, ctx.QueryAtTime(queryPodsUID, end))
}

func (pds *PrometheusMetricsQuerier) QueryRAMBytesAllocated(start, end time.Time) *source.Future[source.RAMBytesAllocatedResult] {
	const queryName = "QueryRAMBytesAllocated"
	const queryFmtRAMBytesAllocated = `avg(avg_over_time(container_memory_allocation_bytes{container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, uid, %s, provider_id)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryRAMBytesAllocated := fmt.Sprintf(queryFmtRAMBytesAllocated, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryRAMBytesAllocated)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeRAMBytesAllocatedResult, ctx.QueryAtTime(queryRAMBytesAllocated, end))
}

func (pds *PrometheusMetricsQuerier) QueryRAMRequests(start, end time.Time) *source.Future[source.RAMRequestsResult] {
	const queryName = "QueryRAMRequests"
	const queryFmtRAMRequests = `avg(avg_over_time(kube_pod_container_resource_requests{resource="memory", unit="byte", container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryRAMRequests := fmt.Sprintf(queryFmtRAMRequests, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryRAMRequests)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeRAMRequestsResult, ctx.QueryAtTime(queryRAMRequests, end))
}

func (pds *PrometheusMetricsQuerier) QueryRAMLimits(start, end time.Time) *source.Future[source.RAMLimitsResult] {
	const queryName = "QueryRAMLimits"
	const queryFmtRAMLimits = `avg(avg_over_time(kube_pod_container_resource_limits{resource="memory", unit="byte", container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryRAMLimits := fmt.Sprintf(queryFmtRAMLimits, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryRAMLimits)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeRAMLimitsResult, ctx.QueryAtTime(queryRAMLimits, end))
}

func (pds *PrometheusMetricsQuerier) QueryRAMUsageAvg(start, end time.Time) *source.Future[source.RAMUsageAvgResult] {
	const queryName = "QueryRAMUsageAvg"
	const queryFmtRAMUsageAvg = `avg(avg_over_time(container_memory_working_set_bytes{container!="", container_name!="POD", container!="POD", %s}[%s])) by (container_name, container, pod_name, pod, namespace, node, instance, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryRAMUsageAvg := fmt.Sprintf(queryFmtRAMUsageAvg, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryRAMUsageAvg)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeRAMUsageAvgResult, ctx.QueryAtTime(queryRAMUsageAvg, end))
}

func (pds *PrometheusMetricsQuerier) QueryRAMUsageMax(start, end time.Time) *source.Future[source.RAMUsageMaxResult] {
	const queryName = "QueryRAMUsageMax"
	const queryFmtRAMUsageMax = `max(max_over_time(container_memory_working_set_bytes{container!="", container_name!="POD", container!="POD", %s}[%s])) by (container_name, container, pod_name, pod, namespace, node, instance, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryRAMUsageMax := fmt.Sprintf(queryFmtRAMUsageMax, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryRAMUsageMax)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeRAMUsageMaxResult, ctx.QueryAtTime(queryRAMUsageMax, end))
}

func (pds *PrometheusMetricsQuerier) QueryCPUCoresAllocated(start, end time.Time) *source.Future[source.CPUCoresAllocatedResult] {
	const queryName = "QueryCPUCoresAllocated"
	const queryFmtCPUCoresAllocated = `avg(avg_over_time(container_cpu_allocation{container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryCPUCoresAllocated := fmt.Sprintf(queryFmtCPUCoresAllocated, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryCPUCoresAllocated)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeCPUCoresAllocatedResult, ctx.QueryAtTime(queryCPUCoresAllocated, end))
}

func (pds *PrometheusMetricsQuerier) QueryCPURequests(start, end time.Time) *source.Future[source.CPURequestsResult] {
	const queryName = "QueryCPURequests"
	const queryFmtCPURequests = `avg(avg_over_time(kube_pod_container_resource_requests{resource="cpu", unit="core", container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryCPURequests := fmt.Sprintf(queryFmtCPURequests, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryCPURequests)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeCPURequestsResult, ctx.QueryAtTime(queryCPURequests, end))
}

func (pds *PrometheusMetricsQuerier) QueryCPULimits(start, end time.Time) *source.Future[source.CPULimitsResult] {
	const queryName = "QueryCPULimits"
	const queryFmtCPULimits = `avg(avg_over_time(kube_pod_container_resource_limits{resource="cpu", unit="core", container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryCPULimits := fmt.Sprintf(queryFmtCPULimits, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryCPULimits)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeCPULimitsResult, ctx.QueryAtTime(queryCPULimits, end))
}

func (pds *PrometheusMetricsQuerier) QueryCPUUsageAvg(start, end time.Time) *source.Future[source.CPUUsageAvgResult] {
	const queryName = "QueryCPUUsageAvg"
	const queryFmtCPUUsageAvg = `avg(rate(container_cpu_usage_seconds_total{container!="", container_name!="POD", container!="POD", %s}[%s])) by (container_name, container, pod_name, pod, namespace, node, instance, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryCPUUsageAvg := fmt.Sprintf(queryFmtCPUUsageAvg, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryCPUUsageAvg)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeCPUUsageAvgResult, ctx.QueryAtTime(queryCPUUsageAvg, end))
}

func (pds *PrometheusMetricsQuerier) QueryCPUUsageMax(start, end time.Time) *source.Future[source.CPUUsageMaxResult] {
	const queryName = "QueryCPUUsageMax"
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
	const queryFmtCPUUsageMaxRecordingRule = `max(max_over_time(kubecost_container_cpu_usage_irate{%s}[%s])) by (container_name, container, pod_name, pod, namespace, node, instance, uid, %s)`

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
	const queryFmtCPUUsageMaxSubquery = `max(max_over_time(irate(container_cpu_usage_seconds_total{container!="POD", container!="", %s}[%dm])[%s:%dm])) by (container, pod_name, pod, namespace, node, instance, uid, %s)`

	cfg := pds.promConfig
	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryCPUUsageMaxRecordingRule := fmt.Sprintf(queryFmtCPUUsageMaxRecordingRule, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryCPUUsageMaxRecordingRule)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	resCPUUsageMaxRR := ctx.QueryAtTime(queryCPUUsageMaxRecordingRule, end)
	resCPUUsageMax, _ := resCPUUsageMaxRR.Await()

	if len(resCPUUsageMax) > 0 {
		return source.NewFutureFrom(source.DecodeAll(resCPUUsageMax, source.DecodeCPUUsageMaxResult))
	}

	minsPerResolution := cfg.DataResolutionMinutes

	durStr = pds.durationStringFor(start, end, minsPerResolution, false)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryCPUUsageMaxSubquery := fmt.Sprintf(queryFmtCPUUsageMaxSubquery, cfg.ClusterFilter, 2*minsPerResolution, durStr, minsPerResolution, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryCPUUsageMaxSubquery)

	return source.NewFuture(source.DecodeCPUUsageMaxResult, ctx.QueryAtTime(queryCPUUsageMaxSubquery, end))
}

func (pds *PrometheusMetricsQuerier) QueryGPUsRequested(start, end time.Time) *source.Future[source.GPUsRequestedResult] {
	const queryName = "QueryGPUsRequested"
	const queryFmtGPUsRequested = `avg(avg_over_time(kube_pod_container_resource_requests{resource="nvidia_com_gpu", container!="",container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryGPUsRequested := fmt.Sprintf(queryFmtGPUsRequested, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryGPUsRequested)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeGPUsRequestedResult, ctx.QueryAtTime(queryGPUsRequested, end))
}

func (pds *PrometheusMetricsQuerier) QueryGPUsUsageAvg(start, end time.Time) *source.Future[source.GPUsUsageAvgResult] {
	const queryName = "QueryGPUsUsageAvg"
	const queryFmtGPUsUsageAvg = `avg(avg_over_time(DCGM_FI_PROF_GR_ENGINE_ACTIVE{container!=""}[%s])) by (container, pod, namespace, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryGPUsUsageAvg := fmt.Sprintf(queryFmtGPUsUsageAvg, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryGPUsUsageAvg)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeGPUsUsageAvgResult, ctx.QueryAtTime(queryGPUsUsageAvg, end))
}

func (pds *PrometheusMetricsQuerier) QueryGPUsUsageMax(start, end time.Time) *source.Future[source.GPUsUsageMaxResult] {
	const queryName = "QueryGPUsUsageMax"
	const queryFmtGPUsUsageMax = `max(max_over_time(DCGM_FI_PROF_GR_ENGINE_ACTIVE{container!=""}[%s])) by (container, pod, namespace, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryGPUsUsageMax := fmt.Sprintf(queryFmtGPUsUsageMax, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryGPUsUsageMax)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeGPUsUsageMaxResult, ctx.QueryAtTime(queryGPUsUsageMax, end))
}

func (pds *PrometheusMetricsQuerier) QueryGPUsAllocated(start, end time.Time) *source.Future[source.GPUsAllocatedResult] {
	const queryName = "QueryGPUsAllocated"
	const queryFmtGPUsAllocated = `avg(avg_over_time(container_gpu_allocation{container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryGPUsAllocated := fmt.Sprintf(queryFmtGPUsAllocated, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryGPUsAllocated)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeGPUsAllocatedResult, ctx.QueryAtTime(queryGPUsAllocated, end))
}

func (pds *PrometheusMetricsQuerier) QueryIsGPUShared(start, end time.Time) *source.Future[source.IsGPUSharedResult] {
	const queryName = "QueryIsGPUShared"
	const queryFmtIsGPUShared = `avg(avg_over_time(kube_pod_container_resource_requests{container!="", node != "", pod != "", container!= "", unit = "integer",  %s}[%s])) by (container, pod, namespace, node, resource, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryIsGPUShared := fmt.Sprintf(queryFmtIsGPUShared, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryIsGPUShared)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeIsGPUSharedResult, ctx.QueryAtTime(queryIsGPUShared, end))
}

func (pds *PrometheusMetricsQuerier) QueryGPUInfo(start, end time.Time) *source.Future[source.GPUInfoResult] {
	const queryName = "QueryGPUInfo"
	const queryFmtGetGPUInfo = `avg(avg_over_time(DCGM_FI_DEV_DEC_UTIL{container!="",%s}[%s])) by (container, pod, namespace, device, modelName, UUID, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryGetGPUInfo := fmt.Sprintf(queryFmtGetGPUInfo, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryGetGPUInfo)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeGPUInfoResult, ctx.QueryAtTime(queryGetGPUInfo, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeCPUPricePerHr(start, end time.Time) *source.Future[source.NodeCPUPricePerHrResult] {
	const queryName = "QueryNodeCPUPricePerHr"
	const queryFmtNodeCostPerCPUHr = `avg(avg_over_time(node_cpu_hourly_cost{%s}[%s])) by (node, uid, %s, instance_type, provider_id)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryNodeCostPerCPUHr := fmt.Sprintf(queryFmtNodeCostPerCPUHr, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryNodeCostPerCPUHr)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeNodeCPUPricePerHrResult, ctx.QueryAtTime(queryNodeCostPerCPUHr, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeRAMPricePerGiBHr(start, end time.Time) *source.Future[source.NodeRAMPricePerGiBHrResult] {
	const queryName = "QueryNodeRAMPricePerGiBHr"
	const queryFmtNodeCostPerRAMGiBHr = `avg(avg_over_time(node_ram_hourly_cost{%s}[%s])) by (node, uid, %s, instance_type, provider_id)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryNodeCostPerRAMGiBHr := fmt.Sprintf(queryFmtNodeCostPerRAMGiBHr, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryNodeCostPerRAMGiBHr)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeNodeRAMPricePerGiBHrResult, ctx.QueryAtTime(queryNodeCostPerRAMGiBHr, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeGPUPricePerHr(start, end time.Time) *source.Future[source.NodeGPUPricePerHrResult] {
	const queryName = "QueryNodeGPUPricePerHr"
	const queryFmtNodeCostPerGPUHr = `avg(avg_over_time(node_gpu_hourly_cost{%s}[%s])) by (node, uid, %s, instance_type, provider_id)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryNodeCostPerGPUHr := fmt.Sprintf(queryFmtNodeCostPerGPUHr, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryNodeCostPerGPUHr)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeNodeGPUPricePerHrResult, ctx.QueryAtTime(queryNodeCostPerGPUHr, end))
}

func (pds *PrometheusMetricsQuerier) QueryNodeIsSpot(start, end time.Time) *source.Future[source.NodeIsSpotResult] {
	const queryName = "QueryNodeIsSpot"
	const queryFmtNodeIsSpot = `avg_over_time(kubecost_node_is_spot{%s}[%s])`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryNodeIsSpot := fmt.Sprintf(queryFmtNodeIsSpot, cfg.ClusterFilter, durStr)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryNodeIsSpot)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeNodeIsSpotResult, ctx.QueryAtTime(queryNodeIsSpot, end))
}

func (pds *PrometheusMetricsQuerier) QueryPodPVCAllocation(start, end time.Time) *source.Future[source.PodPVCAllocationResult] {
	const queryName = "QueryPodPVCAllocation"
	const queryFmtPodPVCAllocation = `avg(avg_over_time(pod_pvc_allocation{%s}[%s])) by (persistentvolume, persistentvolumeclaim, pod, namespace, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryPodPVCAllocation := fmt.Sprintf(queryFmtPodPVCAllocation, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryPodPVCAllocation)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodePodPVCAllocationResult, ctx.QueryAtTime(queryPodPVCAllocation, end))
}

func (pds *PrometheusMetricsQuerier) QueryPVCBytesRequested(start, end time.Time) *source.Future[source.PVCBytesRequestedResult] {
	const queryName = "QueryPVCBytesRequested"
	const queryFmtPVCBytesRequested = `avg(avg_over_time(kube_persistentvolumeclaim_resource_requests_storage_bytes{%s}[%s])) by (persistentvolumeclaim, namespace, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryPVCBytesRequested := fmt.Sprintf(queryFmtPVCBytesRequested, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryPVCBytesRequested)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodePVCBytesRequestedResult, ctx.QueryAtTime(queryPVCBytesRequested, end))
}

func (pds *PrometheusMetricsQuerier) QueryPVBytes(start, end time.Time) *source.Future[source.PVBytesResult] {
	const queryName = "QueryPVBytes"
	const queryFmtPVBytes = `avg(avg_over_time(kube_persistentvolume_capacity_bytes{%s}[%s])) by (persistentvolume, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryPVBytes := fmt.Sprintf(queryFmtPVBytes, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryPVBytes)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodePVBytesResult, ctx.QueryAtTime(queryPVBytes, end))
}

func (pds *PrometheusMetricsQuerier) QueryPVInfo(start, end time.Time) *source.Future[source.PVInfoResult] {
	const queryName = "QueryPVInfo"
	const queryFmtPVMeta = `avg(avg_over_time(kubecost_pv_info{%s}[%s])) by (%s, storageclass, persistentvolume, uid, provider_id)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryPVMeta := fmt.Sprintf(queryFmtPVMeta, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryPVMeta)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodePVInfoResult, ctx.QueryAtTime(queryPVMeta, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetZoneGiB(start, end time.Time) *source.Future[source.NetZoneGiBResult] {
	const queryName = "QueryNetZoneGiB"
	const queryFmtNetZoneGiB = `sum(increase(kubecost_pod_network_egress_bytes_total{internet="false", same_zone="false", same_region="true", %s}[%s:%dm])) by (pod_name, namespace, uid, %s) / 1024 / 1024 / 1024`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, true)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryNetZoneGiB := fmt.Sprintf(queryFmtNetZoneGiB, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryNetZoneGiB)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeNetZoneGiBResult, ctx.QueryAtTime(queryNetZoneGiB, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetZonePricePerGiB(start, end time.Time) *source.Future[source.NetZonePricePerGiBResult] {
	const queryName = "QueryNetZonePricePerGiB"
	const queryFmtNetZoneCostPerGiB = `avg(avg_over_time(kubecost_network_zone_egress_cost{%s}[%s])) by (%s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryNetZoneCostPerGiB := fmt.Sprintf(queryFmtNetZoneCostPerGiB, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryNetZoneCostPerGiB)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeNetZonePricePerGiBResult, ctx.QueryAtTime(queryNetZoneCostPerGiB, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetRegionGiB(start, end time.Time) *source.Future[source.NetRegionGiBResult] {
	const queryName = "QueryNetRegionGiB"
	const queryFmtNetRegionGiB = `sum(increase(kubecost_pod_network_egress_bytes_total{internet="false", same_zone="false", same_region="false", %s}[%s:%dm])) by (pod_name, namespace, uid, %s) / 1024 / 1024 / 1024`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, true)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryNetRegionGiB := fmt.Sprintf(queryFmtNetRegionGiB, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryNetRegionGiB)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeNetRegionGiBResult, ctx.QueryAtTime(queryNetRegionGiB, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetRegionPricePerGiB(start, end time.Time) *source.Future[source.NetRegionPricePerGiBResult] {
	const queryName = "QueryNetRegionPricePerGiB"
	const queryFmtNetRegionCostPerGiB = `avg(avg_over_time(kubecost_network_region_egress_cost{%s}[%s])) by (%s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryNetRegionPricePerGiB")
	}

	queryNetRegionCostPerGiB := fmt.Sprintf(queryFmtNetRegionCostPerGiB, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryNetRegionCostPerGiB)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeNetRegionPricePerGiBResult, ctx.QueryAtTime(queryNetRegionCostPerGiB, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetInternetGiB(start, end time.Time) *source.Future[source.NetInternetGiBResult] {
	const queryName = "QueryNetInternetGiB"
	const queryFmtNetInternetGiB = `sum(increase(kubecost_pod_network_egress_bytes_total{internet="true", %s}[%s:%dm])) by (pod_name, namespace, uid, %s) / 1024 / 1024 / 1024`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, true)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryNetInternetGiB := fmt.Sprintf(queryFmtNetInternetGiB, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryNetInternetGiB)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeNetInternetGiBResult, ctx.QueryAtTime(queryNetInternetGiB, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetInternetPricePerGiB(start, end time.Time) *source.Future[source.NetInternetPricePerGiBResult] {
	const queryName = "QueryNetInternetPricePerGiB"
	const queryFmtNetInternetCostPerGiB = `avg(avg_over_time(kubecost_network_internet_egress_cost{%s}[%s])) by (%s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryNetInternetCostPerGiB := fmt.Sprintf(queryFmtNetInternetCostPerGiB, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryNetInternetCostPerGiB)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeNetInternetPricePerGiBResult, ctx.QueryAtTime(queryNetInternetCostPerGiB, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetInternetServiceGiB(start, end time.Time) *source.Future[source.NetInternetServiceGiBResult] {
	const queryName = "QueryNetInternetServiceGiB"
	const queryFmtNetInternetGiB = `sum(increase(kubecost_pod_network_egress_bytes_total{internet="true", %s}[%s:%dm])) by (pod_name, namespace, service, uid, %s) / 1024 / 1024 / 1024`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, true)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryNetInternetGiB := fmt.Sprintf(queryFmtNetInternetGiB, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryNetInternetGiB)

	ctx := pds.promContexts.NewNamedContext(NetworkInsightsContextName)
	return source.NewFuture(source.DecodeNetInternetServiceGiBResult, ctx.QueryAtTime(queryNetInternetGiB, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetTransferBytes(start, end time.Time) *source.Future[source.NetTransferBytesResult] {
	const queryName = "QueryNetTransferBytes"
	const queryFmtNetTransferBytes = `sum(increase(container_network_transmit_bytes_total{pod!="", %s}[%s:%dm])) by (pod_name, pod, namespace, uid, %s)`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, true)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryNetTransferBytes := fmt.Sprintf(queryFmtNetTransferBytes, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryNetTransferBytes)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeNetTransferBytesResult, ctx.QueryAtTime(queryNetTransferBytes, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetZoneIngressGiB(start, end time.Time) *source.Future[source.NetZoneIngressGiBResult] {
	const queryName = "QueryNetZoneIngressGiB"
	const queryFmtIngNetZoneGiB = `sum(increase(kubecost_pod_network_ingress_bytes_total{internet="false", same_zone="false", same_region="true", %s}[%s:%dm])) by (pod_name, namespace, uid, %s) / 1024 / 1024 / 1024`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, true)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryNetZoneCostPerGiB := fmt.Sprintf(queryFmtIngNetZoneGiB, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryNetZoneCostPerGiB)

	ctx := pds.promContexts.NewNamedContext(NetworkInsightsContextName)
	return source.NewFuture(source.DecodeNetZoneIngressGiBResult, ctx.QueryAtTime(queryNetZoneCostPerGiB, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetRegionIngressGiB(start, end time.Time) *source.Future[source.NetRegionIngressGiBResult] {
	const queryName = "QueryNetRegionIngressGiB"
	const queryFmtIngNetRegionGiB = `sum(increase(kubecost_pod_network_ingress_bytes_total{internet="false", same_zone="false", same_region="false", %s}[%s:%dm])) by (pod_name, namespace, uid, %s) / 1024 / 1024 / 1024`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, true)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryNetRegionIngGiB := fmt.Sprintf(queryFmtIngNetRegionGiB, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryNetRegionIngGiB)

	ctx := pds.promContexts.NewNamedContext(NetworkInsightsContextName)
	return source.NewFuture(source.DecodeNetRegionIngressGiBResult, ctx.QueryAtTime(queryNetRegionIngGiB, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetInternetIngressGiB(start, end time.Time) *source.Future[source.NetInternetIngressGiBResult] {
	const queryName = "QueryNetInternetIngressGiB"
	const queryFmtNetIngInternetGiB = `sum(increase(kubecost_pod_network_ingress_bytes_total{internet="true", %s}[%s:%dm])) by (pod_name, namespace, uid, %s) / 1024 / 1024 / 1024`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, true)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryNetIngInternetGiB := fmt.Sprintf(queryFmtNetIngInternetGiB, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryNetIngInternetGiB)

	ctx := pds.promContexts.NewNamedContext(NetworkInsightsContextName)
	return source.NewFuture(source.DecodeNetInternetIngressGiBResult, ctx.QueryAtTime(queryNetIngInternetGiB, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetInternetServiceIngressGiB(start, end time.Time) *source.Future[source.NetInternetServiceIngressGiBResult] {
	const queryName = "QueryNetInternetServiceIngressGiB"
	const queryFmtIngNetInternetGiB = `sum(increase(kubecost_pod_network_ingress_bytes_total{internet="true", %s}[%s:%dm])) by (pod_name, namespace, service, uid, %s) / 1024 / 1024 / 1024`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, true)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryNetIngInternetGiB := fmt.Sprintf(queryFmtIngNetInternetGiB, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryNetIngInternetGiB)

	ctx := pds.promContexts.NewNamedContext(NetworkInsightsContextName)
	return source.NewFuture(source.DecodeNetInternetServiceIngressGiBResult, ctx.QueryAtTime(queryNetIngInternetGiB, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetReceiveBytes(start, end time.Time) *source.Future[source.NetReceiveBytesResult] {
	const queryName = "QueryNetReceiveBytes"
	const queryFmtNetReceiveBytes = `sum(increase(container_network_receive_bytes_total{pod!="", %s}[%s:%dm])) by (pod_name, pod, namespace, uid, %s)`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, true)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryNetReceiveBytes := fmt.Sprintf(queryFmtNetReceiveBytes, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryNetReceiveBytes)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeNetReceiveBytesResult, ctx.QueryAtTime(queryNetReceiveBytes, end))
}

// Note: namespace_info is not currently emitted
func (pds *PrometheusMetricsQuerier) QueryNamespaceUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	const queryName = "QueryNamespaceUptime"
	const queryFmtNamespaceUptime = `avg(namespace_info{%s}) by (%s, uid)[%s:%dm]`

	cfg := pds.promConfig

	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, false)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryNamespaceUptime := fmt.Sprintf(queryFmtNamespaceUptime, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryFmtNamespaceUptime)

	ctx := pds.promContexts.NewNamedContext(KubeModelContextName)
	return source.NewFuture(source.DecodeUptimeResult, ctx.QueryAtTime(queryNamespaceUptime, end))
}

func (pds *PrometheusMetricsQuerier) QueryNamespaceLabels(start, end time.Time) *source.Future[source.NamespaceLabelsResult] {
	const queryName = "QueryNamespaceLabels"
	const queryFmtNamespaceLabels = `avg_over_time(kube_namespace_labels{%s}[%s])`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryNamespaceLabels := fmt.Sprintf(queryFmtNamespaceLabels, cfg.ClusterFilter, durStr)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryNamespaceLabels)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeNamespaceLabelsResult, ctx.QueryAtTime(queryNamespaceLabels, end))
}

func (pds *PrometheusMetricsQuerier) QueryNamespaceAnnotations(start, end time.Time) *source.Future[source.NamespaceAnnotationsResult] {
	const queryName = "QueryNamespaceAnnotations"
	const queryFmtNamespaceAnnotations = `avg_over_time(kube_namespace_annotations{%s}[%s])`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryNamespaceAnnotations := fmt.Sprintf(queryFmtNamespaceAnnotations, cfg.ClusterFilter, durStr)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryNamespaceAnnotations)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeNamespaceAnnotationsResult, ctx.QueryAtTime(queryNamespaceAnnotations, end))
}

func (pds *PrometheusMetricsQuerier) QueryPodLabels(start, end time.Time) *source.Future[source.PodLabelsResult] {
	const queryName = "QueryPodLabels"
	const queryFmtPodLabels = `avg_over_time(kube_pod_labels{%s}[%s])`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryPodLabels := fmt.Sprintf(queryFmtPodLabels, cfg.ClusterFilter, durStr)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryPodLabels)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodePodLabelsResult, ctx.QueryAtTime(queryPodLabels, end))
}

func (pds *PrometheusMetricsQuerier) QueryPodAnnotations(start, end time.Time) *source.Future[source.PodAnnotationsResult] {
	const queryName = "QueryPodAnnotations"
	const queryFmtPodAnnotations = `avg_over_time(kube_pod_annotations{%s}[%s])`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryPodAnnotations := fmt.Sprintf(queryFmtPodAnnotations, cfg.ClusterFilter, durStr)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryPodAnnotations)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodePodAnnotationsResult, ctx.QueryAtTime(queryPodAnnotations, end))
}

func (pds *PrometheusMetricsQuerier) QueryServiceLabels(start, end time.Time) *source.Future[source.ServiceLabelsResult] {
	const queryName = "QueryServiceLabels"
	const queryFmtServiceLabels = `avg_over_time(service_selector_labels{%s}[%s])`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryServiceLabels := fmt.Sprintf(queryFmtServiceLabels, cfg.ClusterFilter, durStr)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryServiceLabels)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeServiceLabelsResult, ctx.QueryAtTime(queryServiceLabels, end))
}

func (pds *PrometheusMetricsQuerier) QueryDeploymentLabels(start, end time.Time) *source.Future[source.DeploymentLabelsResult] {
	const queryName = "QueryDeploymentLabels"
	const queryFmtDeploymentLabels = `avg_over_time(deployment_match_labels{%s}[%s])`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryDeploymentLabels := fmt.Sprintf(queryFmtDeploymentLabels, cfg.ClusterFilter, durStr)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryDeploymentLabels)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeDeploymentLabelsResult, ctx.QueryAtTime(queryDeploymentLabels, end))
}

func (pds *PrometheusMetricsQuerier) QueryStatefulSetLabels(start, end time.Time) *source.Future[source.StatefulSetLabelsResult] {
	const queryName = "QueryStatefulSetLabels"
	const queryFmtStatefulSetLabels = `avg_over_time(statefulSet_match_labels{%s}[%s])`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryStatefulSetLabels := fmt.Sprintf(queryFmtStatefulSetLabels, cfg.ClusterFilter, durStr)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryStatefulSetLabels)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeStatefulSetLabelsResult, ctx.QueryAtTime(queryStatefulSetLabels, end))
}

func (pds *PrometheusMetricsQuerier) QueryDaemonSetLabels(start, end time.Time) *source.Future[source.DaemonSetLabelsResult] {
	const queryName = "QueryDaemonSetLabels"
	const queryFmtDaemonSetLabels = `sum(avg_over_time(kube_pod_owner{owner_kind="DaemonSet", %s}[%s])) by (pod, owner_name, namespace, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryDaemonSetLabels := fmt.Sprintf(queryFmtDaemonSetLabels, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryDaemonSetLabels)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeDaemonSetLabelsResult, ctx.QueryAtTime(queryDaemonSetLabels, end))
}

func (pds *PrometheusMetricsQuerier) QueryJobLabels(start, end time.Time) *source.Future[source.JobLabelsResult] {
	const queryName = "QueryJobLabels"
	const queryFmtJobLabels = `sum(avg_over_time(kube_pod_owner{owner_kind="Job", %s}[%s])) by (pod, owner_name, namespace, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryJobLabels := fmt.Sprintf(queryFmtJobLabels, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryJobLabels)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeJobLabelsResult, ctx.QueryAtTime(queryJobLabels, end))
}

func (pds *PrometheusMetricsQuerier) QueryPodsWithReplicaSetOwner(start, end time.Time) *source.Future[source.PodsWithReplicaSetOwnerResult] {
	const queryName = "QueryPodsWithReplicaSetOwner"
	const queryFmtPodsWithReplicaSetOwner = `sum(avg_over_time(kube_pod_owner{owner_kind="ReplicaSet", %s}[%s])) by (pod, owner_name, namespace, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryPodsWithReplicaSetOwner := fmt.Sprintf(queryFmtPodsWithReplicaSetOwner, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryPodsWithReplicaSetOwner)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodePodsWithReplicaSetOwnerResult, ctx.QueryAtTime(queryPodsWithReplicaSetOwner, end))
}

func (pds *PrometheusMetricsQuerier) QueryReplicaSetsWithoutOwners(start, end time.Time) *source.Future[source.ReplicaSetsWithoutOwnersResult] {
	const queryName = "QueryReplicaSetsWithoutOwners"
	const queryFmtReplicaSetsWithoutOwners = `avg(avg_over_time(kube_replicaset_owner{owner_kind="<none>", owner_name="<none>", %s}[%s])) by (replicaset, namespace, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryReplicaSetsWithoutOwners := fmt.Sprintf(queryFmtReplicaSetsWithoutOwners, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryReplicaSetsWithoutOwners)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeReplicaSetsWithoutOwnersResult, ctx.QueryAtTime(queryReplicaSetsWithoutOwners, end))
}

func (pds *PrometheusMetricsQuerier) QueryReplicaSetsWithRollout(start, end time.Time) *source.Future[source.ReplicaSetsWithRolloutResult] {
	const queryName = "QueryReplicaSetsWithRollout"
	const queryFmtReplicaSetsWithRolloutOwner = `avg(avg_over_time(kube_replicaset_owner{owner_kind="Rollout", %s}[%s])) by (replicaset, namespace, owner_kind, owner_name, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryReplicaSetsWithRolloutOwner := fmt.Sprintf(queryFmtReplicaSetsWithRolloutOwner, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryReplicaSetsWithRolloutOwner)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeReplicaSetsWithRolloutResult, ctx.QueryAtTime(queryReplicaSetsWithRolloutOwner, end))
}

// Note: The ResourceQuota metrics are _not_ emitted at the moment. Leaving the query implementations here in case we add metric emission later on.

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	const queryName = "QueryResourceQuotaUptime"
	const queryFmtResourceQuotaUptime = `avg(resourcequota_info{%s}) by (%s, uid)[%s:%dm]`

	cfg := pds.promConfig

	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, false)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaUptime := fmt.Sprintf(queryFmtResourceQuotaUptime, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryFmtResourceQuotaUptime)

	ctx := pds.promContexts.NewNamedContext(KubeModelContextName)
	return source.NewFuture(source.DecodeUptimeResult, ctx.QueryAtTime(queryResourceQuotaUptime, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaSpecCPURequestAverage(start, end time.Time) *source.Future[source.ResourceQuotaSpecCPURequestAvgResult] {
	const queryName = "QueryResourceQuotaSpecCPURequestAverage"
	const queryFmtResourceQuotaSpecCPURequests = `avg(avg_over_time(resourcequota_spec_resource_requests{resource="cpu",unit="core", %s}[%s])) by (resourcequota, namespace, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaSpecCPURequests := fmt.Sprintf(queryFmtResourceQuotaSpecCPURequests, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaSpecCPURequests)

	ctx := pds.promContexts.NewNamedContext(KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaSpecCPURequestAvgResult, ctx.QueryAtTime(queryResourceQuotaSpecCPURequests, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaSpecCPURequestMax(start, end time.Time) *source.Future[source.ResourceQuotaSpecCPURequestMaxResult] {
	const queryName = "QueryResourceQuotaSpecCPURequestMax"
	const queryFmtResourceQuotaSpecCPURequests = `max(max_over_time(resourcequota_spec_resource_requests{resource="cpu",unit="core", %s}[%s])) by (resourcequota, namespace, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaSpecCPURequests := fmt.Sprintf(queryFmtResourceQuotaSpecCPURequests, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaSpecCPURequests)

	ctx := pds.promContexts.NewNamedContext(KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaSpecCPURequestMaxResult, ctx.QueryAtTime(queryResourceQuotaSpecCPURequests, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaSpecRAMRequestAverage(start, end time.Time) *source.Future[source.ResourceQuotaSpecRAMRequestAvgResult] {
	const queryName = "QueryResourceQuotaSpecRAMRequestAverage"
	const queryFmtResourceQuotaSpecRAMRequests = `avg(avg_over_time(resourcequota_spec_resource_requests{resource="memory",unit="byte", %s}[%s])) by (resourcequota, namespace, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaSpecRAMRequests := fmt.Sprintf(queryFmtResourceQuotaSpecRAMRequests, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaSpecRAMRequests)

	ctx := pds.promContexts.NewNamedContext(KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaSpecRAMRequestAvgResult, ctx.QueryAtTime(queryResourceQuotaSpecRAMRequests, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaSpecRAMRequestMax(start, end time.Time) *source.Future[source.ResourceQuotaSpecRAMRequestMaxResult] {
	const queryName = "QueryResourceQuotaSpecRAMRequestMax"
	const queryFmtResourceQuotaSpecRAMRequests = `max(max_over_time(resourcequota_spec_resource_requests{resource="memory",unit="byte", %s}[%s])) by (resourcequota, namespace, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaSpecRAMRequests := fmt.Sprintf(queryFmtResourceQuotaSpecRAMRequests, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaSpecRAMRequests)

	ctx := pds.promContexts.NewNamedContext(KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaSpecRAMRequestMaxResult, ctx.QueryAtTime(queryResourceQuotaSpecRAMRequests, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaSpecCPULimitAverage(start, end time.Time) *source.Future[source.ResourceQuotaSpecCPULimitAvgResult] {
	const queryName = "QueryResourceQuotaSpecCPULimitAverage"
	const queryFmtResourceQuotaSpecCPULimits = `avg(avg_over_time(resourcequota_spec_resource_limits{resource="cpu",unit="core", %s}[%s])) by (resourcequota, namespace, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaSpecCPULimits := fmt.Sprintf(queryFmtResourceQuotaSpecCPULimits, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaSpecCPULimits)

	ctx := pds.promContexts.NewNamedContext(KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaSpecCPULimitAvgResult, ctx.QueryAtTime(queryResourceQuotaSpecCPULimits, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaSpecCPULimitMax(start, end time.Time) *source.Future[source.ResourceQuotaSpecCPULimitMaxResult] {
	const queryName = "QueryResourceQuotaSpecCPULimitMax"
	const queryFmtResourceQuotaSpecCPULimits = `max(max_over_time(resourcequota_spec_resource_limits{resource="cpu",unit="core", %s}[%s])) by (resourcequota, namespace, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaSpecCPULimits := fmt.Sprintf(queryFmtResourceQuotaSpecCPULimits, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaSpecCPULimits)

	ctx := pds.promContexts.NewNamedContext(KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaSpecCPULimitMaxResult, ctx.QueryAtTime(queryResourceQuotaSpecCPULimits, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaSpecRAMLimitAverage(start, end time.Time) *source.Future[source.ResourceQuotaSpecRAMLimitAvgResult] {
	const queryName = "QueryResourceQuotaSpecRAMLimitAverage"
	const queryFmtResourceQuotaSpecRAMLimits = `avg(avg_over_time(resourcequota_spec_resource_limits{resource="memory",unit="byte", %s}[%s])) by (resourcequota, namespace, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaSpecRAMLimits := fmt.Sprintf(queryFmtResourceQuotaSpecRAMLimits, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaSpecRAMLimits)

	ctx := pds.promContexts.NewNamedContext(KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaSpecRAMLimitAvgResult, ctx.QueryAtTime(queryResourceQuotaSpecRAMLimits, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaSpecRAMLimitMax(start, end time.Time) *source.Future[source.ResourceQuotaSpecRAMLimitMaxResult] {
	const queryName = "QueryResourceQuotaSpecRAMLimitMax"
	const queryFmtResourceQuotaSpecRAMLimits = `max(max_over_time(resourcequota_spec_resource_limits{resource="memory",unit="byte", %s}[%s])) by (resourcequota, namespace, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaSpecRAMLimits := fmt.Sprintf(queryFmtResourceQuotaSpecRAMLimits, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaSpecRAMLimits)

	ctx := pds.promContexts.NewNamedContext(KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaSpecRAMLimitMaxResult, ctx.QueryAtTime(queryResourceQuotaSpecRAMLimits, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaStatusUsedCPURequestAverage(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedCPURequestAvgResult] {
	const queryName = "QueryResourceQuotaStatusUsedCPURequestAverage"
	const queryFmtResourceQuotaStatusUsedCPURequests = `avg(avg_over_time(resourcequota_status_used_resource_requests{resource="cpu",unit="core", %s}[%s])) by (resourcequota, namespace, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaStatusUsedCPURequests := fmt.Sprintf(queryFmtResourceQuotaStatusUsedCPURequests, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaStatusUsedCPURequests)

	ctx := pds.promContexts.NewNamedContext(KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaStatusUsedCPURequestAvgResult, ctx.QueryAtTime(queryResourceQuotaStatusUsedCPURequests, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaStatusUsedCPURequestMax(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedCPURequestMaxResult] {
	const queryName = "QueryResourceQuotaStatusUsedCPURequestMax"
	const queryFmtResourceQuotaStatusUsedCPURequests = `max(max_over_time(resourcequota_status_used_resource_requests{resource="cpu",unit="core", %s}[%s])) by (resourcequota, namespace, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaStatusUsedCPURequests := fmt.Sprintf(queryFmtResourceQuotaStatusUsedCPURequests, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaStatusUsedCPURequests)

	ctx := pds.promContexts.NewNamedContext(KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaStatusUsedCPURequestMaxResult, ctx.QueryAtTime(queryResourceQuotaStatusUsedCPURequests, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaStatusUsedRAMRequestAverage(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedRAMRequestAvgResult] {
	const queryName = "QueryResourceQuotaStatusUsedRAMRequestAverage"
	const queryFmtResourceQuotaStatusUsedRAMRequests = `avg(avg_over_time(resourcequota_status_used_resource_requests{resource="memory",unit="byte", %s}[%s])) by (resourcequota, namespace, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaStatusUsedRAMRequests := fmt.Sprintf(queryFmtResourceQuotaStatusUsedRAMRequests, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaStatusUsedRAMRequests)

	ctx := pds.promContexts.NewNamedContext(KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaStatusUsedRAMRequestAvgResult, ctx.QueryAtTime(queryResourceQuotaStatusUsedRAMRequests, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaStatusUsedRAMRequestMax(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedRAMRequestMaxResult] {
	const queryName = "QueryResourceQuotaStatusUsedRAMRequestMax"
	const queryFmtResourceQuotaStatusUsedRAMRequests = `max(max_over_time(resourcequota_status_used_resource_requests{resource="memory",unit="byte", %s}[%s])) by (resourcequota, namespace, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaStatusUsedRAMRequests := fmt.Sprintf(queryFmtResourceQuotaStatusUsedRAMRequests, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaStatusUsedRAMRequests)

	ctx := pds.promContexts.NewNamedContext(KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaStatusUsedRAMRequestMaxResult, ctx.QueryAtTime(queryResourceQuotaStatusUsedRAMRequests, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaStatusUsedCPULimitAverage(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedCPULimitAvgResult] {
	const queryName = "QueryResourceQuotaStatusUsedCPULimitAverage"
	const queryFmtResourceQuotaStatusUsedCPULimits = `avg(avg_over_time(resourcequota_status_used_resource_limits{resource="cpu",unit="core", %s}[%s])) by (resourcequota, namespace, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaStatusUsedCPULimits := fmt.Sprintf(queryFmtResourceQuotaStatusUsedCPULimits, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaStatusUsedCPULimits)

	ctx := pds.promContexts.NewNamedContext(KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaStatusUsedCPULimitAvgResult, ctx.QueryAtTime(queryResourceQuotaStatusUsedCPULimits, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaStatusUsedCPULimitMax(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedCPULimitMaxResult] {
	const queryName = "QueryResourceQuotaStatusUsedCPULimitMax"
	const queryFmtResourceQuotaStatusUsedCPULimits = `max(max_over_time(resourcequota_status_used_resource_limits{resource="cpu",unit="core", %s}[%s])) by (resourcequota, namespace, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaStatusUsedCPULimits := fmt.Sprintf(queryFmtResourceQuotaStatusUsedCPULimits, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaStatusUsedCPULimits)

	ctx := pds.promContexts.NewNamedContext(KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaStatusUsedCPULimitMaxResult, ctx.QueryAtTime(queryResourceQuotaStatusUsedCPULimits, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaStatusUsedRAMLimitAverage(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedRAMLimitAvgResult] {
	const queryName = "QueryResourceQuotaStatusUsedRAMLimitAverage"
	const queryFmtResourceQuotaStatusUsedRAMLimits = `avg(avg_over_time(resourcequota_status_used_resource_limits{resource="memory",unit="byte", %s}[%s])) by (resourcequota, namespace, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaStatusUsedRAMLimits := fmt.Sprintf(queryFmtResourceQuotaStatusUsedRAMLimits, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaStatusUsedRAMLimits)

	ctx := pds.promContexts.NewNamedContext(KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaStatusUsedRAMLimitAvgResult, ctx.QueryAtTime(queryResourceQuotaStatusUsedRAMLimits, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaStatusUsedRAMLimitMax(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedRAMLimitMaxResult] {
	const queryName = "QueryResourceQuotaStatusUsedRAMLimitMax"
	const queryFmtResourceQuotaStatusUsedRAMLimits = `max(max_over_time(resourcequota_status_used_resource_limits{resource="memory",unit="byte", %s}[%s])) by (resourcequota, namespace, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaStatusUsedRAMLimits := fmt.Sprintf(queryFmtResourceQuotaStatusUsedRAMLimits, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaStatusUsedRAMLimits)

	ctx := pds.promContexts.NewNamedContext(KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaStatusUsedRAMLimitMaxResult, ctx.QueryAtTime(queryResourceQuotaStatusUsedRAMLimits, end))
}

func (pds *PrometheusMetricsQuerier) QueryDataCoverage(limitDays int) (time.Time, time.Time, error) {
	const (
		queryName            = "QueryDataCoverage"
		queryFmtOldestSample = `min_over_time(timestamp(group(node_cpu_hourly_cost{%s}))[%s:%s])`
		queryFmtNewestSample = `max_over_time(timestamp(group(node_cpu_hourly_cost{%s}))[%s:%s])`
	)

	cfg := pds.promConfig
	minutesPerDuration := 60
	dur := time.Duration(limitDays) * timeutil.Day
	end := time.Now().UTC().Truncate(timeutil.Day).Add(timeutil.Day)
	start := end.Add(-dur)

	durStr := pds.durationStringFor(start, end, minutesPerDuration, false)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	queryOldest := fmt.Sprintf(queryFmtOldestSample, cfg.ClusterFilter, durStr, "1h")
	log.Debugf("[Prometheus][%s[Oldest]][At Time: %d]: %s", queryName, end.Unix(), queryOldest)

	resOldestFut := ctx.QueryAtTime(queryOldest, end)

	resOldest, err := resOldestFut.Await()
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("querying oldest sample: %w", err)
	}
	if len(resOldest) == 0 || len(resOldest[0].Values) == 0 {
		// If node_cpu_hourly_cost metric is not available, fallback to a reasonable time range
		// This prevents CSV export from failing when the metric doesn't exist yet
		log.Warnf("QueryDataCoverage: node_cpu_hourly_cost metric not available, using fallback time range")

		// Use a reasonable fallback: start from 1 day ago to account for metric collection delay
		fallbackEnd := time.Now().UTC().Truncate(timeutil.Day)
		fallbackStart := fallbackEnd.AddDate(0, 0, -1) // 1 day ago

		return fallbackStart, fallbackEnd, nil
	}

	oldest := time.Unix(int64(resOldest[0].Values[0].Value), 0)

	queryNewest := fmt.Sprintf(queryFmtNewestSample, cfg.ClusterFilter, durStr, "1h")
	log.Debugf("[Prometheus][%s[Newest]][At Time: %d]: %s", queryName, end.Unix(), queryNewest)

	resNewestFut := ctx.QueryAtTime(queryNewest, end)

	resNewest, err := resNewestFut.Await()
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("querying newest sample: %w", err)
	}
	if len(resNewest) == 0 || len(resNewest[0].Values) == 0 {
		// If newest query fails but oldest succeeded, use oldest as both start and end
		// This allows CSV export to proceed with at least some time range
		log.Warnf("QueryDataCoverage: newest sample query returned no results, using oldest timestamp")
		return oldest, oldest, nil
	}

	newest := time.Unix(int64(resNewest[0].Values[0].Value), 0)

	return oldest, newest, nil
}

// durationStringFor simplifies the determination of query duration based on the version of prom and if the function
// in the query needs all data points in the vector it is provided or if it will extrapolate its own. Functions
// that extrapolate will add on another resolution if given a duration that is one resolution longer than the intended
// duration.
func (pds *PrometheusMetricsQuerier) durationStringFor(start, end time.Time, minsPerResolution int, extrapolated bool) string {
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
	if pds.promConfig.IsOffsetResolution && !extrapolated {
		// increase the query time by the resolution
		dur = dur + (time.Duration(minsPerResolution) * time.Minute)
	}

	return timeutil.DurationString(dur)
}
