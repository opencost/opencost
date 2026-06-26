package prom

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	promsource "github.com/opencost/opencost/modules/prometheus-source/pkg/prom"
	prometheus "github.com/prometheus/client_golang/api"
)

//--------------------------------------------------------------------------
//  PrometheusMetricsQuerier
//--------------------------------------------------------------------------

// PrometheusMetricsQueryLogFormat is the log format used to log metric queries before being sent to the prometheus
// instance
const PrometheusMetricsQueryLogFormat = `[PrometheusMetricsQuerier][%s][At Time: %d]: %s`

// PrometheusMetricsQuerier is the implementation of the data source's MetricsQuerier interface for Prometheus
// with OpenTelemetry Collector metrics.
type PrometheusMetricsQuerier struct {
	promConfig   *promsource.OpenCostPrometheusConfig
	promClient   prometheus.Client
	promContexts *promsource.ContextFactory
}

func (pds *PrometheusMetricsQuerier) QueryPVActiveMinutes(start, end time.Time) *source.Future[source.PVActiveMinutesResult] {
	const queryName = "QueryPVActiveMinutes"
	// kube_persistentvolume_capacity_bytes uses standard 'persistentvolume' label
	// Use label_replace to transform persistentvolume -> k8s_persistentvolume_name for decoder compatibility
	const pvActiveMinsQuery = `avg(label_replace(kube_persistentvolume_capacity_bytes{%s}, "k8s_persistentvolume_name", "$1", "persistentvolume", "(.*)")) by (%s, k8s_persistentvolume_name, uid)[%s:%dm]`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, false)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryPVActiveMins := fmt.Sprintf(pvActiveMinsQuery, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryPVActiveMins)

	ctx := pds.promContexts.NewNamedContext(promsource.ClusterContextName)
	return source.NewFuture(source.DecodePVActiveMinutesResult, ctx.QueryAtTime(queryPVActiveMins, end))
}

func (pds *PrometheusMetricsQuerier) QueryPVUsedAverage(start, end time.Time) *source.Future[source.PVUsedAvgResult] {
	const queryName = "QueryPVUsedAverage"
	// OTel k8scluster receiver uses k8s_volume_capacity and k8s_volume_available
	// Used = Capacity - Available
	const pvUsedAverageQuery = `avg(avg_over_time(k8s_volume_capacity{%s}[%s]) - avg_over_time(k8s_volume_available{%s}[%s])) by (%s, k8s_persistentvolumeclaim_name, k8s_namespace_name, uid)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryPVUsedAvg := fmt.Sprintf(pvUsedAverageQuery, cfg.ClusterFilter, durStr, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryPVUsedAvg)

	ctx := pds.promContexts.NewNamedContext(promsource.ClusterContextName)
	return source.NewFuture(source.DecodePVUsedAvgResult, ctx.QueryAtTime(queryPVUsedAvg, end))
}

func (pds *PrometheusMetricsQuerier) QueryPVUsedMax(start, end time.Time) *source.Future[source.PVUsedMaxResult] {
	const queryName = "QueryPVUsedMax"
	// OTel k8scluster receiver uses k8s_volume_capacity and k8s_volume_available
	// Used = Capacity - Available (max used = max capacity - min available)
	const pvUsedMaxQuery = `max(max_over_time(k8s_volume_capacity{%s}[%s]) - min_over_time(k8s_volume_available{%s}[%s])) by (%s, k8s_persistentvolumeclaim_name, k8s_namespace_name, uid)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryPVUsedMax := fmt.Sprintf(pvUsedMaxQuery, cfg.ClusterFilter, durStr, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryPVUsedMax)

	ctx := pds.promContexts.NewNamedContext(promsource.ClusterContextName)
	return source.NewFuture(source.DecodePVUsedMaxResult, ctx.QueryAtTime(queryPVUsedMax, end))
}

func (pds *PrometheusMetricsQuerier) QueryLocalStorageActiveMinutes(start, end time.Time) *source.Future[source.LocalStorageActiveMinutesResult] {
	const queryName = "QueryLocalStorageActiveMinutes"
	// node_total_hourly_cost uses standard 'node' label
	// Use label_replace to transform node -> k8s_node_name for decoder compatibility
	const localStorageActiveMinutesQuery = `count(label_replace(node_total_hourly_cost{%s}, "k8s_node_name", "$1", "node", "(.*)")) by (%s, k8s_node_name, uid, instance, provider_id)[%s:%dm]`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, false)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryLocalStorageActiveMins := fmt.Sprintf(localStorageActiveMinutesQuery, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryLocalStorageActiveMins)

	ctx := pds.promContexts.NewNamedContext(promsource.ClusterContextName)
	return source.NewFuture(source.DecodeLocalStorageActiveMinutesResult, ctx.QueryAtTime(queryLocalStorageActiveMins, end))
}

func (pds *PrometheusMetricsQuerier) QueryLocalStorageCost(start, end time.Time) *source.Future[source.LocalStorageCostResult] {
	const queryName = "QueryLocalStorageCost"
	// OTel hostmetrics uses system_filesystem_usage with state label (free+used+reserved = capacity)
	// Filter to root mountpoint "/" for node-level local storage
	const localStorageCostQuery = `sum_over_time(sum(system_filesystem_usage{mountpoint="/", %s}) by (k8s_node_name, device, uid, %s)[%s:%dm]) / 1024 / 1024 / 1024 * %f * %f`

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

	ctx := pds.promContexts.NewNamedContext(promsource.ClusterContextName)
	return source.NewFuture(source.DecodeLocalStorageCostResult, ctx.QueryAtTime(queryLocalStorageCost, end))
}

func (pds *PrometheusMetricsQuerier) QueryLocalStorageUsedCost(start, end time.Time) *source.Future[source.LocalStorageUsedCostResult] {
	const queryName = "QueryLocalStorageUsedCost"
	// OTel hostmetrics uses system_filesystem_usage with state="used" for used bytes
	// Filter to root mountpoint "/" for node-level local storage
	const localStorageUsedCostQuery = `sum_over_time(sum(system_filesystem_usage{mountpoint="/", state="used", %s}) by (k8s_node_name, device, uid, %s)[%s:%dm]) / 1024 / 1024 / 1024 * %f * %f`

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

	ctx := pds.promContexts.NewNamedContext(promsource.ClusterContextName)
	return source.NewFuture(source.DecodeLocalStorageUsedCostResult, ctx.QueryAtTime(queryLocalStorageUsedCost, end))
}

func (pds *PrometheusMetricsQuerier) QueryLocalStorageUsedAvg(start, end time.Time) *source.Future[source.LocalStorageUsedAvgResult] {
	const queryName = "QueryLocalStorageUsedAvg"
	// OTel hostmetrics uses system_filesystem_usage with state="used" for used bytes
	// Filter to root mountpoint "/" for node-level local storage
	const localStorageUsedAvgQuery = `avg(sum(avg_over_time(system_filesystem_usage{mountpoint="/", state="used", %s}[%s])) by (k8s_node_name, device, uid, %s)) by (k8s_node_name, device, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryLocalStorageUsedAvg := fmt.Sprintf(localStorageUsedAvgQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryLocalStorageUsedAvg)

	ctx := pds.promContexts.NewNamedContext(promsource.ClusterContextName)
	return source.NewFuture(source.DecodeLocalStorageUsedAvgResult, ctx.QueryAtTime(queryLocalStorageUsedAvg, end))
}

func (pds *PrometheusMetricsQuerier) QueryLocalStorageUsedMax(start, end time.Time) *source.Future[source.LocalStorageUsedMaxResult] {
	const queryName = "QueryLocalStorageUsedMax"
	// OTel hostmetrics uses system_filesystem_usage with state="used" for used bytes
	// Filter to root mountpoint "/" for node-level local storage
	const localStorageUsedMaxQuery = `max(sum(max_over_time(system_filesystem_usage{mountpoint="/", state="used", %s}[%s])) by (k8s_node_name, device, uid, %s)) by (k8s_node_name, device, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic("failed to parse duration string passed to QueryLocalStorageUsedMax")
	}

	queryLocalStorageUsedMax := fmt.Sprintf(localStorageUsedMaxQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryLocalStorageUsedMax)

	ctx := pds.promContexts.NewNamedContext(promsource.ClusterContextName)
	return source.NewFuture(source.DecodeLocalStorageUsedMaxResult, ctx.QueryAtTime(queryLocalStorageUsedMax, end))
}

func (pds *PrometheusMetricsQuerier) QueryLocalStorageBytes(start, end time.Time) *source.Future[source.LocalStorageBytesResult] {
	const queryName = "QueryLocalStorageBytes"
	// OTel hostmetrics uses system_filesystem_usage - sum all states (free+used+reserved) for total capacity
	// Filter to root mountpoint "/" for node-level local storage
	const localStorageBytesQuery = `avg_over_time(sum(system_filesystem_usage{mountpoint="/", %s}) by (k8s_node_name, device, uid, %s)[%s:%dm])`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, false)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryLocalStorageBytes := fmt.Sprintf(localStorageBytesQuery, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryLocalStorageBytes)

	ctx := pds.promContexts.NewNamedContext(promsource.ClusterContextName)
	return source.NewFuture(source.DecodeLocalStorageBytesResult, ctx.QueryAtTime(queryLocalStorageBytes, end))
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

	ctx := pds.promContexts.NewNamedContext(promsource.KubeModelContextName)
	return source.NewFuture(source.DecodeUptimeResult, ctx.QueryAtTime(queryClusterUptime, end))
}

func (pds *PrometheusMetricsQuerier) QueryRAMLimits(start, end time.Time) *source.Future[source.RAMLimitsResult] {
	const queryName = "QueryRAMLimits"
	// Using k8sclusterreceiver metric with OTel label names
	const queryFmtRAMLimits = `avg(avg_over_time(k8s_container_memory_limit{k8s_container_name!="", k8s_container_name!="POD", k8s_node_name!="", %s}[%s])) by (k8s_container_name, k8s_pod_name, k8s_namespace_name, k8s_node_name, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryRAMLimits := fmt.Sprintf(queryFmtRAMLimits, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryRAMLimits)

	ctx := pds.promContexts.NewNamedContext(promsource.AllocationContextName)
	return source.NewFuture(source.DecodeRAMLimitsResult, ctx.QueryAtTime(queryRAMLimits, end))
}

func (pds *PrometheusMetricsQuerier) QueryCPULimits(start, end time.Time) *source.Future[source.CPULimitsResult] {
	const queryName = "QueryCPULimits"
	// Using k8sclusterreceiver metric with OTel label names
	const queryFmtCPULimits = `avg(avg_over_time(k8s_container_cpu_limit{k8s_container_name!="", k8s_container_name!="POD", k8s_node_name!="", %s}[%s])) by (k8s_container_name, k8s_pod_name, k8s_namespace_name, k8s_node_name, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryCPULimits := fmt.Sprintf(queryFmtCPULimits, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryCPULimits)

	ctx := pds.promContexts.NewNamedContext(promsource.AllocationContextName)
	return source.NewFuture(source.DecodeCPULimitsResult, ctx.QueryAtTime(queryCPULimits, end))
}

func (pds *PrometheusMetricsQuerier) QueryPVCInfo(start, end time.Time) *source.Future[source.PVCInfoResult] {
	const queryName = "QueryPVCInfo"
	// kube_persistentvolumeclaim_info uses standard labels: persistentvolumeclaim, storageclass, volumename
	// Use label_replace to transform to OTel-style labels for decoder compatibility
	// Note: k8s_namespace_name is already present from kube-state-metrics OTel relabeling
	const queryFmtPVCInfo = `avg(
  label_replace(
    label_replace(
      label_replace(
        kube_persistentvolumeclaim_info{volumename != "", %s},
        "k8s_persistentvolumeclaim_name", "$1", "persistentvolumeclaim", "(.*)"
      ),
      "k8s_storageclass_name", "$1", "storageclass", "(.*)"
    ),
    "k8s_volume_name", "$1", "volumename", "(.*)"
  )
) by (k8s_persistentvolumeclaim_name, k8s_storageclass_name, k8s_volume_name, k8s_namespace_name, uid, %s)[%s:%dm]`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, false)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryPVCInfo := fmt.Sprintf(queryFmtPVCInfo, cfg.ClusterFilter, cfg.ClusterLabel, durStr, minsPerResolution)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryPVCInfo)

	ctx := pds.promContexts.NewNamedContext(promsource.AllocationContextName)
	return source.NewFuture(source.DecodePVCInfoResult, ctx.QueryAtTime(queryPVCInfo, end))
}

func (pds *PrometheusMetricsQuerier) QueryPVPricePerGiBHour(start, end time.Time) *source.Future[source.PVPricePerGiBHourResult] {
	const queryName = "QueryPVPricePerGiBHour"
	// pv_hourly_cost uses standard labels: persistentvolume, volumename
	// Use label_replace to transform to OTel-style labels for decoder compatibility
	// Note: label_replace must wrap avg_over_time since range selectors only work on vector selectors
	const pvCostQuery = `avg(
  label_replace(
    label_replace(
      avg_over_time(pv_hourly_cost{%s}[%s]),
      "k8s_persistentvolume_name", "$1", "persistentvolume", "(.*)"
    ),
    "k8s_volume_name", "$1", "volumename", "(.*)"
  )
) by (%s, k8s_persistentvolume_name, k8s_volume_name, uid, provider_id)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryPVCost := fmt.Sprintf(pvCostQuery, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryPVCost)

	ctx := pds.promContexts.NewNamedContext(promsource.ClusterContextName)
	return source.NewFuture(source.DecodePVPricePerGiBHourResult, ctx.QueryAtTime(queryPVCost, end))
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

	ctx := pds.promContexts.NewNamedContext(promsource.KubeModelContextName)
	return source.NewFuture(source.DecodeUptimeResult, ctx.QueryAtTime(queryNamespaceUptime, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetNatGatewayPricePerGiB(start, end time.Time) *source.Future[source.NetNatGatewayPricePerGiBResult] {
	const queryName = "QueryNetNatGatewayPricePerGiB"
	const queryFmtNetNatGatewayPricePerGiB = `avg(avg_over_time(kubecost_network_nat_gateway_egress_cost{%s}[%s])) by (%s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryNetNatGatewayPricePerGiB := fmt.Sprintf(queryFmtNetNatGatewayPricePerGiB, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryNetNatGatewayPricePerGiB)

	ctx := pds.promContexts.NewNamedContext(promsource.AllocationContextName)
	return source.NewFuture(source.DecodeNetNatGatewayPricePerGiBResult, ctx.QueryAtTime(queryNetNatGatewayPricePerGiB, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetNatGatewayGiB(start, end time.Time) *source.Future[source.NetNatGatewayGiBResult] {
	const queryName = "QueryNetNatGatewayGiB"
	// Use OTel labels: k8s_pod_name, k8s_namespace_name
	const queryFmtNetNatGatewayGiB = `sum(increase(kubecost_pod_network_egress_bytes_total{nat_gateway="true", %s}[%s:%dm])) by (k8s_pod_name, k8s_namespace_name, service, uid, %s) / 1024 / 1024 / 1024`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, true)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryNetNatGatewayGiB := fmt.Sprintf(queryFmtNetNatGatewayGiB, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryNetNatGatewayGiB)

	ctx := pds.promContexts.NewNamedContext(promsource.NetworkInsightsContextName)
	return source.NewFuture(source.DecodeNetNatGatewayGiBResult, ctx.QueryAtTime(queryNetNatGatewayGiB, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetNatGatewayIngressPricePerGiB(start, end time.Time) *source.Future[source.NetNatGatewayPricePerGiBResult] {
	const queryName = "QueryNetNatGatewayIngressPricePerGiB"
	const queryFmtNetNatGatewayIngressPricePerGiB = `avg(avg_over_time(kubecost_network_nat_gateway_ingress_cost{%s}[%s])) by (%s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryNetNatGatewayIngressPricePerGiB := fmt.Sprintf(queryFmtNetNatGatewayIngressPricePerGiB, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryNetNatGatewayIngressPricePerGiB)

	ctx := pds.promContexts.NewNamedContext(promsource.AllocationContextName)
	return source.NewFuture(source.DecodeNetNatGatewayPricePerGiBResult, ctx.QueryAtTime(queryNetNatGatewayIngressPricePerGiB, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetNatGatewayIngressGiB(start, end time.Time) *source.Future[source.NetNatGatewayIngressGiBResult] {
	const queryName = "QueryNetNatGatewayIngressGiB"
	// Use OTel labels: k8s_pod_name, k8s_namespace_name
	const queryFmtNetNatGatewayIngressGiB = `sum(increase(kubecost_pod_network_ingress_bytes_total{nat_gateway="true", %s}[%s:%dm])) by (k8s_pod_name, k8s_namespace_name, service, uid, %s) / 1024 / 1024 / 1024`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, true)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryNetNatGatewayIngressGiB := fmt.Sprintf(queryFmtNetNatGatewayIngressGiB, cfg.ClusterFilter, durStr, minsPerResolution, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryNetNatGatewayIngressGiB)

	ctx := pds.promContexts.NewNamedContext(promsource.NetworkInsightsContextName)
	return source.NewFuture(source.DecodeNetNatGatewayIngressGiBResult, ctx.QueryAtTime(queryNetNatGatewayIngressGiB, end))
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

	ctx := pds.promContexts.NewNamedContext(promsource.KubeModelContextName)
	return source.NewFuture(source.DecodeUptimeResult, ctx.QueryAtTime(queryResourceQuotaUptime, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaSpecCPURequestAverage(start, end time.Time) *source.Future[source.ResourceQuotaSpecCPURequestAvgResult] {
	const queryName = "QueryResourceQuotaSpecCPURequestAverage"
	const queryFmtResourceQuotaSpecCPURequests = `avg(avg_over_time(resourcequota_spec_resource_requests{resource="cpu",unit="core", %s}[%s])) by (resourcequota, k8s_namespace_name, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaSpecCPURequests := fmt.Sprintf(queryFmtResourceQuotaSpecCPURequests, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaSpecCPURequests)

	ctx := pds.promContexts.NewNamedContext(promsource.KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaSpecCPURequestAvgResult, ctx.QueryAtTime(queryResourceQuotaSpecCPURequests, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaSpecCPURequestMax(start, end time.Time) *source.Future[source.ResourceQuotaSpecCPURequestMaxResult] {
	const queryName = "QueryResourceQuotaSpecCPURequestMax"
	const queryFmtResourceQuotaSpecCPURequests = `max(max_over_time(resourcequota_spec_resource_requests{resource="cpu",unit="core", %s}[%s])) by (resourcequota, k8s_namespace_name, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaSpecCPURequests := fmt.Sprintf(queryFmtResourceQuotaSpecCPURequests, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaSpecCPURequests)

	ctx := pds.promContexts.NewNamedContext(promsource.KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaSpecCPURequestMaxResult, ctx.QueryAtTime(queryResourceQuotaSpecCPURequests, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaSpecRAMRequestAverage(start, end time.Time) *source.Future[source.ResourceQuotaSpecRAMRequestAvgResult] {
	const queryName = "QueryResourceQuotaSpecRAMRequestAverage"
	const queryFmtResourceQuotaSpecRAMRequests = `avg(avg_over_time(resourcequota_spec_resource_requests{resource="memory",unit="byte", %s}[%s])) by (resourcequota, k8s_namespace_name, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaSpecRAMRequests := fmt.Sprintf(queryFmtResourceQuotaSpecRAMRequests, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaSpecRAMRequests)

	ctx := pds.promContexts.NewNamedContext(promsource.KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaSpecRAMRequestAvgResult, ctx.QueryAtTime(queryResourceQuotaSpecRAMRequests, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaSpecRAMRequestMax(start, end time.Time) *source.Future[source.ResourceQuotaSpecRAMRequestMaxResult] {
	const queryName = "QueryResourceQuotaSpecRAMRequestMax"
	const queryFmtResourceQuotaSpecRAMRequests = `max(max_over_time(resourcequota_spec_resource_requests{resource="memory",unit="byte", %s}[%s])) by (resourcequota, k8s_namespace_name, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaSpecRAMRequests := fmt.Sprintf(queryFmtResourceQuotaSpecRAMRequests, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaSpecRAMRequests)

	ctx := pds.promContexts.NewNamedContext(promsource.KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaSpecRAMRequestMaxResult, ctx.QueryAtTime(queryResourceQuotaSpecRAMRequests, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaSpecCPULimitAverage(start, end time.Time) *source.Future[source.ResourceQuotaSpecCPULimitAvgResult] {
	const queryName = "QueryResourceQuotaSpecCPULimitAverage"
	const queryFmtResourceQuotaSpecCPULimits = `avg(avg_over_time(resourcequota_spec_resource_limits{resource="cpu",unit="core", %s}[%s])) by (resourcequota, k8s_namespace_name, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaSpecCPULimits := fmt.Sprintf(queryFmtResourceQuotaSpecCPULimits, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaSpecCPULimits)

	ctx := pds.promContexts.NewNamedContext(promsource.KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaSpecCPULimitAvgResult, ctx.QueryAtTime(queryResourceQuotaSpecCPULimits, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaSpecCPULimitMax(start, end time.Time) *source.Future[source.ResourceQuotaSpecCPULimitMaxResult] {
	const queryName = "QueryResourceQuotaSpecCPULimitMax"
	const queryFmtResourceQuotaSpecCPULimits = `max(max_over_time(resourcequota_spec_resource_limits{resource="cpu",unit="core", %s}[%s])) by (resourcequota, k8s_namespace_name, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaSpecCPULimits := fmt.Sprintf(queryFmtResourceQuotaSpecCPULimits, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaSpecCPULimits)

	ctx := pds.promContexts.NewNamedContext(promsource.KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaSpecCPULimitMaxResult, ctx.QueryAtTime(queryResourceQuotaSpecCPULimits, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaSpecRAMLimitAverage(start, end time.Time) *source.Future[source.ResourceQuotaSpecRAMLimitAvgResult] {
	const queryName = "QueryResourceQuotaSpecRAMLimitAverage"
	const queryFmtResourceQuotaSpecRAMLimits = `avg(avg_over_time(resourcequota_spec_resource_limits{resource="memory",unit="byte", %s}[%s])) by (resourcequota, k8s_namespace_name, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaSpecRAMLimits := fmt.Sprintf(queryFmtResourceQuotaSpecRAMLimits, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaSpecRAMLimits)

	ctx := pds.promContexts.NewNamedContext(promsource.KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaSpecRAMLimitAvgResult, ctx.QueryAtTime(queryResourceQuotaSpecRAMLimits, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaSpecRAMLimitMax(start, end time.Time) *source.Future[source.ResourceQuotaSpecRAMLimitMaxResult] {
	const queryName = "QueryResourceQuotaSpecRAMLimitMax"
	const queryFmtResourceQuotaSpecRAMLimits = `max(max_over_time(resourcequota_spec_resource_limits{resource="memory",unit="byte", %s}[%s])) by (resourcequota, k8s_namespace_name, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaSpecRAMLimits := fmt.Sprintf(queryFmtResourceQuotaSpecRAMLimits, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaSpecRAMLimits)

	ctx := pds.promContexts.NewNamedContext(promsource.KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaSpecRAMLimitMaxResult, ctx.QueryAtTime(queryResourceQuotaSpecRAMLimits, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaStatusUsedCPURequestAverage(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedCPURequestAvgResult] {
	const queryName = "QueryResourceQuotaStatusUsedCPURequestAverage"
	const queryFmtResourceQuotaStatusUsedCPURequests = `avg(avg_over_time(resourcequota_status_used_resource_requests{resource="cpu",unit="core", %s}[%s])) by (resourcequota, k8s_namespace_name, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaStatusUsedCPURequests := fmt.Sprintf(queryFmtResourceQuotaStatusUsedCPURequests, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaStatusUsedCPURequests)

	ctx := pds.promContexts.NewNamedContext(promsource.KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaStatusUsedCPURequestAvgResult, ctx.QueryAtTime(queryResourceQuotaStatusUsedCPURequests, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaStatusUsedCPURequestMax(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedCPURequestMaxResult] {
	const queryName = "QueryResourceQuotaStatusUsedCPURequestMax"
	const queryFmtResourceQuotaStatusUsedCPURequests = `max(max_over_time(resourcequota_status_used_resource_requests{resource="cpu",unit="core", %s}[%s])) by (resourcequota, k8s_namespace_name, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaStatusUsedCPURequests := fmt.Sprintf(queryFmtResourceQuotaStatusUsedCPURequests, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaStatusUsedCPURequests)

	ctx := pds.promContexts.NewNamedContext(promsource.KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaStatusUsedCPURequestMaxResult, ctx.QueryAtTime(queryResourceQuotaStatusUsedCPURequests, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaStatusUsedRAMRequestAverage(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedRAMRequestAvgResult] {
	const queryName = "QueryResourceQuotaStatusUsedRAMRequestAverage"
	const queryFmtResourceQuotaStatusUsedRAMRequests = `avg(avg_over_time(resourcequota_status_used_resource_requests{resource="memory",unit="byte", %s}[%s])) by (resourcequota, k8s_namespace_name, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaStatusUsedRAMRequests := fmt.Sprintf(queryFmtResourceQuotaStatusUsedRAMRequests, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaStatusUsedRAMRequests)

	ctx := pds.promContexts.NewNamedContext(promsource.KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaStatusUsedRAMRequestAvgResult, ctx.QueryAtTime(queryResourceQuotaStatusUsedRAMRequests, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaStatusUsedRAMRequestMax(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedRAMRequestMaxResult] {
	const queryName = "QueryResourceQuotaStatusUsedRAMRequestMax"
	const queryFmtResourceQuotaStatusUsedRAMRequests = `max(max_over_time(resourcequota_status_used_resource_requests{resource="memory",unit="byte", %s}[%s])) by (resourcequota, k8s_namespace_name, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaStatusUsedRAMRequests := fmt.Sprintf(queryFmtResourceQuotaStatusUsedRAMRequests, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaStatusUsedRAMRequests)

	ctx := pds.promContexts.NewNamedContext(promsource.KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaStatusUsedRAMRequestMaxResult, ctx.QueryAtTime(queryResourceQuotaStatusUsedRAMRequests, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaStatusUsedCPULimitAverage(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedCPULimitAvgResult] {
	const queryName = "QueryResourceQuotaStatusUsedCPULimitAverage"
	const queryFmtResourceQuotaStatusUsedCPULimits = `avg(avg_over_time(resourcequota_status_used_resource_limits{resource="cpu",unit="core", %s}[%s])) by (resourcequota, k8s_namespace_name, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaStatusUsedCPULimits := fmt.Sprintf(queryFmtResourceQuotaStatusUsedCPULimits, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaStatusUsedCPULimits)

	ctx := pds.promContexts.NewNamedContext(promsource.KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaStatusUsedCPULimitAvgResult, ctx.QueryAtTime(queryResourceQuotaStatusUsedCPULimits, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaStatusUsedCPULimitMax(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedCPULimitMaxResult] {
	const queryName = "QueryResourceQuotaStatusUsedCPULimitMax"
	const queryFmtResourceQuotaStatusUsedCPULimits = `max(max_over_time(resourcequota_status_used_resource_limits{resource="cpu",unit="core", %s}[%s])) by (resourcequota, k8s_namespace_name, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaStatusUsedCPULimits := fmt.Sprintf(queryFmtResourceQuotaStatusUsedCPULimits, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaStatusUsedCPULimits)

	ctx := pds.promContexts.NewNamedContext(promsource.KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaStatusUsedCPULimitMaxResult, ctx.QueryAtTime(queryResourceQuotaStatusUsedCPULimits, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaStatusUsedRAMLimitAverage(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedRAMLimitAvgResult] {
	const queryName = "QueryResourceQuotaStatusUsedRAMLimitAverage"
	const queryFmtResourceQuotaStatusUsedRAMLimits = `avg(avg_over_time(resourcequota_status_used_resource_limits{resource="memory",unit="byte", %s}[%s])) by (resourcequota, k8s_namespace_name, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaStatusUsedRAMLimits := fmt.Sprintf(queryFmtResourceQuotaStatusUsedRAMLimits, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaStatusUsedRAMLimits)

	ctx := pds.promContexts.NewNamedContext(promsource.KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaStatusUsedRAMLimitAvgResult, ctx.QueryAtTime(queryResourceQuotaStatusUsedRAMLimits, end))
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaStatusUsedRAMLimitMax(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedRAMLimitMaxResult] {
	const queryName = "QueryResourceQuotaStatusUsedRAMLimitMax"
	const queryFmtResourceQuotaStatusUsedRAMLimits = `max(max_over_time(resourcequota_status_used_resource_limits{resource="memory",unit="byte", %s}[%s])) by (resourcequota, k8s_namespace_name, uid, %s)`

	cfg := pds.promConfig

	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryResourceQuotaStatusUsedRAMLimits := fmt.Sprintf(queryFmtResourceQuotaStatusUsedRAMLimits, cfg.ClusterFilter, durStr, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryResourceQuotaStatusUsedRAMLimits)

	ctx := pds.promContexts.NewNamedContext(promsource.KubeModelContextName)
	return source.NewFuture(source.DecodeResourceQuotaStatusUsedRAMLimitMaxResult, ctx.QueryAtTime(queryResourceQuotaStatusUsedRAMLimits, end))
}

func newPrometheusMetricsQuerier(
	promConfig *promsource.OpenCostPrometheusConfig,
	promClient prometheus.Client,
	promContexts *promsource.ContextFactory,
) *PrometheusMetricsQuerier {
	return &PrometheusMetricsQuerier{
		promConfig:   promConfig,
		promClient:   promClient,
		promContexts: promContexts,
	}
}

// NewPrometheusMetricsQuerierForTesting creates a PrometheusMetricsQuerier for testing purposes.
// This allows external test code to instantiate a querier with custom configuration.
func NewPrometheusMetricsQuerierForTesting(
	promConfig *promsource.OpenCostPrometheusConfig,
	promClient prometheus.Client,
	promContexts *promsource.ContextFactory,
) *PrometheusMetricsQuerier {
	return newPrometheusMetricsQuerier(promConfig, promClient, promContexts)
}

// NewNamedContext creates a new query context with the specified name
func (pds *PrometheusMetricsQuerier) NewNamedContext(name string) *promsource.Context {
	return pds.promContexts.NewNamedContext(name)
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
