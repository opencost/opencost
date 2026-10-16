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
	// KSM kube_persistentvolume_capacity_bytes has k8s_persistentvolume_name natively (OTel-scraped).
	// No label_replace needed.
	const pvActiveMinsQuery = `avg(kube_persistentvolume_capacity_bytes{%s}) by (%s, k8s_persistentvolume_name)[%s:%dm]`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, false)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryPVActiveMins := fmt.Sprintf(pvActiveMinsQuery, pds.clusterFilterSuffix(), cfg.ClusterLabel, durStr, minsPerResolution)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryPVActiveMins)

	ctx := pds.promContexts.NewNamedContext(promsource.ClusterContextName)
	return source.NewFuture(source.DecodePVActiveMinutesResult, ctx.QueryAtTime(queryPVActiveMins, end))
}

func (pds *PrometheusMetricsQuerier) QueryPVUsedAverage(start, end time.Time) *source.Future[source.PVUsedAvgResult] {
	// k8s_volume_capacity / k8s_volume_available (OTel kubeletstats) use k8s_volume_name
	// which is the pod volumeMount name (e.g. "pgdata"), not the PV/PVC name.
	// kube_persistentvolumeclaim_info carries volumename (PV name), not volume-mount name,
	// so no PromQL join is possible without an intermediate label mapping.
	// The decoder (DecodePVUsedAvgResult) expects k8s_persistentvolumeclaim_name;
	// returning empty is correct here — callers treat missing PV usage as zero.
	log.Warnf("QueryPVUsedAverage: k8s_volume_* labels cannot be joined to PVC names in PromQL; returning empty")
	return source.NewFutureFrom([]*source.PVUsedAvgResult{})
}

func (pds *PrometheusMetricsQuerier) QueryPVUsedMax(start, end time.Time) *source.Future[source.PVUsedMaxResult] {
	// Same limitation as QueryPVUsedAverage — k8s_volume_name cannot be joined to PVC name.
	log.Warnf("QueryPVUsedMax: k8s_volume_* labels cannot be joined to PVC names in PromQL; returning empty")
	return source.NewFutureFrom([]*source.PVUsedMaxResult{})
}

func (pds *PrometheusMetricsQuerier) QueryLocalStorageActiveMinutes(start, end time.Time) *source.Future[source.LocalStorageActiveMinutesResult] {
	const queryName = "QueryLocalStorageActiveMinutes"
	// Use k8s_node_uptime (OTel k8scluster) — has k8s_node_name natively.
	const localStorageActiveMinutesQuery = `count(k8s_node_uptime{%s}) by (%s, k8s_node_name)[%s:%dm]`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, false)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryLocalStorageActiveMins := fmt.Sprintf(localStorageActiveMinutesQuery, pds.clusterFilterSuffix(), cfg.ClusterLabel, durStr, minsPerResolution)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryLocalStorageActiveMins)

	ctx := pds.promContexts.NewNamedContext(promsource.ClusterContextName)
	return source.NewFuture(source.DecodeLocalStorageActiveMinutesResult, ctx.QueryAtTime(queryLocalStorageActiveMins, end))
}

func (pds *PrometheusMetricsQuerier) QueryLocalStorageCost(start, end time.Time) *source.Future[source.LocalStorageCostResult] {
	// system_filesystem_usage (OTel hostmetrics) not available in our cluster.
	// k8s_node_filesystem_* is available but has no cost dimension without pricing data.
	log.Warnf("QueryLocalStorageCost: system_filesystem_usage not available; returning empty")
	return source.NewFutureFrom([]*source.LocalStorageCostResult{})
}

func (pds *PrometheusMetricsQuerier) QueryLocalStorageUsedCost(start, end time.Time) *source.Future[source.LocalStorageUsedCostResult] {
	log.Warnf("QueryLocalStorageUsedCost: system_filesystem_usage not available; returning empty")
	return source.NewFutureFrom([]*source.LocalStorageUsedCostResult{})
}

func (pds *PrometheusMetricsQuerier) QueryLocalStorageUsedAvg(start, end time.Time) *source.Future[source.LocalStorageUsedAvgResult] {
	// avg_over_time cannot take a binary expression as range vector directly.
	// Compute avg of each side separately, then subtract.
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(
		`avg(avg_over_time(k8s_node_filesystem_capacity{%s}[%s]) - avg_over_time(k8s_node_filesystem_available{%s}[%s])) by (k8s_node_name, %s)`,
		pds.clusterFilterSuffix(), d, pds.clusterFilterSuffix(), d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryLocalStorageUsedAvg", end.Unix(), q)
	ctx := pds.promContexts.NewNamedContext(promsource.ClusterContextName)
	return source.NewFuture(source.DecodeLocalStorageUsedAvgResult, ctx.QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryLocalStorageUsedMax(start, end time.Time) *source.Future[source.LocalStorageUsedMaxResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(
		`max(max_over_time(k8s_node_filesystem_capacity{%s}[%s]) - max_over_time(k8s_node_filesystem_available{%s}[%s])) by (k8s_node_name, %s)`,
		pds.clusterFilterSuffix(), d, pds.clusterFilterSuffix(), d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryLocalStorageUsedMax", end.Unix(), q)
	ctx := pds.promContexts.NewNamedContext(promsource.ClusterContextName)
	return source.NewFuture(source.DecodeLocalStorageUsedMaxResult, ctx.QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryLocalStorageBytes(start, end time.Time) *source.Future[source.LocalStorageBytesResult] {
	// Total filesystem capacity from OTel k8scluster receiver.
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(
		`avg(avg_over_time(k8s_node_filesystem_capacity{%s}[%s])) by (k8s_node_name, %s)`,
		pds.clusterFilterSuffix(), d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryLocalStorageBytes", end.Unix(), q)
	ctx := pds.promContexts.NewNamedContext(promsource.ClusterContextName)
	return source.NewFuture(source.DecodeLocalStorageBytesResult, ctx.QueryAtTime(q, end))
}

// QueryClusterUptime — cluster_info not emitted by OTel receivers.
func (pds *PrometheusMetricsQuerier) QueryClusterUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	log.Warnf("QueryClusterUptime: cluster_info not available via OTel receivers; returning empty")
	return source.NewFutureFrom([]*source.UptimeResult{})
}

func (pds *PrometheusMetricsQuerier) QueryPVCInfo(start, end time.Time) *source.Future[source.PVCInfoResult] {
	const queryName = "QueryPVCInfo"
	// kube_persistentvolumeclaim_info (OTel-scraped KSM) has k8s_persistentvolumeclaim_name,
	// k8s_storageclass_name, k8s_namespace_name natively. volumename label holds the PV name.
	// The decoder reads VolumeNameKey = k8s_volume_name, so we alias volumename → k8s_volume_name.
	const queryFmtPVCInfo = `avg(label_replace(kube_persistentvolumeclaim_info{volumename != "", %s}, "k8s_volume_name", "$1", "volumename", "(.*)")) by (k8s_persistentvolumeclaim_name, k8s_storageclass_name, k8s_volume_name, k8s_namespace_name, %s)[%s:%dm]`

	cfg := pds.promConfig
	minsPerResolution := cfg.DataResolutionMinutes

	durStr := pds.durationStringFor(start, end, minsPerResolution, false)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	queryPVCInfo := fmt.Sprintf(queryFmtPVCInfo, pds.clusterFilterSuffix(), cfg.ClusterLabel, durStr, minsPerResolution)
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), queryPVCInfo)

	ctx := pds.promContexts.NewNamedContext(promsource.AllocationContextName)
	return source.NewFuture(source.DecodePVCInfoResult, ctx.QueryAtTime(queryPVCInfo, end))
}

func (pds *PrometheusMetricsQuerier) QueryPVPricePerGiBHour(start, end time.Time) *source.Future[source.PVPricePerGiBHourResult] {
	// pv_hourly_cost is an OpenCost-generated output metric, not available as an input.
	// PV pricing falls back to cloud provider pricing or defaults.
	log.Warnf("QueryPVPricePerGiBHour: pv_hourly_cost is an OpenCost output metric, not available as input; returning empty result")
	return source.NewFutureFrom([]*source.PVPricePerGiBHourResult{})
}

// QueryNamespaceUptime — namespace_info not emitted by OTel receivers.
func (pds *PrometheusMetricsQuerier) QueryNamespaceUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	log.Warnf("QueryNamespaceUptime: namespace_info not available via OTel receivers; returning empty")
	return source.NewFutureFrom([]*source.UptimeResult{})
}

// QueryNetNatGateway* — kubecost_network_* and kubecost_pod_network_*_bytes_total are OpenCost output metrics.
func (pds *PrometheusMetricsQuerier) QueryNetNatGatewayPricePerGiB(start, end time.Time) *source.Future[source.NetNatGatewayPricePerGiBResult] {
	log.Warnf("QueryNetNatGatewayPricePerGiB: kubecost_network_nat_gateway_egress_cost not available; returning empty")
	return source.NewFutureFrom([]*source.NetNatGatewayPricePerGiBResult{})
}

func (pds *PrometheusMetricsQuerier) QueryNetNatGatewayGiB(start, end time.Time) *source.Future[source.NetNatGatewayGiBResult] {
	log.Warnf("QueryNetNatGatewayGiB: kubecost_pod_network_egress_bytes_total not available; returning empty")
	return source.NewFutureFrom([]*source.NetNatGatewayGiBResult{})
}

func (pds *PrometheusMetricsQuerier) QueryNetNatGatewayIngressPricePerGiB(start, end time.Time) *source.Future[source.NetNatGatewayPricePerGiBResult] {
	log.Warnf("QueryNetNatGatewayIngressPricePerGiB: kubecost_network_nat_gateway_ingress_cost not available; returning empty")
	return source.NewFutureFrom([]*source.NetNatGatewayPricePerGiBResult{})
}

func (pds *PrometheusMetricsQuerier) QueryNetNatGatewayIngressGiB(start, end time.Time) *source.Future[source.NetNatGatewayIngressGiBResult] {
	log.Warnf("QueryNetNatGatewayIngressGiB: kubecost_pod_network_ingress_bytes_total not available; returning empty")
	return source.NewFutureFrom([]*source.NetNatGatewayIngressGiBResult{})
}

// Note: resourcequota_* metrics are not emitted in OTel-only setups.
// These implementations use k8s_namespace_name (OTel label) and ResourceResult (upstream type).

// QueryResourceQuota* — resourcequota_info / resourcequota_spec_* / resourcequota_status_* are
// not emitted by OTel receivers. Use kube_resourcequota (KSM) instead when implementing.
// Returning empty stubs for now.

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaUptime(start, end time.Time) *source.Future[source.UptimeResult] {
	log.Warnf("QueryResourceQuotaUptime: resourcequota_info not available; returning empty")
	return source.NewFutureFrom([]*source.UptimeResult{})
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaSpecCPURequestAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	log.Warnf("QueryResourceQuotaSpecCPURequestAverage: resourcequota_spec_resource_requests not available; returning empty")
	return source.NewFutureFrom([]*source.ResourceResult{})
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaSpecCPURequestMax(start, end time.Time) *source.Future[source.ResourceResult] {
	log.Warnf("QueryResourceQuotaSpecCPURequestMax: resourcequota_spec_resource_requests not available; returning empty")
	return source.NewFutureFrom([]*source.ResourceResult{})
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaSpecRAMRequestAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	log.Warnf("QueryResourceQuotaSpecRAMRequestAverage: resourcequota_spec_resource_requests not available; returning empty")
	return source.NewFutureFrom([]*source.ResourceResult{})
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaSpecRAMRequestMax(start, end time.Time) *source.Future[source.ResourceResult] {
	log.Warnf("QueryResourceQuotaSpecRAMRequestMax: resourcequota_spec_resource_requests not available; returning empty")
	return source.NewFutureFrom([]*source.ResourceResult{})
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaSpecCPULimitAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	log.Warnf("QueryResourceQuotaSpecCPULimitAverage: resourcequota_spec_resource_limits not available; returning empty")
	return source.NewFutureFrom([]*source.ResourceResult{})
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaSpecCPULimitMax(start, end time.Time) *source.Future[source.ResourceResult] {
	log.Warnf("QueryResourceQuotaSpecCPULimitMax: resourcequota_spec_resource_limits not available; returning empty")
	return source.NewFutureFrom([]*source.ResourceResult{})
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaSpecRAMLimitAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	log.Warnf("QueryResourceQuotaSpecRAMLimitAverage: resourcequota_spec_resource_limits not available; returning empty")
	return source.NewFutureFrom([]*source.ResourceResult{})
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaSpecRAMLimitMax(start, end time.Time) *source.Future[source.ResourceResult] {
	log.Warnf("QueryResourceQuotaSpecRAMLimitMax: resourcequota_spec_resource_limits not available; returning empty")
	return source.NewFutureFrom([]*source.ResourceResult{})
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaStatusUsedCPURequestAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	log.Warnf("QueryResourceQuotaStatusUsedCPURequestAverage: resourcequota_status_used_resource_requests not available; returning empty")
	return source.NewFutureFrom([]*source.ResourceResult{})
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaStatusUsedCPURequestMax(start, end time.Time) *source.Future[source.ResourceResult] {
	log.Warnf("QueryResourceQuotaStatusUsedCPURequestMax: resourcequota_status_used_resource_requests not available; returning empty")
	return source.NewFutureFrom([]*source.ResourceResult{})
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaStatusUsedRAMRequestAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	log.Warnf("QueryResourceQuotaStatusUsedRAMRequestAverage: resourcequota_status_used_resource_requests not available; returning empty")
	return source.NewFutureFrom([]*source.ResourceResult{})
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaStatusUsedRAMRequestMax(start, end time.Time) *source.Future[source.ResourceResult] {
	log.Warnf("QueryResourceQuotaStatusUsedRAMRequestMax: resourcequota_status_used_resource_requests not available; returning empty")
	return source.NewFutureFrom([]*source.ResourceResult{})
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaStatusUsedCPULimitAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	log.Warnf("QueryResourceQuotaStatusUsedCPULimitAverage: resourcequota_status_used_resource_limits not available; returning empty")
	return source.NewFutureFrom([]*source.ResourceResult{})
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaStatusUsedCPULimitMax(start, end time.Time) *source.Future[source.ResourceResult] {
	log.Warnf("QueryResourceQuotaStatusUsedCPULimitMax: resourcequota_status_used_resource_limits not available; returning empty")
	return source.NewFutureFrom([]*source.ResourceResult{})
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaStatusUsedRAMLimitAverage(start, end time.Time) *source.Future[source.ResourceResult] {
	log.Warnf("QueryResourceQuotaStatusUsedRAMLimitAverage: resourcequota_status_used_resource_limits not available; returning empty")
	return source.NewFutureFrom([]*source.ResourceResult{})
}

func (pds *PrometheusMetricsQuerier) QueryResourceQuotaStatusUsedRAMLimitMax(start, end time.Time) *source.Future[source.ResourceResult] {
	log.Warnf("QueryResourceQuotaStatusUsedRAMLimitMax: resourcequota_status_used_resource_limits not available; returning empty")
	return source.NewFutureFrom([]*source.ResourceResult{})
}
// clusterFilterSuffix returns ",<filter>" when ClusterFilter is non-empty,
// or "" when it is empty. Use this instead of embedding ClusterFilter directly
// after a comma in selector literals, to avoid generating invalid PromQL like
// {resource="cpu",} when cluster filtering is disabled.
func (pds *PrometheusMetricsQuerier) clusterFilterSuffix() string {
	if pds.promConfig.ClusterFilter == "" {
		return ""
	}
	return "," + pds.promConfig.ClusterFilter
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
