package prom

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	promsource "github.com/opencost/opencost/modules/prometheus-source/pkg/prom"
)

func (pds *PrometheusMetricsQuerier) QueryNetZoneGiB(start, end time.Time) *source.Future[source.NetZoneGiBResult] {
	cfg := pds.promConfig
	m := cfg.DataResolutionMinutes
	d := pds.durationStringFor(start, end, m, false)
	q := fmt.Sprintf(`sum(increase(kubecost_pod_network_egress_bytes_total{internet="false",same_zone="false",same_region="true",%s}[%s:%dm])) by (pod_name,k8s_namespace_name,%s) / 1024 / 1024 / 1024`, cfg.ClusterFilter, d, m, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNetZoneGiB", end.Unix(), q)
	return source.NewFuture(source.DecodeNetZoneGiBResult, pds.promContexts.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetZonePricePerGiB(start, end time.Time) *source.Future[source.NetZonePricePerGiBResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg(avg_over_time(kubecost_network_zone_egress_cost{%s}[%s])) by (%s)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNetZonePricePerGiB", end.Unix(), q)
	return source.NewFuture(source.DecodeNetZonePricePerGiBResult, pds.promContexts.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetRegionGiB(start, end time.Time) *source.Future[source.NetRegionGiBResult] {
	cfg := pds.promConfig
	m := cfg.DataResolutionMinutes
	d := pds.durationStringFor(start, end, m, false)
	q := fmt.Sprintf(`sum(increase(kubecost_pod_network_egress_bytes_total{internet="false",same_zone="false",same_region="false",%s}[%s:%dm])) by (pod_name,k8s_namespace_name,%s) / 1024 / 1024 / 1024`, cfg.ClusterFilter, d, m, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNetRegionGiB", end.Unix(), q)
	return source.NewFuture(source.DecodeNetRegionGiBResult, pds.promContexts.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetRegionPricePerGiB(start, end time.Time) *source.Future[source.NetRegionPricePerGiBResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg(avg_over_time(kubecost_network_region_egress_cost{%s}[%s])) by (%s)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNetRegionPricePerGiB", end.Unix(), q)
	return source.NewFuture(source.DecodeNetRegionPricePerGiBResult, pds.promContexts.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetInternetGiB(start, end time.Time) *source.Future[source.NetInternetGiBResult] {
	cfg := pds.promConfig
	m := cfg.DataResolutionMinutes
	d := pds.durationStringFor(start, end, m, false)
	q := fmt.Sprintf(`sum(increase(kubecost_pod_network_egress_bytes_total{internet="true",%s}[%s:%dm])) by (pod_name,k8s_namespace_name,%s) / 1024 / 1024 / 1024`, cfg.ClusterFilter, d, m, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNetInternetGiB", end.Unix(), q)
	return source.NewFuture(source.DecodeNetInternetGiBResult, pds.promContexts.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetInternetPricePerGiB(start, end time.Time) *source.Future[source.NetInternetPricePerGiBResult] {
	cfg := pds.promConfig
	d := timeutil.DurationString(end.Sub(start))
	q := fmt.Sprintf(`avg(avg_over_time(kubecost_network_internet_egress_cost{%s}[%s])) by (%s)`, cfg.ClusterFilter, d, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNetInternetPricePerGiB", end.Unix(), q)
	return source.NewFuture(source.DecodeNetInternetPricePerGiBResult, pds.promContexts.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetInternetServiceGiB(start, end time.Time) *source.Future[source.NetInternetServiceGiBResult] {
	cfg := pds.promConfig
	m := cfg.DataResolutionMinutes
	d := pds.durationStringFor(start, end, m, false)
	q := fmt.Sprintf(`sum(increase(kubecost_pod_network_egress_bytes_total{internet="true",%s}[%s:%dm])) by (pod_name,k8s_namespace_name,service,%s) / 1024 / 1024 / 1024`, cfg.ClusterFilter, d, m, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNetInternetServiceGiB", end.Unix(), q)
	return source.NewFuture(source.DecodeNetInternetServiceGiBResult, pds.promContexts.NewNamedContext(promsource.NetworkInsightsContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetTransferBytes(start, end time.Time) *source.Future[source.NetTransferBytesResult] {
	cfg := pds.promConfig
	m := cfg.DataResolutionMinutes
	d := pds.durationStringFor(start, end, m, false)
	q := fmt.Sprintf(`sum(increase(k8s_pod_network_io{direction="transmit",k8s_pod_name!="",%s}[%s:%dm])) by (pod_name,k8s_pod_name,k8s_namespace_name,%s)`, cfg.ClusterFilter, d, m, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNetTransferBytes", end.Unix(), q)
	return source.NewFuture(source.DecodeNetTransferBytesResult, pds.promContexts.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetZoneIngressGiB(start, end time.Time) *source.Future[source.NetZoneIngressGiBResult] {
	cfg := pds.promConfig
	m := cfg.DataResolutionMinutes
	d := pds.durationStringFor(start, end, m, false)
	q := fmt.Sprintf(`sum(increase(kubecost_pod_network_ingress_bytes_total{internet="false",same_zone="false",same_region="true",%s}[%s:%dm])) by (pod_name,k8s_namespace_name,%s) / 1024 / 1024 / 1024`, cfg.ClusterFilter, d, m, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNetZoneIngressGiB", end.Unix(), q)
	return source.NewFuture(source.DecodeNetZoneIngressGiBResult, pds.promContexts.NewNamedContext(promsource.NetworkInsightsContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetRegionIngressGiB(start, end time.Time) *source.Future[source.NetRegionIngressGiBResult] {
	cfg := pds.promConfig
	m := cfg.DataResolutionMinutes
	d := pds.durationStringFor(start, end, m, false)
	q := fmt.Sprintf(`sum(increase(kubecost_pod_network_ingress_bytes_total{internet="false",same_zone="false",same_region="false",%s}[%s:%dm])) by (pod_name,k8s_namespace_name,%s) / 1024 / 1024 / 1024`, cfg.ClusterFilter, d, m, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNetRegionIngressGiB", end.Unix(), q)
	return source.NewFuture(source.DecodeNetRegionIngressGiBResult, pds.promContexts.NewNamedContext(promsource.NetworkInsightsContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetInternetIngressGiB(start, end time.Time) *source.Future[source.NetInternetIngressGiBResult] {
	cfg := pds.promConfig
	m := cfg.DataResolutionMinutes
	d := pds.durationStringFor(start, end, m, false)
	q := fmt.Sprintf(`sum(increase(kubecost_pod_network_ingress_bytes_total{internet="true",%s}[%s:%dm])) by (pod_name,k8s_namespace_name,%s) / 1024 / 1024 / 1024`, cfg.ClusterFilter, d, m, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNetInternetIngressGiB", end.Unix(), q)
	return source.NewFuture(source.DecodeNetInternetIngressGiBResult, pds.promContexts.NewNamedContext(promsource.NetworkInsightsContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetInternetServiceIngressGiB(start, end time.Time) *source.Future[source.NetInternetServiceIngressGiBResult] {
	cfg := pds.promConfig
	m := cfg.DataResolutionMinutes
	d := pds.durationStringFor(start, end, m, false)
	q := fmt.Sprintf(`sum(increase(kubecost_pod_network_ingress_bytes_total{internet="true",%s}[%s:%dm])) by (pod_name,k8s_namespace_name,service,%s) / 1024 / 1024 / 1024`, cfg.ClusterFilter, d, m, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNetInternetServiceIngressGiB", end.Unix(), q)
	return source.NewFuture(source.DecodeNetInternetServiceIngressGiBResult, pds.promContexts.NewNamedContext(promsource.NetworkInsightsContextName).QueryAtTime(q, end))
}

func (pds *PrometheusMetricsQuerier) QueryNetReceiveBytes(start, end time.Time) *source.Future[source.NetReceiveBytesResult] {
	cfg := pds.promConfig
	m := cfg.DataResolutionMinutes
	d := pds.durationStringFor(start, end, m, false)
	q := fmt.Sprintf(`sum(increase(k8s_pod_network_io{direction="receive",k8s_pod_name!="",%s}[%s:%dm])) by (pod_name,k8s_pod_name,k8s_namespace_name,%s)`, cfg.ClusterFilter, d, m, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNetReceiveBytes", end.Unix(), q)
	return source.NewFuture(source.DecodeNetReceiveBytesResult, pds.promContexts.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}
