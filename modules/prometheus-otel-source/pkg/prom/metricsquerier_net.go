package prom

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	promsource "github.com/opencost/opencost/modules/prometheus-source/pkg/prom"
)

// QueryNetZoneGiB / QueryNetRegionGiB / QueryNetInternetGiB / QueryNetInternetServiceGiB —
// kubecost_pod_network_egress_bytes_total is an OpenCost output metric (network-costs plugin),
// not available in OTel-only setups.

func (pds *PrometheusMetricsQuerier) QueryNetZoneGiB(start, end time.Time) *source.Future[source.NetZoneGiBResult] {
	log.Warnf("QueryNetZoneGiB: kubecost_pod_network_egress_bytes_total not available; returning empty")
	return source.NewFutureFrom([]*source.NetZoneGiBResult{})
}

func (pds *PrometheusMetricsQuerier) QueryNetZonePricePerGiB(start, end time.Time) *source.Future[source.NetZonePricePerGiBResult] {
	log.Warnf("QueryNetZonePricePerGiB: kubecost_network_zone_egress_cost not available; returning empty")
	return source.NewFutureFrom([]*source.NetZonePricePerGiBResult{})
}

func (pds *PrometheusMetricsQuerier) QueryNetRegionGiB(start, end time.Time) *source.Future[source.NetRegionGiBResult] {
	log.Warnf("QueryNetRegionGiB: kubecost_pod_network_egress_bytes_total not available; returning empty")
	return source.NewFutureFrom([]*source.NetRegionGiBResult{})
}

func (pds *PrometheusMetricsQuerier) QueryNetRegionPricePerGiB(start, end time.Time) *source.Future[source.NetRegionPricePerGiBResult] {
	log.Warnf("QueryNetRegionPricePerGiB: kubecost_network_region_egress_cost not available; returning empty")
	return source.NewFutureFrom([]*source.NetRegionPricePerGiBResult{})
}

func (pds *PrometheusMetricsQuerier) QueryNetInternetGiB(start, end time.Time) *source.Future[source.NetInternetGiBResult] {
	log.Warnf("QueryNetInternetGiB: kubecost_pod_network_egress_bytes_total not available; returning empty")
	return source.NewFutureFrom([]*source.NetInternetGiBResult{})
}

func (pds *PrometheusMetricsQuerier) QueryNetInternetPricePerGiB(start, end time.Time) *source.Future[source.NetInternetPricePerGiBResult] {
	log.Warnf("QueryNetInternetPricePerGiB: kubecost_network_internet_egress_cost not available; returning empty")
	return source.NewFutureFrom([]*source.NetInternetPricePerGiBResult{})
}

func (pds *PrometheusMetricsQuerier) QueryNetInternetServiceGiB(start, end time.Time) *source.Future[source.NetInternetServiceGiBResult] {
	log.Warnf("QueryNetInternetServiceGiB: kubecost_pod_network_egress_bytes_total not available; returning empty")
	return source.NewFutureFrom([]*source.NetInternetServiceGiBResult{})
}

// QueryNetTransferBytes uses OTel k8s_pod_network_io (kubeletstats receiver) — available in cluster.
func (pds *PrometheusMetricsQuerier) QueryNetTransferBytes(start, end time.Time) *source.Future[source.NetTransferBytesResult] {
	cfg := pds.promConfig
	m := cfg.DataResolutionMinutes
	d := pds.durationStringFor(start, end, m, false)
	q := fmt.Sprintf(`sum(increase(k8s_pod_network_io{direction="transmit",k8s_pod_name!=""%s}[%s:%dm])) by (k8s_pod_name,k8s_namespace_name,%s)`, pds.clusterFilterSuffix(), d, m, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNetTransferBytes", end.Unix(), q)
	return source.NewFuture(source.DecodeNetTransferBytesResult, pds.promContexts.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}

// QueryNetZoneIngressGiB / QueryNetRegionIngressGiB / QueryNetInternetIngressGiB / QueryNetInternetServiceIngressGiB —
// kubecost_pod_network_ingress_bytes_total not available in OTel-only setups.

func (pds *PrometheusMetricsQuerier) QueryNetZoneIngressGiB(start, end time.Time) *source.Future[source.NetZoneIngressGiBResult] {
	log.Warnf("QueryNetZoneIngressGiB: kubecost_pod_network_ingress_bytes_total not available; returning empty")
	return source.NewFutureFrom([]*source.NetZoneIngressGiBResult{})
}

func (pds *PrometheusMetricsQuerier) QueryNetRegionIngressGiB(start, end time.Time) *source.Future[source.NetRegionIngressGiBResult] {
	log.Warnf("QueryNetRegionIngressGiB: kubecost_pod_network_ingress_bytes_total not available; returning empty")
	return source.NewFutureFrom([]*source.NetRegionIngressGiBResult{})
}

func (pds *PrometheusMetricsQuerier) QueryNetInternetIngressGiB(start, end time.Time) *source.Future[source.NetInternetIngressGiBResult] {
	log.Warnf("QueryNetInternetIngressGiB: kubecost_pod_network_ingress_bytes_total not available; returning empty")
	return source.NewFutureFrom([]*source.NetInternetIngressGiBResult{})
}

func (pds *PrometheusMetricsQuerier) QueryNetInternetServiceIngressGiB(start, end time.Time) *source.Future[source.NetInternetServiceIngressGiBResult] {
	log.Warnf("QueryNetInternetServiceIngressGiB: kubecost_pod_network_ingress_bytes_total not available; returning empty")
	return source.NewFutureFrom([]*source.NetInternetServiceIngressGiBResult{})
}

// QueryNetReceiveBytes uses OTel k8s_pod_network_io (kubeletstats receiver) — available in cluster.
func (pds *PrometheusMetricsQuerier) QueryNetReceiveBytes(start, end time.Time) *source.Future[source.NetReceiveBytesResult] {
	cfg := pds.promConfig
	m := cfg.DataResolutionMinutes
	d := pds.durationStringFor(start, end, m, false)
	q := fmt.Sprintf(`sum(increase(k8s_pod_network_io{direction="receive",k8s_pod_name!=""%s}[%s:%dm])) by (k8s_pod_name,k8s_namespace_name,%s)`, pds.clusterFilterSuffix(), d, m, cfg.ClusterLabel)
	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryNetReceiveBytes", end.Unix(), q)
	return source.NewFuture(source.DecodeNetReceiveBytesResult, pds.promContexts.NewNamedContext(promsource.AllocationContextName).QueryAtTime(q, end))
}
