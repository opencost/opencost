package prom

import (
	"fmt"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

// GPU saturation queries
//
// These queries derive USE-method saturation signals for GPUs from
// dcgm-exporter metrics. Each query is an independent primitive; when the
// underlying DCGM field is not collected (no dcgm-exporter, the field is
// disabled in its config, or the GPU lacks DCP profiling support) the query
// simply returns no series, which downstream code treats as "signal absent"
// rather than zero.
//
// Sources by dcgm-exporter configuration:
//   - default config: DCGM_FI_DEV_*_VIOLATION, DCGM_FI_DEV_FB_USED/FREE,
//     DCGM_FI_DEV_XID_ERRORS, DCGM_FI_PROF_DRAM_ACTIVE,
//     DCGM_FI_PROF_PCIE_TX/RX_BYTES
//   - requires explicit enablement: DCGM_FI_DEV_CLOCK_THROTTLE_REASONS
//     (DCGM_FI_DEV_CLOCKS_EVENT_REASONS in DCGM 3.3+),
//     DCGM_FI_PROF_SM_ACTIVE, DCGM_FI_PROF_SM_OCCUPANCY,
//     DCGM_FI_PROF_NVLINK_TX/RX_BYTES
//
// All DCGM_FI_PROF_* fields additionally require DCP profiling (Volta+).

// gpuSaturationByLabels is the grouping shared by every GPU saturation
// query: container attribution, GPU identity, and MIG instance labels.
// Grouping by a label that is absent from a series yields an empty value,
// so non-MIG series pass through unchanged.
const gpuSaturationByLabels = `container, pod, namespace, device, modelName, UUID, GPU_I_PROFILE, GPU_I_ID, uid`

// gpuThrottleViolationMetrics maps each DCGM violation counter to its
// canonical reason name. The counters accumulate microseconds spent
// throttled, so increase(counter[window]) / window-in-microseconds is the
// fraction of the window spent throttled for that reason.
var gpuThrottleViolationMetrics = []struct {
	Metric string
	Reason string
}{
	{Metric: "DCGM_FI_DEV_POWER_VIOLATION", Reason: opencost.GPUThrottleViolationPower},
	{Metric: "DCGM_FI_DEV_THERMAL_VIOLATION", Reason: opencost.GPUThrottleViolationThermal},
	{Metric: "DCGM_FI_DEV_SYNC_BOOST_VIOLATION", Reason: opencost.GPUThrottleViolationSyncBoost},
	{Metric: "DCGM_FI_DEV_BOARD_LIMIT_VIOLATION", Reason: opencost.GPUThrottleViolationBoardLimit},
}

// buildGPUThrottleViolationQuery returns one query producing a series per
// (GPU, container, reason): the fraction of the window each violation
// counter accumulated. Branches for the four violation metrics are joined
// with "or" and tagged with a constant "reason" label so a single decoder
// handles all of them.
func buildGPUThrottleViolationQuery(clusterFilter, durStr, clusterLabel string, windowSeconds float64) string {
	windowMicros := windowSeconds * 1e6
	branches := make([]string, 0, len(gpuThrottleViolationMetrics))
	for _, violation := range gpuThrottleViolationMetrics {
		branches = append(branches, fmt.Sprintf(
			`label_replace(avg(increase(%s{container!="",%s}[%s])) by (%s, %s) / %g, "reason", "%s", "", "")`,
			violation.Metric, clusterFilter, durStr, gpuSaturationByLabels, clusterLabel, windowMicros, violation.Reason,
		))
	}
	return strings.Join(branches, " or ")
}

// buildGPUThrottleReasonQuery returns one query producing a series per
// (GPU, container, reason): the fraction of window samples in which the
// reason bit was set in the clock throttle reasons bitmask. The bit test
// floor(mask / bit) % 2 is evaluated per sample via a subquery at the
// configured resolution. Both the pre-3.3 and post-3.3 DCGM field names are
// queried; at most one exists per dcgm-exporter version.
func buildGPUThrottleReasonQuery(clusterFilter, durStr, clusterLabel string, minsPerResolution int) string {
	branches := make([]string, 0, len(opencost.GPUThrottleReasons))
	for _, reason := range opencost.GPUThrottleReasons {
		branches = append(branches, fmt.Sprintf(
			`label_replace(avg(avg_over_time(((floor((DCGM_FI_DEV_CLOCK_THROTTLE_REASONS{container!="",%s} or DCGM_FI_DEV_CLOCKS_EVENT_REASONS{container!="",%s}) / %d)) %% 2)[%s:%dm])) by (%s, %s), "reason", "%s", "", "")`,
			clusterFilter, clusterFilter, reason.Bit, durStr, minsPerResolution, gpuSaturationByLabels, clusterLabel, reason.Name,
		))
	}
	return strings.Join(branches, " or ")
}

// queryGPUSaturation centralizes the shared shape of every GPU saturation
// query method: log it, then issue it at the window end.
func (pds *PrometheusMetricsQuerier) queryGPUSaturation(queryName, query string, end time.Time) *source.Future[source.GPUSaturationResult] {
	log.Debugf(PrometheusMetricsQueryLogFormat, queryName, end.Unix(), query)

	ctx := pds.promContexts.NewNamedContext(AllocationContextName)
	return source.NewFuture(source.DecodeGPUSaturationResult, ctx.QueryAtTime(query, end))
}

// mustDurationString panics like every other querier when the window cannot
// be expressed as a duration string.
func mustDurationString(queryName string, start, end time.Time) string {
	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}
	return durStr
}

// QueryGPUThrottleViolationRatio queries the fraction of the window each GPU
// spent throttled, per reason, from the DCGM violation counters (default
// dcgm-exporter configuration).
func (pds *PrometheusMetricsQuerier) QueryGPUThrottleViolationRatio(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	const queryName = "QueryGPUThrottleViolationRatio"
	cfg := pds.promConfig

	durStr := mustDurationString(queryName, start, end)
	query := buildGPUThrottleViolationQuery(cfg.ClusterFilter, durStr, cfg.ClusterLabel, end.Sub(start).Seconds())
	return pds.queryGPUSaturation(queryName, query, end)
}

// QueryGPUThrottleReasonRatio queries the fraction of the window each
// saturation-relevant bit of the clock throttle reasons bitmask was set.
// Requires DCGM_FI_DEV_CLOCK_THROTTLE_REASONS (or its DCGM 3.3+ rename) to
// be enabled in the dcgm-exporter configuration.
func (pds *PrometheusMetricsQuerier) QueryGPUThrottleReasonRatio(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	const queryName = "QueryGPUThrottleReasonRatio"
	cfg := pds.promConfig

	minsPerResolution := cfg.DataResolutionMinutes
	durStr := pds.durationStringFor(start, end, minsPerResolution, false)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	query := buildGPUThrottleReasonQuery(cfg.ClusterFilter, durStr, cfg.ClusterLabel, minsPerResolution)
	return pds.queryGPUSaturation(queryName, query, end)
}

// QueryGPUMemoryUsedRatioAvg queries average framebuffer occupancy over the
// window: FB_USED / (FB_USED + FB_FREE). Default dcgm-exporter configuration.
func (pds *PrometheusMetricsQuerier) QueryGPUMemoryUsedRatioAvg(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	const queryName = "QueryGPUMemoryUsedRatioAvg"
	const queryFmt = `avg(avg_over_time(DCGM_FI_DEV_FB_USED{container!="",%s}[%s])) by (%s, %s) / (avg(avg_over_time(DCGM_FI_DEV_FB_USED{container!="",%s}[%s])) by (%s, %s) + avg(avg_over_time(DCGM_FI_DEV_FB_FREE{container!="",%s}[%s])) by (%s, %s))`
	cfg := pds.promConfig

	durStr := mustDurationString(queryName, start, end)
	query := fmt.Sprintf(queryFmt,
		cfg.ClusterFilter, durStr, gpuSaturationByLabels, cfg.ClusterLabel,
		cfg.ClusterFilter, durStr, gpuSaturationByLabels, cfg.ClusterLabel,
		cfg.ClusterFilter, durStr, gpuSaturationByLabels, cfg.ClusterLabel,
	)
	return pds.queryGPUSaturation(queryName, query, end)
}

// QueryGPUMemoryUsedRatioMax queries peak framebuffer occupancy over the
// window. The denominator uses window averages, which is exact because
// FB_USED + FB_FREE is the fixed framebuffer capacity.
func (pds *PrometheusMetricsQuerier) QueryGPUMemoryUsedRatioMax(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	const queryName = "QueryGPUMemoryUsedRatioMax"
	const queryFmt = `max(max_over_time(DCGM_FI_DEV_FB_USED{container!="",%s}[%s])) by (%s, %s) / (avg(avg_over_time(DCGM_FI_DEV_FB_USED{container!="",%s}[%s])) by (%s, %s) + avg(avg_over_time(DCGM_FI_DEV_FB_FREE{container!="",%s}[%s])) by (%s, %s))`
	cfg := pds.promConfig

	durStr := mustDurationString(queryName, start, end)
	query := fmt.Sprintf(queryFmt,
		cfg.ClusterFilter, durStr, gpuSaturationByLabels, cfg.ClusterLabel,
		cfg.ClusterFilter, durStr, gpuSaturationByLabels, cfg.ClusterLabel,
		cfg.ClusterFilter, durStr, gpuSaturationByLabels, cfg.ClusterLabel,
	)
	return pds.queryGPUSaturation(queryName, query, end)
}

// QueryGPUMemoryPressureRatio queries the fraction of window samples in
// which framebuffer occupancy exceeded the configured threshold.
func (pds *PrometheusMetricsQuerier) QueryGPUMemoryPressureRatio(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	const queryName = "QueryGPUMemoryPressureRatio"
	const queryFmt = `avg(avg_over_time(((DCGM_FI_DEV_FB_USED{container!="",%s} / (DCGM_FI_DEV_FB_USED{container!="",%s} + DCGM_FI_DEV_FB_FREE{container!="",%s})) >= bool %g)[%s:%dm])) by (%s, %s)`
	cfg := pds.promConfig

	minsPerResolution := cfg.DataResolutionMinutes
	durStr := pds.durationStringFor(start, end, minsPerResolution, false)
	if durStr == "" {
		panic(fmt.Sprintf("failed to parse duration string passed to %s", queryName))
	}

	query := fmt.Sprintf(queryFmt,
		cfg.ClusterFilter, cfg.ClusterFilter, cfg.ClusterFilter,
		cfg.GPUMemorySaturationThreshold, durStr, minsPerResolution,
		gpuSaturationByLabels, cfg.ClusterLabel,
	)
	return pds.queryGPUSaturation(queryName, query, end)
}

// QueryGPUXIDErrorCount queries the number of XID error events observed in
// the window. DCGM_FI_DEV_XID_ERRORS reports the most recent XID code, so
// this counts changes of that value; consecutive identical errors are
// undercounted. Default dcgm-exporter configuration.
func (pds *PrometheusMetricsQuerier) QueryGPUXIDErrorCount(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	const queryName = "QueryGPUXIDErrorCount"
	const queryFmt = `sum(changes(DCGM_FI_DEV_XID_ERRORS{container!="",%s}[%s])) by (%s, %s)`
	cfg := pds.promConfig

	durStr := mustDurationString(queryName, start, end)
	query := fmt.Sprintf(queryFmt, cfg.ClusterFilter, durStr, gpuSaturationByLabels, cfg.ClusterLabel)
	return pds.queryGPUSaturation(queryName, query, end)
}

// queryGPUGaugeOverTime is the shared implementation for the DCP profiling
// gauges that need only an avg or max over the window.
func (pds *PrometheusMetricsQuerier) queryGPUGaugeOverTime(queryName, metric, agg string, start, end time.Time) *source.Future[source.GPUSaturationResult] {
	const queryFmt = `%s(%s_over_time(%s{container!="",%s}[%s])) by (%s, %s)`
	cfg := pds.promConfig

	durStr := mustDurationString(queryName, start, end)
	query := fmt.Sprintf(queryFmt, agg, agg, metric, cfg.ClusterFilter, durStr, gpuSaturationByLabels, cfg.ClusterLabel)
	return pds.queryGPUSaturation(queryName, query, end)
}

// queryGPUCounterRate is the shared implementation for the DCP byte
// counters reported as average bytes/sec over the window.
func (pds *PrometheusMetricsQuerier) queryGPUCounterRate(queryName, metric string, start, end time.Time) *source.Future[source.GPUSaturationResult] {
	const queryFmt = `avg(rate(%s{container!="",%s}[%s])) by (%s, %s)`
	cfg := pds.promConfig

	durStr := mustDurationString(queryName, start, end)
	query := fmt.Sprintf(queryFmt, metric, cfg.ClusterFilter, durStr, gpuSaturationByLabels, cfg.ClusterLabel)
	return pds.queryGPUSaturation(queryName, query, end)
}

// QueryGPUDRAMActiveAvg queries the average ratio of cycles the device
// memory interface was active. Requires DCP profiling.
func (pds *PrometheusMetricsQuerier) QueryGPUDRAMActiveAvg(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	return pds.queryGPUGaugeOverTime("QueryGPUDRAMActiveAvg", "DCGM_FI_PROF_DRAM_ACTIVE", "avg", start, end)
}

// QueryGPUDRAMActiveMax queries the peak ratio of cycles the device memory
// interface was active. Requires DCP profiling.
func (pds *PrometheusMetricsQuerier) QueryGPUDRAMActiveMax(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	return pds.queryGPUGaugeOverTime("QueryGPUDRAMActiveMax", "DCGM_FI_PROF_DRAM_ACTIVE", "max", start, end)
}

// QueryGPUSMActiveAvg queries the average ratio of cycles at least one warp
// was assigned to any SM. Requires DCP profiling and explicit enablement in
// the dcgm-exporter configuration.
func (pds *PrometheusMetricsQuerier) QueryGPUSMActiveAvg(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	return pds.queryGPUGaugeOverTime("QueryGPUSMActiveAvg", "DCGM_FI_PROF_SM_ACTIVE", "avg", start, end)
}

// QueryGPUSMOccupancyAvg queries the average ratio of resident warps to the
// SM maximum. Requires DCP profiling and explicit enablement in the
// dcgm-exporter configuration.
func (pds *PrometheusMetricsQuerier) QueryGPUSMOccupancyAvg(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	return pds.queryGPUGaugeOverTime("QueryGPUSMOccupancyAvg", "DCGM_FI_PROF_SM_OCCUPANCY", "avg", start, end)
}

// QueryGPUPCIeTxBytesAvg queries average PCIe transmit throughput in
// bytes/sec. Requires DCP profiling.
func (pds *PrometheusMetricsQuerier) QueryGPUPCIeTxBytesAvg(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	return pds.queryGPUCounterRate("QueryGPUPCIeTxBytesAvg", "DCGM_FI_PROF_PCIE_TX_BYTES", start, end)
}

// QueryGPUPCIeRxBytesAvg queries average PCIe receive throughput in
// bytes/sec. Requires DCP profiling.
func (pds *PrometheusMetricsQuerier) QueryGPUPCIeRxBytesAvg(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	return pds.queryGPUCounterRate("QueryGPUPCIeRxBytesAvg", "DCGM_FI_PROF_PCIE_RX_BYTES", start, end)
}

// QueryGPUNVLinkTxBytesAvg queries average NVLink transmit throughput in
// bytes/sec. Requires DCP profiling and explicit enablement in the
// dcgm-exporter configuration.
func (pds *PrometheusMetricsQuerier) QueryGPUNVLinkTxBytesAvg(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	return pds.queryGPUCounterRate("QueryGPUNVLinkTxBytesAvg", "DCGM_FI_PROF_NVLINK_TX_BYTES", start, end)
}

// QueryGPUNVLinkRxBytesAvg queries average NVLink receive throughput in
// bytes/sec. Requires DCP profiling and explicit enablement in the
// dcgm-exporter configuration.
func (pds *PrometheusMetricsQuerier) QueryGPUNVLinkRxBytesAvg(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	return pds.queryGPUCounterRate("QueryGPUNVLinkRxBytesAvg", "DCGM_FI_PROF_NVLINK_RX_BYTES", start, end)
}
