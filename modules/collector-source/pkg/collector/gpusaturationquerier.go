package collector

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric/aggregator"
)

// GPU saturation queries over the collector store. These mirror the
// prometheus-source queries: same DataSource method names, same
// GPUSaturationResult semantics. Signals whose DCGM field was never scraped
// produce no results, which downstream treats as absent rather than zero.

// gpuSaturationResultsFuture wraps a set of MetricResults into the shared
// GPUSaturationResult future shape.
func gpuSaturationResultsFuture(name string, results []*aggregator.MetricResult, err error) *source.Future[source.GPUSaturationResult] {
	queryResults := source.NewQueryResults(name)
	queryResults.Error = err
	for _, result := range results {
		queryResults.Results = append(queryResults.Results, result.ToQueryResult())
	}
	ch := make(source.QueryResultsChan, 1)
	ch <- queryResults
	return source.NewFuture(source.DecodeGPUSaturationResult, ch)
}

// queryGPUReasonTagged queries one collector per reason, tags each result
// with the reason label, and optionally transforms every value.
func (c *collectorMetricsQuerier) queryGPUReasonTagged(name string, start, end time.Time, idReasons map[metric.MetricCollectorID]string, transform func(float64) float64) *source.Future[source.GPUSaturationResult] {
	var tagged []*aggregator.MetricResult
	var firstErr error

	collector := c.collectorProvider.GetStore(start, end)
	if collector != nil {
		for id, reason := range idReasons {
			results, err := collector.Query(id)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			for _, result := range results {
				if result.MetricLabels == nil {
					result.MetricLabels = map[string]string{}
				}
				result.MetricLabels[source.ReasonLabel] = reason
				if transform != nil {
					for i := range result.Values {
						result.Values[i].Value = transform(result.Values[i].Value)
					}
				}
				tagged = append(tagged, result)
			}
		}
	}

	return gpuSaturationResultsFuture(name, tagged, firstErr)
}

// QueryGPUThrottleViolationRatio reports the fraction of the window each GPU
// spent throttled, per reason, from the DCGM violation microsecond counters.
func (c *collectorMetricsQuerier) QueryGPUThrottleViolationRatio(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	idReasons := make(map[metric.MetricCollectorID]string, len(gpuThrottleViolationCollectors))
	for _, violation := range gpuThrottleViolationCollectors {
		idReasons[violation.ID] = violation.Reason
	}

	windowMicros := float64(end.Sub(start).Microseconds())
	if windowMicros <= 0 {
		return gpuSaturationResultsFuture("GPUThrottleViolationRatio", nil, fmt.Errorf("invalid window for GPUThrottleViolationRatio: %s to %s", start, end))
	}

	return c.queryGPUReasonTagged("GPUThrottleViolationRatio", start, end, idReasons, func(increaseMicros float64) float64 {
		return increaseMicros / windowMicros
	})
}

// QueryGPUThrottleReasonRatio reports the fraction of scraped samples in
// which each saturation-relevant bit of the clock throttle reasons bitmask
// was set. Both DCGM field names are queried; at most one is ever scraped.
func (c *collectorMetricsQuerier) QueryGPUThrottleReasonRatio(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	idReasons := make(map[metric.MetricCollectorID]string, 2*len(opencost.GPUThrottleReasons))
	for _, metricName := range gpuThrottleBitmaskMetrics {
		for _, reason := range opencost.GPUThrottleReasons {
			idReasons[metric.GPUThrottleReasonCollectorID(metricName, reason.Name)] = reason.Name
		}
	}
	return c.queryGPUReasonTagged("GPUThrottleReasonRatio", start, end, idReasons, nil)
}

// gpuSaturationJoinKey builds a deterministic join key from a result's
// labels so framebuffer used/free series for the same GPU and container can
// be matched.
func gpuSaturationJoinKey(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(labels[k])
		sb.WriteByte(';')
	}
	return sb.String()
}

// firstValue returns the single aggregated value of a MetricResult.
func firstValue(result *aggregator.MetricResult) (float64, bool) {
	if result == nil || len(result.Values) == 0 {
		return 0, false
	}
	return result.Values[0].Value, true
}

// queryGPUMemoryUsedRatio joins a framebuffer numerator collector (used avg
// or used max, in MiB) against used+free averages to produce an occupancy
// ratio. FB_USED + FB_FREE is the fixed framebuffer capacity, so averages
// are an exact denominator. GPUs missing either component are skipped.
func (c *collectorMetricsQuerier) queryGPUMemoryUsedRatio(name string, start, end time.Time, numeratorID metric.MetricCollectorID) *source.Future[source.GPUSaturationResult] {
	var ratios []*aggregator.MetricResult
	var firstErr error

	collector := c.collectorProvider.GetStore(start, end)
	if collector != nil {
		numeratorResults, errNum := collector.Query(numeratorID)
		usedAvgResults, errUsed := collector.Query(metric.GPUMemoryUsedAvgID)
		freeAvgResults, errFree := collector.Query(metric.GPUMemoryFreeAvgID)
		for _, err := range []error{errNum, errUsed, errFree} {
			if err != nil && firstErr == nil {
				firstErr = err
			}
		}

		usedAvgByKey := make(map[string]*aggregator.MetricResult, len(usedAvgResults))
		for _, result := range usedAvgResults {
			usedAvgByKey[gpuSaturationJoinKey(result.MetricLabels)] = result
		}
		freeAvgByKey := make(map[string]*aggregator.MetricResult, len(freeAvgResults))
		for _, result := range freeAvgResults {
			freeAvgByKey[gpuSaturationJoinKey(result.MetricLabels)] = result
		}

		for _, numerator := range numeratorResults {
			key := gpuSaturationJoinKey(numerator.MetricLabels)
			usedAvg, okUsed := firstValue(usedAvgByKey[key])
			freeAvg, okFree := firstValue(freeAvgByKey[key])
			numeratorValue, okNum := firstValue(numerator)
			if !okUsed || !okFree || !okNum {
				continue
			}
			total := usedAvg + freeAvg
			if total <= 0 {
				continue
			}
			numerator.Values = []aggregator.MetricValue{{Value: numeratorValue / total}}
			ratios = append(ratios, numerator)
		}
	}

	return gpuSaturationResultsFuture(name, ratios, firstErr)
}

// QueryGPUMemoryUsedRatioAvg reports average framebuffer occupancy over the
// window: FB_USED / (FB_USED + FB_FREE).
func (c *collectorMetricsQuerier) QueryGPUMemoryUsedRatioAvg(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	return c.queryGPUMemoryUsedRatio("GPUMemoryUsedRatioAvg", start, end, metric.GPUMemoryUsedAvgID)
}

// QueryGPUMemoryUsedRatioMax reports peak framebuffer occupancy over the
// window.
func (c *collectorMetricsQuerier) QueryGPUMemoryUsedRatioMax(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	return c.queryGPUMemoryUsedRatio("GPUMemoryUsedRatioMax", start, end, metric.GPUMemoryUsedMaxID)
}

// QueryGPUMemoryPressureRatio requires evaluating the framebuffer occupancy
// ratio per sample across two metrics, which the collector aggregation
// framework cannot express today. It intentionally returns no results so
// the signal stays absent rather than fabricated; the prometheus-source
// implementation provides it via subquery.
func (c *collectorMetricsQuerier) QueryGPUMemoryPressureRatio(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	return gpuSaturationResultsFuture("GPUMemoryPressureRatio", nil, nil)
}

// QueryGPUXIDErrorCount reports the number of XID error transitions
// observed in the window.
func (c *collectorMetricsQuerier) QueryGPUXIDErrorCount(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	return queryCollector(c, start, end, metric.GPUXIDErrorCountID, source.DecodeGPUSaturationResult)
}

// QueryGPUDRAMActiveAvg reports the average ratio of cycles the device
// memory interface was active. Requires DCP profiling.
func (c *collectorMetricsQuerier) QueryGPUDRAMActiveAvg(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	return queryCollector(c, start, end, metric.GPUDRAMActiveAvgID, source.DecodeGPUSaturationResult)
}

// QueryGPUDRAMActiveMax reports the peak ratio of cycles the device memory
// interface was active. Requires DCP profiling.
func (c *collectorMetricsQuerier) QueryGPUDRAMActiveMax(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	return queryCollector(c, start, end, metric.GPUDRAMActiveMaxID, source.DecodeGPUSaturationResult)
}

// QueryGPUSMActiveAvg reports the average ratio of cycles at least one warp
// was resident on any SM. Requires DCP profiling and explicit enablement.
func (c *collectorMetricsQuerier) QueryGPUSMActiveAvg(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	return queryCollector(c, start, end, metric.GPUSMActiveAvgID, source.DecodeGPUSaturationResult)
}

// QueryGPUSMOccupancyAvg reports the average ratio of resident warps to the
// SM maximum. Requires DCP profiling and explicit enablement.
func (c *collectorMetricsQuerier) QueryGPUSMOccupancyAvg(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	return queryCollector(c, start, end, metric.GPUSMOccupancyAvgID, source.DecodeGPUSaturationResult)
}

// QueryGPUPCIeTxBytesAvg reports average PCIe transmit throughput in
// bytes/sec. Requires DCP profiling.
func (c *collectorMetricsQuerier) QueryGPUPCIeTxBytesAvg(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	return queryCollector(c, start, end, metric.GPUPCIeTxBytesAvgID, source.DecodeGPUSaturationResult)
}

// QueryGPUPCIeRxBytesAvg reports average PCIe receive throughput in
// bytes/sec. Requires DCP profiling.
func (c *collectorMetricsQuerier) QueryGPUPCIeRxBytesAvg(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	return queryCollector(c, start, end, metric.GPUPCIeRxBytesAvgID, source.DecodeGPUSaturationResult)
}

// QueryGPUNVLinkTxBytesAvg reports average NVLink transmit throughput in
// bytes/sec. Requires DCP profiling and explicit enablement.
func (c *collectorMetricsQuerier) QueryGPUNVLinkTxBytesAvg(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	return queryCollector(c, start, end, metric.GPUNVLinkTxBytesAvgID, source.DecodeGPUSaturationResult)
}

// QueryGPUNVLinkRxBytesAvg reports average NVLink receive throughput in
// bytes/sec. Requires DCP profiling and explicit enablement.
func (c *collectorMetricsQuerier) QueryGPUNVLinkRxBytesAvg(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	return queryCollector(c, start, end, metric.GPUNVLinkRxBytesAvgID, source.DecodeGPUSaturationResult)
}
