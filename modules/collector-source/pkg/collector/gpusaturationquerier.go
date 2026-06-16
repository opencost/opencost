package collector

import (
	"fmt"
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

// QueryGPUMemoryUsedRatioAvg reports average framebuffer occupancy over the
// window: FB_USED / (FB_USED + FB_FREE), aggregated from the per-sample
// occupancy ratio synthesized at scrape time.
func (c *collectorMetricsQuerier) QueryGPUMemoryUsedRatioAvg(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	return queryCollector(c, start, end, metric.GPUMemoryUsedAvgID, source.DecodeGPUSaturationResult)
}

// QueryGPUMemoryUsedRatioMax reports peak framebuffer occupancy over the
// window.
func (c *collectorMetricsQuerier) QueryGPUMemoryUsedRatioMax(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	return queryCollector(c, start, end, metric.GPUMemoryUsedMaxID, source.DecodeGPUSaturationResult)
}

// QueryGPUMemoryPressureRatio reports the fraction of scraped samples in
// which framebuffer occupancy was at or above the configured threshold,
// from the same synthesized per-sample occupancy ratio.
func (c *collectorMetricsQuerier) QueryGPUMemoryPressureRatio(start, end time.Time) *source.Future[source.GPUSaturationResult] {
	return queryCollector(c, start, end, metric.GPUMemoryPressureRatioID, source.DecodeGPUSaturationResult)
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

// Device-level GPU metric queries (DeviceInfo / DevicePerformance support).

// QueryGPUDevicePowerAvg reports average device power draw in watts.
func (c *collectorMetricsQuerier) QueryGPUDevicePowerAvg(start, end time.Time) *source.Future[source.GPUDeviceMetricResult] {
	return queryCollector(c, start, end, metric.GPUDevicePowerAvgID, source.DecodeGPUDeviceMetricResult)
}

// QueryGPUDeviceTempAvg reports average device temperature in Celsius.
func (c *collectorMetricsQuerier) QueryGPUDeviceTempAvg(start, end time.Time) *source.Future[source.GPUDeviceMetricResult] {
	return queryCollector(c, start, end, metric.GPUDeviceTempAvgID, source.DecodeGPUDeviceMetricResult)
}

// QueryGPUDeviceUsageAvg reports average device-level compute utilization
// as a 0-1 ratio.
func (c *collectorMetricsQuerier) QueryGPUDeviceUsageAvg(start, end time.Time) *source.Future[source.GPUDeviceMetricResult] {
	return queryCollector(c, start, end, metric.GPUDeviceUsageAvgID, source.DecodeGPUDeviceMetricResult)
}

// QueryGPUDeviceUsageMax reports peak device-level compute utilization as a
// 0-1 ratio.
func (c *collectorMetricsQuerier) QueryGPUDeviceUsageMax(start, end time.Time) *source.Future[source.GPUDeviceMetricResult] {
	return queryCollector(c, start, end, metric.GPUDeviceUsageMaxID, source.DecodeGPUDeviceMetricResult)
}

// QueryGPUDeviceMemoryUsedAvg reports average framebuffer used in MiB.
func (c *collectorMetricsQuerier) QueryGPUDeviceMemoryUsedAvg(start, end time.Time) *source.Future[source.GPUDeviceMetricResult] {
	return queryCollector(c, start, end, metric.GPUDeviceMemoryUsedAvgID, source.DecodeGPUDeviceMetricResult)
}

// QueryGPUDeviceMemoryUsedMax reports peak framebuffer used in MiB.
func (c *collectorMetricsQuerier) QueryGPUDeviceMemoryUsedMax(start, end time.Time) *source.Future[source.GPUDeviceMetricResult] {
	return queryCollector(c, start, end, metric.GPUDeviceMemoryUsedMaxID, source.DecodeGPUDeviceMetricResult)
}
