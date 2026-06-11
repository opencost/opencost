package collector

import (
	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric/aggregator"
)

// gpuMemorySaturationThreshold returns the framebuffer occupancy ratio
// above which GPU memory counts as pressured, mirroring the
// prometheus-source configuration. Values outside (0, 1] fall back to the
// default of 0.9.
func gpuMemorySaturationThreshold() float64 {
	threshold := coreenv.GetFloat64("GPU_MEMORY_SATURATION_THRESHOLD", 0.9)
	if threshold <= 0.0 || threshold > 1.0 {
		return 0.9
	}
	return threshold
}

// GPU saturation collectors
//
// These collectors aggregate USE-method GPU saturation signals from
// dcgm-exporter scrapes. When a DCGM field is absent from the scrape (not in
// the exporter config, or no DCP profiling support) the corresponding
// collector simply accumulates nothing and its query returns no results,
// which downstream treats as "signal absent" rather than zero.

// gpuSaturationLabels is the label set shared by every GPU saturation
// collector: container attribution, GPU identity, and MIG instance labels.
// Labels absent from a scrape resolve to empty strings.
var gpuSaturationLabels = []string{
	source.NamespaceLabel,
	source.PodLabel,
	source.UIDLabel,
	source.ContainerLabel,
	source.DeviceLabel,
	source.ModelNameLabel,
	source.UUIDLabel,
	source.MIGProfileLabel,
	source.MIGInstanceLabel,
}

func gpuSaturationFilter(labels map[string]string) bool {
	return labels[source.ContainerLabel] != ""
}

// gpuThrottleViolationCollectorMetrics maps each violation collector ID to
// its DCGM source metric. The counters accumulate microseconds spent
// throttled; the querier converts the windowed increase into a fraction of
// the window and tags the reason.
var gpuThrottleViolationCollectors = []struct {
	ID     metric.MetricCollectorID
	Metric string
	Reason string
}{
	{ID: metric.GPUThrottleViolationPowerID, Metric: metric.DCGMFIDEVPOWERVIOLATION, Reason: opencost.GPUThrottleViolationPower},
	{ID: metric.GPUThrottleViolationThermalID, Metric: metric.DCGMFIDEVTHERMALVIOLATION, Reason: opencost.GPUThrottleViolationThermal},
	{ID: metric.GPUThrottleViolationSyncBoostID, Metric: metric.DCGMFIDEVSYNCBOOSTVIOLATION, Reason: opencost.GPUThrottleViolationSyncBoost},
	{ID: metric.GPUThrottleViolationBoardLimitID, Metric: metric.DCGMFIDEVBOARDLIMITVIOLATION, Reason: opencost.GPUThrottleViolationBoardLimit},
}

// gpuThrottleBitmaskMetrics enumerates both names of the DCGM clock throttle
// reasons bitmask field; at most one is scraped per dcgm-exporter version.
var gpuThrottleBitmaskMetrics = []string{
	metric.DCGMFIDEVCLOCKTHROTTLEREASONS,
	metric.DCGMFIDEVCLOCKSEVENTREASONS,
}

func newGPUSaturationCollector(id metric.MetricCollectorID, metricName string, factory aggregator.MetricAggregatorFactory) *metric.MetricCollector {
	return metric.NewMetricCollector(
		id,
		metricName,
		gpuSaturationLabels,
		factory,
		gpuSaturationFilter,
	)
}

// NewGPUSaturationMetricCollectors returns every collector needed for the
// GPU saturation signals.
func NewGPUSaturationMetricCollectors() []*metric.MetricCollector {
	collectors := []*metric.MetricCollector{
		// framebuffer occupancy over the synthetic per-sample ratio metric
		// joined from FB_USED/FB_FREE at scrape time (see
		// synthetic.GPUMemoryUsedRatioSynthesizer)
		newGPUSaturationCollector(metric.GPUMemoryUsedAvgID, metric.OpencostGPUMemoryUsedRatio, aggregator.AverageOverTime),
		newGPUSaturationCollector(metric.GPUMemoryUsedMaxID, metric.OpencostGPUMemoryUsedRatio, aggregator.MaxOverTime),
		newGPUSaturationCollector(metric.GPUMemoryPressureRatioID, metric.OpencostGPUMemoryUsedRatio, aggregator.AboveThresholdRatio(gpuMemorySaturationThreshold())),
		// XID error events: count value transitions of the last-error gauge
		newGPUSaturationCollector(metric.GPUXIDErrorCountID, metric.DCGMFIDEVXIDERRORS, aggregator.Changes),
		// DCP profiling gauges
		newGPUSaturationCollector(metric.GPUDRAMActiveAvgID, metric.DCGMFIPROFDRAMACTIVE, aggregator.AverageOverTime),
		newGPUSaturationCollector(metric.GPUDRAMActiveMaxID, metric.DCGMFIPROFDRAMACTIVE, aggregator.MaxOverTime),
		newGPUSaturationCollector(metric.GPUSMActiveAvgID, metric.DCGMFIPROFSMACTIVE, aggregator.AverageOverTime),
		newGPUSaturationCollector(metric.GPUSMOccupancyAvgID, metric.DCGMFIPROFSMOCCUPANCY, aggregator.AverageOverTime),
		// DCP byte counters as average bytes/sec
		newGPUSaturationCollector(metric.GPUPCIeTxBytesAvgID, metric.DCGMFIPROFPCIETXBYTES, aggregator.Rate),
		newGPUSaturationCollector(metric.GPUPCIeRxBytesAvgID, metric.DCGMFIPROFPCIERXBYTES, aggregator.Rate),
		newGPUSaturationCollector(metric.GPUNVLinkTxBytesAvgID, metric.DCGMFIPROFNVLINKTXBYTES, aggregator.Rate),
		newGPUSaturationCollector(metric.GPUNVLinkRxBytesAvgID, metric.DCGMFIPROFNVLINKRXBYTES, aggregator.Rate),
	}

	// throttle violation counters: windowed increase, normalized by the
	// querier
	for _, violation := range gpuThrottleViolationCollectors {
		collectors = append(collectors, newGPUSaturationCollector(violation.ID, violation.Metric, aggregator.Increase))
	}

	// throttle reasons bitmask: one bit-ratio collector per
	// (metric name, saturation-relevant reason)
	for _, metricName := range gpuThrottleBitmaskMetrics {
		for _, reason := range opencost.GPUThrottleReasons {
			collectors = append(collectors, newGPUSaturationCollector(
				metric.GPUThrottleReasonCollectorID(metricName, reason.Name),
				metricName,
				aggregator.BitSetRatio(reason.Bit),
			))
		}
	}

	return collectors
}
