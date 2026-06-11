package collector

import (
	"math"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
)

// gpuSaturationMockProvider builds a store with one hour of DCGM saturation
// samples for a single GPU container. SM_ACTIVE, SM_OCCUPANCY, and NVLink
// metrics are deliberately never updated to exercise absent-signal behavior.
func gpuSaturationMockProvider(t *testing.T) (StoreProvider, time.Time, time.Time) {
	t.Helper()
	t.Setenv("GPU_MEMORY_SATURATION_THRESHOLD", "0.6")

	start, _ := time.Parse(time.RFC3339, Start1Str)
	end, _ := time.Parse(time.RFC3339, End1Str)

	gpuInfo := map[string]string{
		source.NamespaceLabel:   "namespace1",
		source.PodLabel:         "pod1",
		source.PodUIDLabel:      "pod-uuid1",
		source.ContainerLabel:   "container1",
		source.DeviceLabel:      "nvidia0",
		source.ModelNameLabel:   "Tesla T4",
		source.UUIDLabel:        "GPU-1",
		source.MIGProfileLabel:  "",
		source.MIGInstanceLabel: "",
	}

	store := NewOpenCostMetricStore()

	// power violation counter: 1.8e9us accumulated over a 3.6e9us window
	store.Update(metric.DCGMFIDEVPOWERVIOLATION, gpuInfo, 0, start, nil)
	store.Update(metric.DCGMFIDEVPOWERVIOLATION, gpuInfo, 1.8e9, end, nil)

	// throttle bitmask (legacy field name): sw_power_cap set in 1 of 2 samples
	store.Update(metric.DCGMFIDEVCLOCKTHROTTLEREASONS, gpuInfo, 0x4, start, nil)
	store.Update(metric.DCGMFIDEVCLOCKTHROTTLEREASONS, gpuInfo, 0x0, end, nil)

	// framebuffer occupancy ratio, as synthesized from FB_USED/FB_FREE per
	// scrape by GPUMemoryUsedRatioSynthesizer (see synthetic package tests
	// for the join itself): avg 0.625, max 0.75, half of samples >= 0.6
	store.Update(metric.OpencostGPUMemoryUsedRatio, gpuInfo, 0.5, start, nil)
	store.Update(metric.OpencostGPUMemoryUsedRatio, gpuInfo, 0.75, end, nil)

	// one XID error transition
	store.Update(metric.DCGMFIDEVXIDERRORS, gpuInfo, 0, start, nil)
	store.Update(metric.DCGMFIDEVXIDERRORS, gpuInfo, 13, end, nil)

	// DRAM activity gauge
	store.Update(metric.DCGMFIPROFDRAMACTIVE, gpuInfo, 0.5, start, nil)
	store.Update(metric.DCGMFIPROFDRAMACTIVE, gpuInfo, 0.7, end, nil)

	// PCIe tx counter: 3.6e12 bytes over 3600s = 1e9 bytes/sec
	store.Update(metric.DCGMFIPROFPCIETXBYTES, gpuInfo, 0, start, nil)
	store.Update(metric.DCGMFIPROFPCIETXBYTES, gpuInfo, 3.6e12, end, nil)

	return &MockStoreProvider{metricsCollector: store}, start, end
}

func awaitGPUSaturation(t *testing.T, f *source.Future[source.GPUSaturationResult]) []*source.GPUSaturationResult {
	t.Helper()
	res, err := f.Await()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return res
}

func requireValue(t *testing.T, results []*source.GPUSaturationResult, want float64) {
	t.Helper()
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	got := results[0].Data[0].Value
	if math.Abs(got-want) > 1e-9 {
		t.Errorf("value = %v, want %v", got, want)
	}
	if results[0].UUID != "GPU-1" || results[0].Container != "container1" {
		t.Errorf("result lost GPU identity labels: %+v", results[0])
	}
}

func TestCollectorMetricsQuerier_GPUThrottleViolationRatio(t *testing.T) {
	provider, start, end := gpuSaturationMockProvider(t)
	c := collectorMetricsQuerier{collectorProvider: provider}

	results := awaitGPUSaturation(t, c.QueryGPUThrottleViolationRatio(start, end))

	// only the power violation counter was scraped
	requireValue(t, results, 0.5)
	if results[0].Reason != opencost.GPUThrottleViolationPower {
		t.Errorf("Reason = %q, want %q", results[0].Reason, opencost.GPUThrottleViolationPower)
	}
}

func TestCollectorMetricsQuerier_GPUThrottleReasonRatio(t *testing.T) {
	provider, start, end := gpuSaturationMockProvider(t)
	c := collectorMetricsQuerier{collectorProvider: provider}

	results := awaitGPUSaturation(t, c.QueryGPUThrottleReasonRatio(start, end))

	// the legacy bitmask field was scraped, so every reason bit reports
	got := map[string]float64{}
	for _, res := range results {
		got[res.Reason] = res.Data[0].Value
	}
	if len(got) != len(opencost.GPUThrottleReasons) {
		t.Fatalf("expected %d reasons, got %d: %v", len(opencost.GPUThrottleReasons), len(got), got)
	}
	for _, reason := range opencost.GPUThrottleReasons {
		want := 0.0
		if reason.Name == opencost.GPUThrottleReasonSwPowerCap {
			want = 0.5
		}
		if math.Abs(got[reason.Name]-want) > 1e-9 {
			t.Errorf("reason %q ratio = %v, want %v", reason.Name, got[reason.Name], want)
		}
	}
}

func TestCollectorMetricsQuerier_GPUMemoryUsedRatio(t *testing.T) {
	provider, start, end := gpuSaturationMockProvider(t)
	c := collectorMetricsQuerier{collectorProvider: provider}

	// avg of per-sample ratios (0.5, 0.75)
	requireValue(t, awaitGPUSaturation(t, c.QueryGPUMemoryUsedRatioAvg(start, end)), 0.625)
	// max of per-sample ratios
	requireValue(t, awaitGPUSaturation(t, c.QueryGPUMemoryUsedRatioMax(start, end)), 0.75)
}

func TestCollectorMetricsQuerier_GPUMemoryPressureRatio(t *testing.T) {
	provider, start, end := gpuSaturationMockProvider(t)
	c := collectorMetricsQuerier{collectorProvider: provider}

	// threshold configured to 0.6: one of two samples (0.75) is at or above
	requireValue(t, awaitGPUSaturation(t, c.QueryGPUMemoryPressureRatio(start, end)), 0.5)
}

func TestCollectorMetricsQuerier_GPUXIDErrorCount(t *testing.T) {
	provider, start, end := gpuSaturationMockProvider(t)
	c := collectorMetricsQuerier{collectorProvider: provider}

	requireValue(t, awaitGPUSaturation(t, c.QueryGPUXIDErrorCount(start, end)), 1)
}

func TestCollectorMetricsQuerier_GPUDRAMActive(t *testing.T) {
	provider, start, end := gpuSaturationMockProvider(t)
	c := collectorMetricsQuerier{collectorProvider: provider}

	requireValue(t, awaitGPUSaturation(t, c.QueryGPUDRAMActiveAvg(start, end)), 0.6)
	requireValue(t, awaitGPUSaturation(t, c.QueryGPUDRAMActiveMax(start, end)), 0.7)
}

func TestCollectorMetricsQuerier_GPUPCIeTxBytesAvg(t *testing.T) {
	provider, start, end := gpuSaturationMockProvider(t)
	c := collectorMetricsQuerier{collectorProvider: provider}

	requireValue(t, awaitGPUSaturation(t, c.QueryGPUPCIeTxBytesAvg(start, end)), 1e9)
}

// TestCollectorMetricsQuerier_GPUDeviceMetrics verifies the device-level
// queries aggregate from the device-labeled DCGM series.
func TestCollectorMetricsQuerier_GPUDeviceMetrics(t *testing.T) {
	start, _ := time.Parse(time.RFC3339, Start1Str)
	end, _ := time.Parse(time.RFC3339, End1Str)

	deviceInfo := map[string]string{
		source.DeviceLabel:      "nvidia0",
		source.ModelNameLabel:   "Tesla T4",
		source.UUIDLabel:        "GPU-1",
		source.MIGProfileLabel:  "",
		source.MIGInstanceLabel: "",
	}
	store := NewOpenCostMetricStore()
	store.Update(metric.DCGMFIDEVPOWERUSAGE, deviceInfo, 120, start, nil)
	store.Update(metric.DCGMFIDEVPOWERUSAGE, deviceInfo, 160, end, nil)
	store.Update(metric.DCGMFIDEVGPUTEMP, deviceInfo, 55, start, nil)
	store.Update(metric.DCGMFIPROFGRENGINEACTIVE, deviceInfo, 0.4, start, nil)
	store.Update(metric.DCGMFIPROFGRENGINEACTIVE, deviceInfo, 0.9, end, nil)
	store.Update(metric.DCGMFIDEVFBUSED, deviceInfo, 1024, start, nil)
	store.Update(metric.DCGMFIDEVFBUSED, deviceInfo, 2048, end, nil)

	c := collectorMetricsQuerier{collectorProvider: &MockStoreProvider{metricsCollector: store}}

	checks := map[string]struct {
		future *source.Future[source.GPUDeviceMetricResult]
		want   float64
	}{
		"power avg":  {c.QueryGPUDevicePowerAvg(start, end), 140},
		"temp avg":   {c.QueryGPUDeviceTempAvg(start, end), 55},
		"usage avg":  {c.QueryGPUDeviceUsageAvg(start, end), 0.65},
		"usage max":  {c.QueryGPUDeviceUsageMax(start, end), 0.9},
		"memory avg": {c.QueryGPUDeviceMemoryUsedAvg(start, end), 1536},
		"memory max": {c.QueryGPUDeviceMemoryUsedMax(start, end), 2048},
	}
	for name, check := range checks {
		results := awaitGPUSaturation(t, check.future)
		if len(results) != 1 {
			t.Fatalf("%s: expected 1 result, got %d", name, len(results))
		}
		if got := results[0].Data[0].Value; math.Abs(got-check.want) > 1e-9 {
			t.Errorf("%s = %v, want %v", name, got, check.want)
		}
		if results[0].UUID != "GPU-1" {
			t.Errorf("%s: lost device identity: %+v", name, results[0])
		}
	}
}

// TestCollectorMetricsQuerier_GPUSaturationAbsentSignals verifies that
// signals whose DCGM fields were never scraped return no results instead of
// zeroes.
func TestCollectorMetricsQuerier_GPUSaturationAbsentSignals(t *testing.T) {
	provider, start, end := gpuSaturationMockProvider(t)
	c := collectorMetricsQuerier{collectorProvider: provider}

	absent := map[string]*source.Future[source.GPUSaturationResult]{
		"SMActiveAvg":      c.QueryGPUSMActiveAvg(start, end),
		"SMOccupancyAvg":   c.QueryGPUSMOccupancyAvg(start, end),
		"PCIeRxBytesAvg":   c.QueryGPUPCIeRxBytesAvg(start, end),
		"NVLinkTxBytesAvg": c.QueryGPUNVLinkTxBytesAvg(start, end),
		"NVLinkRxBytesAvg": c.QueryGPUNVLinkRxBytesAvg(start, end),
	}
	for name, future := range absent {
		if results := awaitGPUSaturation(t, future); len(results) != 0 {
			t.Errorf("%s: expected no results for unscraped metric, got %d", name, len(results))
		}
	}
}
