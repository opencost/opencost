package prom

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/source"
)

// Device-level GPU metric queries supporting the DeviceInfo and
// DevicePerformance contracts on DCGMDevice: power draw, temperature,
// device-level compute utilization, and absolute memory used. Unlike the
// container-attributed saturation queries these group by device identity
// only, since the values describe the whole device (or MIG instance)
// regardless of which containers share it. All source fields are in the
// default dcgm-exporter configuration.

// gpuDeviceByLabels groups series by device identity (and MIG instance)
// without container attribution.
const gpuDeviceByLabels = `device, modelName, UUID, GPU_I_PROFILE, GPU_I_ID`

// queryGPUDeviceGauge issues an agg(agg_over_time(...)) query for a
// device-level DCGM gauge.
func (pds *PrometheusMetricsQuerier) queryGPUDeviceGauge(queryName, metric, agg string, start, end time.Time) *source.Future[source.GPUDeviceMetricResult] {
	const queryFmt = `%s(%s_over_time(%s{%s}[%s])) by (%s, %s)`
	cfg := pds.promConfig

	durStr := mustDurationString(queryName, start, end)
	query := fmt.Sprintf(queryFmt, agg, agg, metric, cfg.ClusterFilter, durStr, gpuDeviceByLabels, cfg.ClusterLabel)
	return pds.queryGPUSaturation(queryName, query, end)
}

// QueryGPUDevicePowerAvg queries average device power draw in watts
// (DCGM_FI_DEV_POWER_USAGE).
func (pds *PrometheusMetricsQuerier) QueryGPUDevicePowerAvg(start, end time.Time) *source.Future[source.GPUDeviceMetricResult] {
	return pds.queryGPUDeviceGauge("QueryGPUDevicePowerAvg", "DCGM_FI_DEV_POWER_USAGE", "avg", start, end)
}

// QueryGPUDeviceTempAvg queries average device temperature in degrees
// Celsius (DCGM_FI_DEV_GPU_TEMP).
func (pds *PrometheusMetricsQuerier) QueryGPUDeviceTempAvg(start, end time.Time) *source.Future[source.GPUDeviceMetricResult] {
	return pds.queryGPUDeviceGauge("QueryGPUDeviceTempAvg", "DCGM_FI_DEV_GPU_TEMP", "avg", start, end)
}

// QueryGPUDeviceUsageAvg queries average device-level compute utilization
// as a 0-1 ratio (DCGM_FI_PROF_GR_ENGINE_ACTIVE without container
// attribution).
func (pds *PrometheusMetricsQuerier) QueryGPUDeviceUsageAvg(start, end time.Time) *source.Future[source.GPUDeviceMetricResult] {
	return pds.queryGPUDeviceGauge("QueryGPUDeviceUsageAvg", "DCGM_FI_PROF_GR_ENGINE_ACTIVE", "avg", start, end)
}

// QueryGPUDeviceUsageMax queries peak device-level compute utilization as a
// 0-1 ratio.
func (pds *PrometheusMetricsQuerier) QueryGPUDeviceUsageMax(start, end time.Time) *source.Future[source.GPUDeviceMetricResult] {
	return pds.queryGPUDeviceGauge("QueryGPUDeviceUsageMax", "DCGM_FI_PROF_GR_ENGINE_ACTIVE", "max", start, end)
}

// QueryGPUDeviceMemoryUsedAvg queries average framebuffer used in MiB
// (DCGM_FI_DEV_FB_USED); hydration converts to bytes.
func (pds *PrometheusMetricsQuerier) QueryGPUDeviceMemoryUsedAvg(start, end time.Time) *source.Future[source.GPUDeviceMetricResult] {
	return pds.queryGPUDeviceGauge("QueryGPUDeviceMemoryUsedAvg", "DCGM_FI_DEV_FB_USED", "avg", start, end)
}

// QueryGPUDeviceMemoryUsedMax queries peak framebuffer used in MiB.
func (pds *PrometheusMetricsQuerier) QueryGPUDeviceMemoryUsedMax(start, end time.Time) *source.Future[source.GPUDeviceMetricResult] {
	return pds.queryGPUDeviceGauge("QueryGPUDeviceMemoryUsedMax", "DCGM_FI_DEV_FB_USED", "max", start, end)
}
