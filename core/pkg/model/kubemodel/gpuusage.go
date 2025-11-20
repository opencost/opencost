package kubemodel

import (
	"errors"
	"fmt"
)

// @bingen:generate:GPUUsage
// GPUUsage represents GPU resources consumed by a container (allocated resource)
// This tracks actual GPU usage by containers for cost analysis
// GPU has three key dimensions: compute, memory, and power consumption
//
// TODO: Power metrics are currently not collected by OpenCost's DCGM scraper.
// To fully populate power fields, the following changes are needed:
// 1. Add DCGM_FI_DEV_POWER_USAGE to modules/collector-source/pkg/metric/metrics.go
// 2. Update DCGM scraper in modules/collector-source/pkg/scrape/dcgm.go to collect power metrics
// 3. Add power metric queries to modules/prometheus-source/pkg/prom/metricsquerier.go
// 4. Implement power metric hydration in the collector/source data pipeline
type GPUUsage struct {
	ContainerUID                string  `json:"containerUid"`                // @bingen:field[version=1] Container consuming GPU resources
	GpuDeviceUID                string  `json:"gpuDeviceUid"`                // @bingen:field[version=1] Reference to the GPU device being used
	GpuSeconds                  float64 `json:"gpuSeconds"`                  // @bingen:field[version=1] GPU compute usage in device-seconds consumed
	GpuRequestPercentageAverage float64 `json:"gpuRequestPercentageAverage"` // @bingen:field[version=1] GPU compute request in percentage (0-100)
	GpuUsagePercentageAverage   float64 `json:"gpuUsagePercentageAverage"`   // @bingen:field[version=1] GPU compute usage average percentage (0-100)
	GpuUsagePercentageMax       float64 `json:"gpuUsagePercentageMax"`       // @bingen:field[version=1] GPU compute usage max percentage (0-100)
	MemoryByteSecondsUsed       uint64  `json:"memoryByteSecondsUsed"`       // @bingen:field[version=1] GPU memory usage in byte-seconds
	PowerWattSeconds            float64 `json:"powerWattSeconds"`            // @bingen:field[version=1] GPU power consumption in watt-seconds (Joules)
	PowerWattAverage            float64 `json:"powerWattAverage"`            // @bingen:field[version=1] GPU average power consumption in watts
	PowerWattMax                float64 `json:"powerWattMax"`                // @bingen:field[version=1] GPU max power consumption in watts
}

// Validate validates the GPUUsage fields
func (u *GPUUsage) Validate() error {
	if u.ContainerUID == "" {
		return errors.New("ContainerUID is required")
	}
	if u.GpuDeviceUID == "" {
		return errors.New("GpuDeviceUID is required")
	}
	if u.GpuRequestPercentageAverage < 0 || u.GpuRequestPercentageAverage > 100 {
		return fmt.Errorf("GpuRequestPercentageAverage must be 0-100, got %.2f", u.GpuRequestPercentageAverage)
	}
	if u.GpuUsagePercentageAverage < 0 || u.GpuUsagePercentageAverage > 100 {
		return fmt.Errorf("GpuUsagePercentageAverage must be 0-100, got %.2f", u.GpuUsagePercentageAverage)
	}
	if u.GpuUsagePercentageMax < 0 || u.GpuUsagePercentageMax > 100 {
		return fmt.Errorf("GpuUsagePercentageMax must be 0-100, got %.2f", u.GpuUsagePercentageMax)
	}
	if u.GpuUsagePercentageMax < u.GpuUsagePercentageAverage {
		return errors.New("GpuUsagePercentageMax cannot be less than GpuUsagePercentageAverage")
	}
	if u.GpuSeconds < 0 {
		return fmt.Errorf("GpuSeconds cannot be negative, got %.2f", u.GpuSeconds)
	}
	if u.PowerWattSeconds < 0 {
		return fmt.Errorf("PowerWattSeconds cannot be negative, got %.2f", u.PowerWattSeconds)
	}
	if u.PowerWattAverage < 0 {
		return fmt.Errorf("PowerWattAverage cannot be negative, got %.2f", u.PowerWattAverage)
	}
	if u.PowerWattMax < 0 {
		return fmt.Errorf("PowerWattMax cannot be negative, got %.2f", u.PowerWattMax)
	}
	if u.PowerWattMax < u.PowerWattAverage {
		return errors.New("PowerWattMax cannot be less than PowerWattAverage")
	}
	return nil
}

// Clone creates a deep copy of the GPUUsage
func (u *GPUUsage) Clone() *GPUUsage {
	if u == nil {
		return nil
	}

	cloned := &GPUUsage{
		ContainerUID:                u.ContainerUID,
		GpuDeviceUID:                u.GpuDeviceUID,
		GpuSeconds:                  u.GpuSeconds,
		GpuRequestPercentageAverage: u.GpuRequestPercentageAverage,
		GpuUsagePercentageAverage:   u.GpuUsagePercentageAverage,
		GpuUsagePercentageMax:       u.GpuUsagePercentageMax,
		MemoryByteSecondsUsed:       u.MemoryByteSecondsUsed,
		PowerWattSeconds:            u.PowerWattSeconds,
		PowerWattAverage:            u.PowerWattAverage,
		PowerWattMax:                u.PowerWattMax,
	}

	return cloned
}
