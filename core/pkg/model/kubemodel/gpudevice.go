package kubemodel

import (
	"errors"
	"fmt"
)

// @bingen:generate:GPUDevice
// GPUDevice represents a physical GPU device with DCGM integration (provisioned resource)
// This tracks available GPU capacity on a node and device-level metrics like power consumption
//
// TODO: Power metrics are available in Prometheus from dcgm-exporter (DCGM_FI_DEV_POWER_USAGE, DCGM_FI_DEV_TOTAL_ENERGY_CONSUMPTION)
// but not yet integrated into OpenCost's data pipeline. To fully populate power fields:
// 1. Add DCGM_FI_DEV_POWER_USAGE constant to modules/collector-source/pkg/metric/metrics.go
// 2. Update DCGM scraper metric list in modules/collector-source/pkg/scrape/dcgm.go to include power metrics
// 3. Add power metric queries to modules/prometheus-source/pkg/prom/metricsquerier.go (e.g., QueryGPUPowerUsageAvg, QueryGPUPowerUsageMax)
// 4. Implement power metric hydration in pkg/costmodel/kubemodel.go to populate PowerWattSeconds/PowerWattAverage/PowerWattMax fields
type GPUDevice struct {
	UID                        string  `json:"uid"`                        // @bingen:field[version=1] GPU UUID (hardware identifier)
	NodeUID                    string  `json:"nodeUid"`                    // @bingen:field[version=1] Node hosting this GPU device
	DeviceNumber               int32   `json:"deviceNumber"`               // @bingen:field[version=1]
	ModelName                  string  `json:"modelName"`                  // @bingen:field[version=1]
	IsShared                   bool    `json:"isShared"`                   // @bingen:field[version=1] GPU sharing information
	SharePercentage            float64 `json:"sharePercentage"`            // @bingen:field[version=1]
	GpuSecondsAllocated        float64 `json:"gpuSecondsAllocated"`        // @bingen:field[version=1] GPU seconds available
	MemoryByteSecondsAllocated uint64  `json:"memoryByteSecondsAllocated"` // @bingen:field[version=1] GPU memory capacity in byte-seconds
	PowerWattSeconds           float64 `json:"powerWattSeconds"`           // @bingen:field[version=1] GPU device power consumption in watt-seconds (Joules)
	PowerWattAverage           float64 `json:"powerWattAverage"`           // @bingen:field[version=1] GPU device average power consumption in watts
	PowerWattMax               float64 `json:"powerWattMax"`               // @bingen:field[version=1] GPU device max power consumption in watts
}

// Validate validates the GPUDevice fields
func (d *GPUDevice) Validate() error {
	if d.UID == "" {
		return errors.New("UID is required")
	}
	if d.NodeUID == "" {
		return errors.New("NodeUID is required")
	}
	if d.SharePercentage < 0 || d.SharePercentage > 100 {
		return fmt.Errorf("SharePercentage must be 0-100, got %.2f", d.SharePercentage)
	}
	if d.GpuSecondsAllocated < 0 {
		return fmt.Errorf("GpuSecondsAllocated cannot be negative, got %.2f", d.GpuSecondsAllocated)
	}
	if d.PowerWattSeconds < 0 {
		return fmt.Errorf("PowerWattSeconds cannot be negative, got %.2f", d.PowerWattSeconds)
	}
	if d.PowerWattAverage < 0 {
		return fmt.Errorf("PowerWattAverage cannot be negative, got %.2f", d.PowerWattAverage)
	}
	if d.PowerWattMax < 0 {
		return fmt.Errorf("PowerWattMax cannot be negative, got %.2f", d.PowerWattMax)
	}
	if d.PowerWattMax < d.PowerWattAverage {
		return errors.New("PowerWattMax cannot be less than PowerWattAverage")
	}
	return nil
}

// Clone creates a deep copy of the GPUDevice
func (d *GPUDevice) Clone() *GPUDevice {
	if d == nil {
		return nil
	}

	cloned := &GPUDevice{
		UID:              d.UID,
		NodeUID:          d.NodeUID,
		DeviceNumber:     d.DeviceNumber,
		ModelName:        d.ModelName,
		IsShared:         d.IsShared,
		SharePercentage:  d.SharePercentage,
		GpuSecondsAllocated:       d.GpuSecondsAllocated,
		MemoryByteSecondsAllocated: d.MemoryByteSecondsAllocated,
		PowerWattSeconds: d.PowerWattSeconds,
		PowerWattAverage: d.PowerWattAverage,
		PowerWattMax:     d.PowerWattMax,
	}

	return cloned
}
