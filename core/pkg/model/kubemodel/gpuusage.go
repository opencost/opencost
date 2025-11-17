package kubemodel

import (
	"errors"
	"fmt"
)

// @bingen:generate:GPUUsage
// GPUUsage represents GPU resources consumed by a container (allocated resource)
// This tracks actual GPU usage by containers for cost analysis
type GPUUsage struct {
	ContainerUID         string            `json:"containerUid"`         // @bingen:field[version=1] Container consuming GPU resources
	GpuDeviceUID         string            `json:"gpuDeviceUid"`         // @bingen:field[version=1] Reference to the GPU device being used
	GpuHours             float64           `json:"gpuHours"`             // @bingen:field[version=1] GPU usage in device-hours consumed
	GpuRequestPercentage float64           `json:"gpuRequestPercentage"` // @bingen:field[version=1] GPU request in percentage (0-100)
	GpuUsageAverage      float64           `json:"gpuUsageAverage"`      // @bingen:field[version=1] GPU usage average percentage (0-100)
	GpuUsageMax          float64           `json:"gpuUsageMax"`          // @bingen:field[version=1] GPU usage max percentage (0-100)
	MemoryBytesUsed      int64             `json:"memoryBytesUsed"`      // @bingen:field[version=1] GPU memory usage in bytes
	Diagnostic           *DiagnosticResult `json:"diagnostic,omitempty"` // @bingen:field[version=1]
}

// Validate validates the GPUUsage fields
func (u *GPUUsage) Validate() error {
	if u.ContainerUID == "" {
		return errors.New("ContainerUID is required")
	}
	if u.GpuDeviceUID == "" {
		return errors.New("GpuDeviceUID is required")
	}
	if u.GpuRequestPercentage < 0 || u.GpuRequestPercentage > 100 {
		return fmt.Errorf("GpuRequestPercentage must be 0-100, got %.2f", u.GpuRequestPercentage)
	}
	if u.GpuUsageAverage < 0 || u.GpuUsageAverage > 100 {
		return fmt.Errorf("GpuUsageAverage must be 0-100, got %.2f", u.GpuUsageAverage)
	}
	if u.GpuUsageMax < 0 || u.GpuUsageMax > 100 {
		return fmt.Errorf("GpuUsageMax must be 0-100, got %.2f", u.GpuUsageMax)
	}
	if u.GpuUsageMax < u.GpuUsageAverage {
		return errors.New("GpuUsageMax cannot be less than GpuUsageAverage")
	}
	if u.GpuHours < 0 {
		return fmt.Errorf("GpuHours cannot be negative, got %.2f", u.GpuHours)
	}
	return nil
}

// Clone creates a deep copy of the GPUUsage
func (u *GPUUsage) Clone() *GPUUsage {
	if u == nil {
		return nil
	}

	cloned := &GPUUsage{
		ContainerUID:         u.ContainerUID,
		GpuDeviceUID:         u.GpuDeviceUID,
		GpuHours:             u.GpuHours,
		GpuRequestPercentage: u.GpuRequestPercentage,
		GpuUsageAverage:      u.GpuUsageAverage,
		GpuUsageMax:          u.GpuUsageMax,
		MemoryBytesUsed:      u.MemoryBytesUsed,
		Diagnostic:           u.Diagnostic,
	}

	return cloned
}
