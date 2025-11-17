package kubemodel

import (
	"errors"
	"fmt"
)

// @bingen:generate:GPUUsage
// GPUUsage represents GPU resources consumed by a container (allocated resource)
// This tracks actual GPU usage by containers for cost analysis
type GPUUsage struct {
	ContainerUID                string            `json:"containerUid"`                // @bingen:field[version=1] Container consuming GPU resources
	GpuDeviceUID                string            `json:"gpuDeviceUid"`                // @bingen:field[version=1] Reference to the GPU device being used
	GpuSeconds                  float64           `json:"gpuSeconds"`                  // @bingen:field[version=1] GPU usage in device-seconds consumed
	GpuRequestPercentageAverage float64           `json:"gpuRequestPercentageAverage"` // @bingen:field[version=1] GPU request in percentage (0-100)
	GpuUsagePercentageAverage   float64           `json:"gpuUsagePercentageAverage"`   // @bingen:field[version=1] GPU usage average percentage (0-100)
	GpuUsagePercentageMax       float64           `json:"gpuUsagePercentageMax"`       // @bingen:field[version=1] GPU usage max percentage (0-100)
	MemoryBytesUsed             uint64            `json:"memoryBytesUsed"`             // @bingen:field[version=1] GPU memory usage in bytes
	Diagnostic                  *DiagnosticResult `json:"diagnostic,omitempty"`        // @bingen:field[version=1]
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
		MemoryBytesUsed:             u.MemoryBytesUsed,
		Diagnostic:                  u.Diagnostic,
	}

	return cloned
}
