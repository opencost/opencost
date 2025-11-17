package kubemodel

import (
	"errors"
	"fmt"
)

// @bingen:generate:GPUDevice
// GPUDevice represents a GPU device with DCGM integration (provisioned resource)
// This tracks available GPU capacity on a node
type GPUDevice struct {
	UID                string            `json:"uid"`                      // @bingen:field[version=1] GPU UUID (hardware identifier)
	NodeUID            string            `json:"nodeUid"`                  // @bingen:field[version=1] Node hosting this GPU device
	DeviceNumber       int32             `json:"deviceNumber"`             // @bingen:field[version=1]
	ModelName          string            `json:"modelName"`                // @bingen:field[version=1]
	IsShared           bool              `json:"isShared"`                 // @bingen:field[version=1] GPU sharing information
	SharePercentage    float64           `json:"sharePercentage"`          // @bingen:field[version=1]
	GpuHours           float64           `json:"gpuHours"`                 // @bingen:field[version=1] GPU hours available
	GpuRequestAverage  float64           `json:"gpuRequestAverage"`        // @bingen:field[version=1] GPU request average percentage (0-100)
	GpuUsageAverage    float64           `json:"gpuUsageAverage"`          // @bingen:field[version=1] GPU usage average percentage (0-100)
	GpuUsageMax        float64           `json:"gpuUsageMax"`              // @bingen:field[version=1] GPU usage max percentage (0-100)
	MemoryBytes        int64             `json:"memoryBytes"`              // @bingen:field[version=1] GPU memory capacity in bytes
	Diagnostic         *DiagnosticResult `json:"diagnostic,omitempty"`     // @bingen:field[version=1]
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
	if d.GpuRequestAverage < 0 || d.GpuRequestAverage > 100 {
		return fmt.Errorf("GpuRequestAverage must be 0-100, got %.2f", d.GpuRequestAverage)
	}
	if d.GpuUsageAverage < 0 || d.GpuUsageAverage > 100 {
		return fmt.Errorf("GpuUsageAverage must be 0-100, got %.2f", d.GpuUsageAverage)
	}
	if d.GpuUsageMax < 0 || d.GpuUsageMax > 100 {
		return fmt.Errorf("GpuUsageMax must be 0-100, got %.2f", d.GpuUsageMax)
	}
	if d.GpuUsageMax < d.GpuUsageAverage {
		return errors.New("GpuUsageMax cannot be less than GpuUsageAverage")
	}
	if d.GpuHours < 0 {
		return fmt.Errorf("GpuHours cannot be negative, got %.2f", d.GpuHours)
	}
	return nil
}

// Clone creates a deep copy of the GPUDevice
func (d *GPUDevice) Clone() *GPUDevice {
	if d == nil {
		return nil
	}

	cloned := &GPUDevice{
		UID:               d.UID,
		NodeUID:           d.NodeUID,
		DeviceNumber:      d.DeviceNumber,
		ModelName:         d.ModelName,
		IsShared:          d.IsShared,
		SharePercentage:   d.SharePercentage,
		GpuHours:          d.GpuHours,
		GpuRequestAverage: d.GpuRequestAverage,
		GpuUsageAverage:   d.GpuUsageAverage,
		GpuUsageMax:       d.GpuUsageMax,
		MemoryBytes:       d.MemoryBytes,
		Diagnostic:        d.Diagnostic,
	}

	return cloned
}