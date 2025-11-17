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
	ModelName                   string            `json:"modelName"`                   // @bingen:field[version=1]
	IsShared                    bool              `json:"isShared"`                    // @bingen:field[version=1] GPU sharing information
	SharePercentage             float64           `json:"sharePercentage"`             // @bingen:field[version=1]
	GpuSeconds                  float64           `json:"gpuSeconds"`                  // @bingen:field[version=1] GPU seconds available
	GpuRequestPercentageAverage float64           `json:"gpuRequestPercentageAverage"` // @bingen:field[version=1] GPU request average percentage (0-100)
	GpuUsagePercentageAverage   float64           `json:"gpuUsagePercentageAverage"`   // @bingen:field[version=1] GPU usage average percentage (0-100)
	GpuUsagePercentageMax       float64           `json:"gpuUsagePercentageMax"`       // @bingen:field[version=1] GPU usage max percentage (0-100)
	MemoryBytes                 uint64            `json:"memoryBytes"`                 // @bingen:field[version=1] GPU memory capacity in bytes
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
	if d.GpuRequestPercentageAverage < 0 || d.GpuRequestPercentageAverage > 100 {
		return fmt.Errorf("GpuRequestPercentageAverage must be 0-100, got %.2f", d.GpuRequestPercentageAverage)
	}
	if d.GpuUsagePercentageAverage < 0 || d.GpuUsagePercentageAverage > 100 {
		return fmt.Errorf("GpuUsagePercentageAverage must be 0-100, got %.2f", d.GpuUsagePercentageAverage)
	}
	if d.GpuUsagePercentageMax < 0 || d.GpuUsagePercentageMax > 100 {
		return fmt.Errorf("GpuUsagePercentageMax must be 0-100, got %.2f", d.GpuUsagePercentageMax)
	}
	if d.GpuUsagePercentageMax < d.GpuUsagePercentageAverage {
		return errors.New("GpuUsagePercentageMax cannot be less than GpuUsagePercentageAverage")
	}
	if d.GpuSeconds < 0 {
		return fmt.Errorf("GpuSeconds cannot be negative, got %.2f", d.GpuSeconds)
	}
	return nil
}

// Clone creates a deep copy of the GPUDevice
func (d *GPUDevice) Clone() *GPUDevice {
	if d == nil {
		return nil
	}

	cloned := &GPUDevice{
		UID:                         d.UID,
		NodeUID:                     d.NodeUID,
		DeviceNumber:                d.DeviceNumber,
		ModelName:                   d.ModelName,
		IsShared:                    d.IsShared,
		SharePercentage:             d.SharePercentage,
		GpuSeconds:                  d.GpuSeconds,
		GpuRequestPercentageAverage: d.GpuRequestPercentageAverage,
		GpuUsagePercentageAverage:   d.GpuUsagePercentageAverage,
		GpuUsagePercentageMax:       d.GpuUsagePercentageMax,
		MemoryBytes:                 d.MemoryBytes,
		Diagnostic:                  d.Diagnostic,
	}

	return cloned
}