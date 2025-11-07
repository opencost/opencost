package kubemodel

import (
	"errors"
	"fmt"
)

// DeviceUsage represents device usage metrics for a container
type DeviceUsage struct {
	// Device identification
	DeviceID   string     `json:"deviceId"`   // References Device.ID
	DeviceType DeviceType `json:"deviceType"` // Type of device (gpu, tpu, etc.)

	// Time-based accumulation (consistent units across all resources)
	DeviceSeconds uint64 `json:"deviceSeconds"` // Device allocation time in seconds

	// Usage metrics (device-agnostic, percentages 0-100)
	RequestPercent float64 `json:"requestPercent"` // Percentage of device requested
	UsageAverage   float64 `json:"usageAverage"`   // Average utilization (0-100%)
	UsageMax       float64 `json:"usageMax"`       // Peak utilization (0-100%)

	// Memory metrics (if applicable)
	MemoryBytes uint64 `json:"memoryBytes,omitempty"` // Device memory used

	// Device-specific metrics (extensible)
	Metrics map[string]any `json:"metrics,omitempty"`

	// Diagnostics
	Diagnostic *DiagnosticResult `json:"diagnostic,omitempty"`
}

// Validate validates the DeviceUsage fields
func (u *DeviceUsage) Validate() error {
	if u.DeviceID == "" {
		return errors.New("DeviceID is required")
	}
	if u.DeviceType == "" {
		return errors.New("DeviceType is required")
	}
	if !u.DeviceType.IsValid() {
		return fmt.Errorf("invalid DeviceType: %s", u.DeviceType)
	}
	if u.RequestPercent < 0 || u.RequestPercent > 100 {
		return fmt.Errorf("RequestPercent must be 0-100, got %.2f", u.RequestPercent)
	}
	if u.UsageAverage < 0 || u.UsageAverage > 100 {
		return fmt.Errorf("UsageAverage must be 0-100, got %.2f", u.UsageAverage)
	}
	if u.UsageMax < 0 || u.UsageMax > 100 {
		return fmt.Errorf("UsageMax must be 0-100, got %.2f", u.UsageMax)
	}
	if u.UsageMax < u.UsageAverage {
		return errors.New("UsageMax cannot be less than UsageAverage")
	}
	return nil
}

// Clone creates a deep copy of the DeviceUsage
func (u *DeviceUsage) Clone() *DeviceUsage {
	if u == nil {
		return nil
	}

	cloned := &DeviceUsage{
		DeviceID:       u.DeviceID,
		DeviceType:     u.DeviceType,
		DeviceSeconds:  u.DeviceSeconds,
		RequestPercent: u.RequestPercent,
		UsageAverage:   u.UsageAverage,
		UsageMax:       u.UsageMax,
		MemoryBytes:    u.MemoryBytes,
		Diagnostic:     u.Diagnostic,
	}

	// Deep copy Metrics map
	if u.Metrics != nil {
		cloned.Metrics = make(map[string]any, len(u.Metrics))
		for k, v := range u.Metrics {
			cloned.Metrics[k] = v
		}
	}

	return cloned
}