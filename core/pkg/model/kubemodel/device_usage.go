package kubemodel

import (
	"errors"
	"fmt"
)

// @bingen:generate:DeviceUsage
type DeviceUsage struct {
	DeviceID       string            `json:"deviceId"`              // @bingen:field[version=1]
	DeviceType     DeviceType        `json:"deviceType"`            // @bingen:field[version=1]
	DeviceSeconds  uint64            `json:"deviceSeconds"`         // @bingen:field[version=1]
	RequestPercent float64           `json:"requestPercent"`        // @bingen:field[version=1]
	UsageAverage   float64           `json:"usageAverage"`          // @bingen:field[version=1]
	UsageMax       float64           `json:"usageMax"`              // @bingen:field[version=1]
	MemoryBytes    uint64            `json:"memoryBytes,omitempty"` // @bingen:field[version=1]
	Metrics        map[string]string `json:"metrics,omitempty"`     // @bingen:field[version=1]
	Diagnostic     *DiagnosticResult `json:"diagnostic,omitempty"`  // @bingen:field[version=1]
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
		cloned.Metrics = make(map[string]string, len(u.Metrics))
		for k, v := range u.Metrics {
			cloned.Metrics[k] = v
		}
	}

	return cloned
}