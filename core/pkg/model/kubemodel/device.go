package kubemodel

import (
	"errors"
	"fmt"
)

// Device represents a physical or logical attached device on a node
type Device struct {
	// Device identification
	ID           string     `json:"id"`           // @bingen:field[version=1]
	NodeID       string     `json:"nodeId"`       // @bingen:field[version=1]
	DeviceType   DeviceType `json:"deviceType"`   // @bingen:field[version=1]
	DeviceNumber uint64     `json:"deviceNumber"` // @bingen:field[version=1]

	// Hardware identification
	ModelName string `json:"modelName"`      // @bingen:field[version=1]
	Vendor    string `json:"vendor"`         // @bingen:field[version=1]
	UUID      string `json:"uuid,omitempty"` // @bingen:field[version=1]

	// Sharing configuration
	IsShared     bool    `json:"isShared"`     // @bingen:field[version=1]
	SharePercent float64 `json:"sharePercent"` // @bingen:field[version=1]

	// Time-based metrics
	DeviceSeconds uint64 `json:"deviceSeconds"` // @bingen:field[version=1]

	// Usage metrics (device-agnostic)
	RequestAverage float64 `json:"requestAverage"` // @bingen:field[version=1]
	UsageAverage   float64 `json:"usageAverage"`   // @bingen:field[version=1]
	UsageMax       float64 `json:"usageMax"`       // @bingen:field[version=1]

	// Memory
	MemoryBytes uint64 `json:"memoryBytes,omitempty"` // @bingen:field[version=1]

	// Device-specific attributes (extensible)
	Attributes map[string]any `json:"attributes,omitempty"` // @bingen:field[version=1]

	// Diagnostics
	Diagnostic *DiagnosticResult `json:"diagnostic,omitempty"` // @bingen:field[version=1]
}

// Validate validates the Device fields
func (d *Device) Validate() error {
	if d.ID == "" {
		return errors.New("ID is required")
	}
	if d.NodeID == "" {
		return errors.New("NodeID is required")
	}
	if d.DeviceType == "" {
		return errors.New("DeviceType is required")
	}
	if !d.DeviceType.IsValid() {
		return fmt.Errorf("invalid DeviceType: %s", d.DeviceType)
	}
	if d.SharePercent < 0 || d.SharePercent > 100 {
		return fmt.Errorf("SharePercent must be 0-100, got %.2f", d.SharePercent)
	}
	if d.RequestAverage < 0 || d.RequestAverage > 100 {
		return fmt.Errorf("RequestAverage must be 0-100, got %.2f", d.RequestAverage)
	}
	if d.UsageAverage < 0 || d.UsageAverage > 100 {
		return fmt.Errorf("UsageAverage must be 0-100, got %.2f", d.UsageAverage)
	}
	if d.UsageMax < 0 || d.UsageMax > 100 {
		return fmt.Errorf("UsageMax must be 0-100, got %.2f", d.UsageMax)
	}
	if d.UsageMax < d.UsageAverage {
		return errors.New("UsageMax cannot be less than UsageAverage")
	}
	return nil
}

// Clone creates a deep copy of the Device
func (d *Device) Clone() *Device {
	if d == nil {
		return nil
	}

	cloned := &Device{
		ID:             d.ID,
		NodeID:         d.NodeID,
		DeviceType:     d.DeviceType,
		DeviceNumber:   d.DeviceNumber,
		ModelName:      d.ModelName,
		Vendor:         d.Vendor,
		UUID:           d.UUID,
		IsShared:       d.IsShared,
		SharePercent:   d.SharePercent,
		DeviceSeconds:  d.DeviceSeconds,
		RequestAverage: d.RequestAverage,
		UsageAverage:   d.UsageAverage,
		UsageMax:       d.UsageMax,
		MemoryBytes:    d.MemoryBytes,
		Diagnostic:     d.Diagnostic,
	}

	// Deep copy Attributes map
	if d.Attributes != nil {
		cloned.Attributes = make(map[string]any, len(d.Attributes))
		for k, v := range d.Attributes {
			cloned.Attributes[k] = v
		}
	}

	return cloned
}