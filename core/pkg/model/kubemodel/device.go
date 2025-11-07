package kubemodel

import (
	"errors"
	"fmt"
)

// Device represents a physical or logical attached device on a node
type Device struct {
	// Device identification
	ID           string     `json:"id"`           // Unique device identifier
	NodeID       string     `json:"nodeId"`       // Parent node
	DeviceType   DeviceType `json:"deviceType"`   // Type of device
	DeviceNumber uint64     `json:"deviceNumber"` // Device index on node (0, 1, 2...)

	// Hardware identification
	ModelName string `json:"modelName"`      // e.g., "NVIDIA A100-SXM4-40GB"
	Vendor    string `json:"vendor"`         // e.g., "NVIDIA", "Google", "Intel"
	UUID      string `json:"uuid,omitempty"` // Hardware UUID if available

	// Sharing configuration
	IsShared     bool    `json:"isShared"`     // Can be shared by multiple containers
	SharePercent float64 `json:"sharePercent"` // Allocation percentage (0-100)

	// Time-based metrics
	DeviceSeconds uint64 `json:"deviceSeconds"` // Total allocated seconds

	// Usage metrics (device-agnostic)
	RequestAverage float64 `json:"requestAverage"` // Average request utilization %
	UsageAverage   float64 `json:"usageAverage"`   // Average actual utilization %
	UsageMax       float64 `json:"usageMax"`       // Peak utilization %

	// Memory
	MemoryBytes uint64 `json:"memoryBytes,omitempty"` // Total device memory in bytes

	// Device-specific attributes (extensible)
	Attributes map[string]any `json:"attributes,omitempty"`

	// Diagnostics
	Diagnostic *DiagnosticResult `json:"diagnostic,omitempty"`
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