package kubemodel

import (
	"errors"
	"fmt"
	"time"
)

// @bingen:generate:Device
type Device struct {
	UID               string      `json:"uid"`            // Device UUID (hardware identifier)
	Type              string      `json:"type,omitempty"` // Device type (e.g., "device", "tpu")
	NodeUID           string      `json:"nodeUid"`        // Node hosting this device
	DeviceNumber      int32       `json:"deviceNumber"`
	ModelName         string      `json:"modelName"`
	IsShared          bool        `json:"isShared"` // Device sharing information
	SharePercentage   float64     `json:"sharePercentage"`
	UsageSeconds      float64     `json:"usageSeconds"`      // Device seconds available
	MemoryByteSeconds Measurement `json:"memoryByteSeconds"` // Device memory capacity in Byte-seconds
	PowerWattSeconds  float64     `json:"powerWattSeconds"`  // Device power consumption in watt-seconds (Joules)
	PowerWattMax      float64     `json:"powerWattMax"`      // Device max power consumption in watts
	// Version 2 fields - Lifecycle tracking
	Start           time.Time   `json:"start,omitempty"` // Device availability start
	End             time.Time   `json:"end,omitempty"`   // Device availability end
	DurationSeconds Measurement `json:"durationSeconds"` // Duration device was available
}

// Validate validates the Device fields
func (d *Device) Validate() error {
	if d.UID == "" {
		return errors.New("UID is required")
	}
	if d.NodeUID == "" {
		return errors.New("NodeUID is required")
	}
	if d.SharePercentage < 0 || d.SharePercentage > 100 {
		return fmt.Errorf("SharePercentage must be 0-100, got %.2f", d.SharePercentage)
	}
	if d.PowerWattSeconds < 0 {
		return fmt.Errorf("PowerWattSeconds cannot be negative, got %.2f", d.PowerWattSeconds)
	}
	if d.PowerWattMax < 0 {
		return fmt.Errorf("PowerWattMax cannot be negative, got %.2f", d.PowerWattMax)
	}
	return nil
}

// Clone creates a deep copy of the Device
func (d *Device) Clone() *Device {
	if d == nil {
		return nil
	}

	cloned := &Device{
		UID:               d.UID,
		Type:              d.Type,
		NodeUID:           d.NodeUID,
		DeviceNumber:      d.DeviceNumber,
		ModelName:         d.ModelName,
		IsShared:          d.IsShared,
		SharePercentage:   d.SharePercentage,
		UsageSeconds:      d.UsageSeconds,
		MemoryByteSeconds: d.MemoryByteSeconds,
		PowerWattSeconds:  d.PowerWattSeconds,
		PowerWattMax:      d.PowerWattMax,
		DurationSeconds:   d.DurationSeconds,
	}

	cloned.Start = d.Start
	cloned.End = d.End

	return cloned
}

func (kms *KubeModelSet) RegisterDevice(uid, nodeUID string) error {
	if uid == "" {
		err := fmt.Errorf("UID is nil for Device")
		kms.Error(err)
		return err
	}

	if _, ok := kms.Devices[uid]; !ok {
		kms.Devices[uid] = &Device{
			UID:     uid,
			NodeUID: nodeUID,
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}
