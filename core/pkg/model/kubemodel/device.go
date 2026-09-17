package kubemodel

import (
	"fmt"
	"time"
)

// Device holds identification for an accelerator device (e.g. an Nvidia GPU) attached to the
// cluster via the k8s Device Plugin API or DRAs. The shape is vendor-agnostic, but the only
// populating source currently implemented is the DCGM exporter. Usage of a Device by a specific
// container is recorded on Container.DeviceUsages, keyed by Device.UUID.
// @bingen:generate:Device
type Device struct {
	UUID      string    `json:"uuid"`
	Start     time.Time `json:"start"`
	End       time.Time `json:"end"`
	Device    string    `json:"device"`
	ModelName string    `json:"modelName"`
}

func (d *Device) ValidateDevice(window Window) error {
	if d.UUID == "" {
		return fmt.Errorf("UUID is missing for Device with device '%s'", d.Device)
	}

	if err := checkWindow(window, d.Start, d.End); err != nil {
		return err
	}

	return nil
}

// RegisterDevice validates and adds a Device to the set, keyed by UUID.
func (kms *KubeModelSet) RegisterDevice(device *Device) error {
	if err := device.ValidateDevice(kms.Window); err != nil {
		err = fmt.Errorf("RegisterDevice: invalid device: %w", err)
		kms.Error(err)
		return err
	}

	if _, ok := kms.Devices[device.UUID]; !ok {
		if kms.Cluster == nil {
			kms.Warnf("RegisterDevice: Cluster is nil")
		}

		kms.Devices[device.UUID] = device

		kms.Metadata.ObjectCount++
	}

	return nil
}
