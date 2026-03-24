package kubemodel

import (
	"fmt"
	"time"
)

// @bingen:generate:Device
type Device struct {
	UID          string
	NodeUID      string
	Model        string
	ResourceName string
	Start        time.Time
	End          time.Time
}

// @bingen:generate:ContainerDeviceUsage
type ContainerDeviceUsage struct {
	DeviceUID string
	UsageAvg  float64
	UsageMax  float64
}

// RegisterDevice validates and adds a device to the set
func (kms *KubeModelSet) RegisterDevice(device *Device) error {
	if device.UID == "" {
		err := fmt.Errorf("UID is missing for Device on node '%s'", device.NodeUID)
		kms.Error(err)
		return err
	}

	if device.NodeUID == "" {
		err := fmt.Errorf("NodeUID is missing for Device '%s'", device.UID)
		kms.Error(err)
		return err
	}

	if _, ok := kms.Devices[device.UID]; !ok {
		kms.Devices[device.UID] = device
		kms.Metadata.ObjectCount++
	}

	return nil
}
