package kubemodel

import (
	"fmt"
	"time"
)

// DCGMDevice holds recording from the DCGM exporter which provides identification and usage metrics for
// Nvidia gpu. These Nvidia devices can be incorporated into the cluster via k8s Device Plugin API or DRAs.
// While the DCGM exporter does provide unique identifiers for the containers that it is reporting metrics on,
// It is split out here to provide some isolate from the rest of the KubeModel which represent universal structures
// from the k8s API. It is left to the end user to interpret the relationships to the rest of the cluster based on
// container unique identifiers
// @bingen:generate:DCGMDevice
type DCGMDevice struct {
	UUID      string
	Start     time.Time
	End       time.Time
	Device    string
	ModelName string
	PodUsage  map[string]DCGMPod
}

// @bingen:generate:DCGMPod
type DCGMPod struct {
	ContainerUsage map[string]DCGMContainer
}

// @bingen:generate:DCGMContainer
type DCGMContainer struct {
	UsageAvg float64
	UsageMax float64
}

// RegisterDCGMDevice validates and adds a DCGMDevice to the set, keyed by UUID.
func (kms *KubeModelSet) RegisterDCGMDevice(device *DCGMDevice) error {
	if device.UUID == "" {
		err := fmt.Errorf("UUID is missing for DCGMDevice with device '%s'", device.Device)
		kms.Error(err)
		return err
	}

	if err := checkWindow(kms.Window, device.Start, device.End); err != nil {
		kms.Error(err)
		return err
	}

	if _, ok := kms.DCGMDevices[device.UUID]; !ok {
		if kms.Cluster == nil {
			kms.Warnf("RegisterDCGMDevice: Cluster is nil")
		}

		kms.DCGMDevices[device.UUID] = device

		kms.Metadata.ObjectCount++
	}

	return nil
}
