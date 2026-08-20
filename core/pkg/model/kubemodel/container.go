package kubemodel

import (
	"fmt"
	"time"
)

// @bingen:generate:Container
type Container struct {
	PodUID                string                 `json:"podUid"`
	Name                  string                 `json:"name"`
	ResourceRequests      ResourceQuantities     `json:"resourceRequests"`
	ResourceLimits        ResourceQuantities     `json:"resourceLimits"`
	CPUCoreAllocationAvg  float64                `json:"cpuCoreAllocationAvg"`
	CPUCoreUsageAvg       float64                `json:"cpuCoreUsageAvg"`
	CPUCoreUsageMax       float64                `json:"cpuCoreUsageMax"`
	RAMBytesAllocationAvg float64                `json:"ramBytesAllocationAvg"`
	RAMBytesUsageAvg      float64                `json:"ramBytesUsageAvg"`
	RAMBytesUsageMax      float64                `json:"ramBytesUsageMax"`
	DeviceUsages          map[string]DeviceUsage `json:"deviceUsages"` // @bingen:field[version=3]
	Start                 time.Time              `json:"start"`
	End                   time.Time              `json:"end"`
}

// DeviceUsage holds usage metrics for a single container/device pairing. The shape is
// vendor-agnostic, but the only populating source currently implemented is the DCGM exporter.
// It is keyed by Device.UUID under Container.DeviceUsages.
// @bingen:generate:DeviceUsage
type DeviceUsage struct {
	UsageAvg float64 `json:"usageAvg"`
	UsageMax float64 `json:"usageMax"`
}

func (c *Container) ValidateContainer(window Window) error {
	if c.PodUID == "" {
		return fmt.Errorf("PodUID is missing for Container with name '%s'", c.Name)
	}

	if c.Name == "" {
		return fmt.Errorf("Name is missing for Container on pod '%s'", c.PodUID)
	}

	if err := checkWindow(window, c.Start, c.End); err != nil {
		return err
	}

	return nil
}

func (kms *KubeModelSet) RegisterContainer(container *Container) error {
	if err := container.ValidateContainer(kms.Window); err != nil {
		err = fmt.Errorf("RegisterContainer: invalid container: %w", err)
		kms.Error(err)
		return err
	}

	key := container.GetKey()
	if _, ok := kms.Containers[key]; !ok {
		kms.Containers[key] = container
		kms.Metadata.ObjectCount++
	}

	return nil
}

func (c *Container) GetKey() string {
	return ContainerKey(c.PodUID, c.Name)
}

func ContainerKey(podUID, conatinerName string) string {
	return fmt.Sprintf("%s/%s", podUID, conatinerName)
}
