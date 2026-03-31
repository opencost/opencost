package kubemodel

import (
	"fmt"
	"time"
)

// @bingen:generate:Container
type Container struct {
	PodUID           string             `json:"podUid"`
	Name             string             `json:"name"`
	ResourceRequests ResourceQuantities `json:"resourceRequests"`
	ResourceLimits   ResourceQuantities `json:"ResourceLimits"`
	CPUCoreUsageAvg  float64            `json:"cpuCoreUsageAvg"`
	CPUCoreUsageMax  float64            `json:"cpuCoreUsageMax"`
	RAMBytesUsageAvg float64            `json:"ramBytesUsageAvg"`
	RAMBytesUsageMax float64            `json:"ramBytesUsageMax"`
	Start            time.Time          `json:"start"`
	End              time.Time          `json:"end"`
}

func (c *Container) GetKey() string {
	return fmt.Sprintf("%s/%s", c.PodUID, c.Name)
}

func (kms *KubeModelSet) RegisterContainer(container *Container) error {
	// Check required fields
	if container.PodUID == "" {
		err := fmt.Errorf("PodUID is missing for Container with name '%s'", container.Name)
		kms.Error(err)
		return err
	}

	if container.Name == "" {
		err := fmt.Errorf("Name is missing for Container on pod '%s'", container.PodUID)
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
