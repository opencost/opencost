package kubemodel

import (
	"fmt"
	"time"
)

// @bingen:generate:Cluster
type Container struct {
	PodUID       string      `json:"podUid"`
	Name         string      `json:"name"`
	CPUAllocated Measurement `json:"cpuAllocated"`
	CPUUsageAvg  Measurement `json:"cpuUsageAvg"`
	CPUUsageMax  Measurement `json:"cpuUsageMax"`
	CPURequest   Measurement `json:"cpuRequest"`
	CPULimit     Measurement `json:"cpuLimit"`
	RAMAllocated Measurement `json:"ramAllocated"`
	RAMUsageAvg  Measurement `json:"ramUsageAvg"`
	RAMUsageMax  Measurement `json:"ramUsageMax"`
	RAMRequest   Measurement `json:"ramRequest"`
	RAMLimit     Measurement `json:"ramLimit"`
	GPUAllocated Measurement `json:"gpuAllocated"`
	GPUUsageAvg  Measurement `json:"gpuUsageAvg"`
	GPUUsageMax  Measurement `json:"gpuUsageMax"`
	GPURequest   Measurement `json:"gpuRequest"`
	//VolumeStorageByteSeconds  map[string]Measurement `json:"volumeStorageByteSeconds,omitempty"`
	//VolumeStorageByteUsageMax map[string]Measurement `json:"volumeStorageByteUsageMax,omitempty"`
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
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
