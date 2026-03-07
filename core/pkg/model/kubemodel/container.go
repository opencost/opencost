package kubemodel

import (
	"fmt"
	"time"
)

// @bingen:generate:Container
type Container struct {
	PodUID       string  `json:"podUid"`
	Name         string  `json:"name"`
	CPUAllocated float64 `json:"cpuAllocated"`
	CPUUsageAvg  float64 `json:"cpuUsageAvg"`
	CPUUsageMax  float64 `json:"cpuUsageMax"`
	CPURequest   float64 `json:"cpuRequest"`
	CPULimit     float64 `json:"cpuLimit"`
	RAMAllocated float64 `json:"ramAllocated"`
	RAMUsageAvg  float64 `json:"ramUsageAvg"`
	RAMUsageMax  float64 `json:"ramUsageMax"`
	RAMRequest   float64 `json:"ramRequest"`
	RAMLimit     float64 `json:"ramLimit"`
	GPUAllocated float64 `json:"gpuAllocated"`
	GPUUsageAvg  float64 `json:"gpuUsageAvg"`
	GPUUsageMax  float64 `json:"gpuUsageMax"`
	GPURequest   float64 `json:"gpuRequest"`
	//VolumeStorageByteSeconds  map[string]float64 `json:"volumeStorageByteSeconds,omitempty"`
	//VolumeStorageByteUsageMax map[string]float64 `json:"volumeStorageByteUsageMax,omitempty"`
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
