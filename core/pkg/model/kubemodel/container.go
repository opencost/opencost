package kubemodel

import (
	"fmt"
	"time"
)

// @bingen:generate:Container
type Container struct {
	PodUID                string    `json:"podUid"`
	Name                  string    `json:"name"`
	CPUMilliCoreAllocated float64   `json:"cpuMilliCoreAllocated"`
	CPUMilliCoreUsageAvg  float64   `json:"cpuMilliCoreUsageAvg"`
	CPUMilliCoreUsageMax  float64   `json:"cpuMilliCoreUsageMax"`
	CPUMilliCoreRequest   float64   `json:"cpuMilliCoreRequest"`
	CPUMilliCoreLimit     float64   `json:"cpuMilliCoreLimit"`
	RAMBytesAllocated     float64   `json:"ramBytesAllocated"`
	RAMBytesUsageAvg      float64   `json:"ramBytesUsageAvg"`
	RAMBytesUsageMax      float64   `json:"ramBytesUsageMax"`
	RAMBytesRequest       float64   `json:"ramBytesRequest"`
	RAMBytesLimit         float64   `json:"ramBytesLimit"`
	GPUAllocated          float64   `json:"gpuAllocated"`
	GPUUsageAvg           float64   `json:"gpuUsageAvg"`
	GPUUsageMax           float64   `json:"gpuUsageMax"`
	GPURequest            float64   `json:"gpuRequest"`
	Start                 time.Time `json:"start"`
	End                   time.Time `json:"end"`
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
