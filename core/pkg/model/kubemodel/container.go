package kubemodel

import (
	"fmt"
	"time"
)

type Container struct {
	PodUID                              string    `json:"podUid"`
	Name                                string    `json:"name"`
	Start                               time.Time `json:"start"`
	End                                 time.Time `json:"end"`
	CpuMillicoreSecondsAllocated        uint64    `json:"cpuMillicoreSecondsAllocated"`
	CpuMillicoreRequestAverageAllocated uint64    `json:"cpuMillicoreRequestAverageAllocated"`
	CpuMillicoreUsageAverage            uint64    `json:"cpuMillicoreUsageAverage"`
	CpuMillicoreUsageMax                uint64    `json:"cpuMillicoreUsageMax"`
	RAMByteSecondsAllocated             uint64    `json:"ramByteSecondsAllocated"`
	RAMByteRequestAverageAllocated      uint64    `json:"ramByteRequestAverageAllocated"`
	RAMByteUsageAverage                 uint64    `json:"ramByteUsageAverage"`
	RAMByteUsageMax                     uint64    `json:"ramByteUsageMax"`
	StorageByteSecondsAllocated         uint64    `json:"storageByteSecondsAllocated"`
	StorageByteRequestAverageAllocated  uint64    `json:"storageByteRequestAverageAllocated"`
	StorageByteUsageAverage             uint64    `json:"storageByteUsageAverage"`
	StorageByteUsageMax                 uint64    `json:"storageByteUsageMax"`
}

func (kms *KubeModelSet) RegisterContainer(uid, name, podUID string) error {
	if uid == "" {
		err := fmt.Errorf("UID is nil for Container '%s'", name)
		kms.Error(err)
		return err
	}

	if _, ok := kms.Containers[uid]; !ok {
		kms.Containers[uid] = &Container{
			PodUID: podUID,
			Name:   name,
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}
