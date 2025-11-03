package kubemodel

import (
	"time"
)

// Container represents a container within a pod (allocated resource)
type Container struct {
	PodID                      string            `json:"podId"`
	Name                       string            `json:"name"`
	CreationTime               *time.Time        `json:"creationTime,omitempty"`
	DeletionTime               *time.Time        `json:"deletionTime,omitempty"`
	CpuCoreHours               float64           `json:"cpuCoreHours"`
	CpuCoreRequestAverage      float64           `json:"cpuCoreRequestAverage"`
	CpuCoreUsageAverage        float64           `json:"cpuCoreUsageAverage"`
	CpuCoreUsageMax            float64           `json:"cpuCoreUsageMax"`
	RamByteHours               uint64            `json:"ramByteHours"`
	RamBytesRequestAverage     uint64            `json:"ramBytesRequestAverage"`
	RamBytesUsageAverage       uint64            `json:"ramBytesUsageAverage"`
	RamBytesUsageMax           uint64            `json:"ramBytesUsageMax"`
	StorageByteHours           uint64            `json:"storageByteHours"`
	StorageBytesRequestAverage uint64            `json:"storageBytesRequestAverage"`
	StorageBytesUsageAverage   uint64            `json:"storageBytesUsageAverage"`
	StorageBytesUsageMax       uint64            `json:"storageBytesUsageMax"`
	GpuUsages                  []GPUUsage        `json:"gpuUsages,omitempty"`
	Diagnostic                 *DiagnosticResult `json:"diagnostic,omitempty"`
}