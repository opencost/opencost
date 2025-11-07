package kubemodel

import (
	"time"
)

// Container represents a container within a pod (allocated resource)
type Container struct {
	PodID                      string                  `json:"podId"`
	Name                       string                  `json:"name"`
	Start                      *time.Time              `json:"start,omitempty"`
	End                        *time.Time              `json:"end,omitempty"`
	CpuMillicoreSeconds        uint64                  `json:"cpuMillicoreSeconds"`
	CpuMillicoreRequestAverage uint64                  `json:"cpuMillicoreRequestAverage"`
	CpuMillicoreUsageAverage   uint64                  `json:"cpuMillicoreUsageAverage"`
	CpuMillicoreUsageMax       uint64                  `json:"cpuMillicoreUsageMax"`
	RamByteSeconds             uint64                  `json:"ramByteSeconds"`
	RamBytesRequestAverage     uint64                  `json:"ramBytesRequestAverage"`
	RamBytesUsageAverage       uint64                  `json:"ramBytesUsageAverage"`
	RamBytesUsageMax           uint64                  `json:"ramBytesUsageMax"`
	StorageByteSeconds         uint64                  `json:"storageByteSeconds"`
	StorageBytesRequestAverage uint64                  `json:"storageBytesRequestAverage"`
	StorageBytesUsageAverage   uint64                  `json:"storageBytesUsageAverage"`
	StorageBytesUsageMax       uint64                  `json:"storageBytesUsageMax"`
	Devices                    map[string]*DeviceUsage `json:"devices,omitempty"`
	Diagnostic                 *DiagnosticResult       `json:"diagnostic,omitempty"`
}
