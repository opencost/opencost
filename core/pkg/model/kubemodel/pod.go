package kubemodel

import (
	"time"
)

// Pod represents a Kubernetes pod
type Pod struct {
	ID                     string            `json:"id"`
	NamespaceID            string            `json:"namespaceId"`
	ControllerID           string            `json:"controllerId"`
	NodeID                 string            `json:"nodeId"`
	Name                   string            `json:"name"`
	Labels                 map[string]string `json:"labels,omitempty"`
	Annotations            map[string]string `json:"annotations,omitempty"`
	CreationTime           *time.Time        `json:"creationTime,omitempty"`
	DeletionTime           *time.Time        `json:"deletionTime,omitempty"`
	CpuCoreHours           float64           `json:"cpuCoreHours"`
	CpuCoreRequestAverage  float64           `json:"cpuCoreRequestAverage"`
	CpuCoreUsageAverage    float64           `json:"cpuCoreUsageAverage"`
	CpuCoreUsageMax        float64           `json:"cpuCoreUsageMax"`
	RamByteHours           uint64            `json:"ramByteHours"`
	RamBytesRequestAverage uint64            `json:"ramBytesRequestAverage"`
	RamBytesUsageAverage   uint64            `json:"ramBytesUsageAverage"`
	RamBytesUsageMax       uint64            `json:"ramBytesUsageMax"`
	StorageByteHours       uint64            `json:"storageByteHours"`
	NetworkTransferBytes   uint64            `json:"networkTransferBytes"`
	NetworkReceiveBytes    uint64            `json:"networkReceiveBytes"`
	Diagnostic             *DiagnosticResult `json:"diagnostic,omitempty"`
}