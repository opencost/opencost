package kubemodel

import (
	"time"
)

// Node represents a Kubernetes node
type Node struct {
	ID                   string            `json:"id"`
	ClusterID            string            `json:"clusterId"`
	ProviderResourceID   string            `json:"providerResourceId"`
	Name                 string            `json:"name"`
	Labels               map[string]string `json:"labels,omitempty"`
	Annotations          map[string]string `json:"annotations,omitempty"`
	CreationTime         *time.Time        `json:"creationTime,omitempty"`
	DeletionTime         *time.Time        `json:"deletionTime,omitempty"`
	CpuCores             uint64            `json:"cpuCores"`
	RamBytes             uint64            `json:"ramBytes"`
	CpuCost              float64           `json:"cpuCost"`
	RamCost              float64           `json:"ramCost"`
	GpuCost              float64           `json:"gpuCost"`
	CpuCoreUsageAverage  float64           `json:"cpuCoreUsageAverage"`
	CpuCoreUsageMax      float64           `json:"cpuCoreUsageMax"`
	RamBytesUsageAverage uint64            `json:"ramBytesUsageAverage"`
	RamBytesUsageMax     uint64            `json:"ramBytesUsageMax"`
	Diagnostic           *DiagnosticResult `json:"diagnostic,omitempty"`
}