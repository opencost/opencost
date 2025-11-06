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
	Start                time.Time         `json:"start"`
	End                  time.Time         `json:"end"`
	CpuCores             uint64            `json:"cpuCores"`
	RamBytes             uint64            `json:"ramBytes"`
	CpuCost              float64           `json:"cpuCost"`
	RamCost              float64           `json:"ramCost"`
	GpuCost              float64           `json:"gpuCost"`
	CpuCoreUsageAverage  float64           `json:"cpuCoreUsageAverage"`
	CpuCoreUsageMax      float64           `json:"cpuCoreUsageMax"`
	RamBytesUsageAverage uint64            `json:"ramBytesUsageAverage"`
	RamBytesUsageMax     uint64            `json:"ramBytesUsageMax"`
	Pods                 map[string]*Pod   `json:"pods"`
	EphemeralVolumes     map[string]*Volume `json:"ephemeralVolumes,omitempty"`
	Diagnostic           *DiagnosticResult `json:"diagnostic,omitempty"`
}