package kubemodel

import (
	"time"
)

// Node represents a Kubernetes node
type Node struct {
	ID                   string             `json:"id"`                            // @bingen:field[version=1]
	ClusterID            string             `json:"clusterId"`                     // @bingen:field[version=1]
	ProviderResourceID   string             `json:"providerResourceId"`            // @bingen:field[version=1]
	Name                 string             `json:"name"`                          // @bingen:field[version=1]
	Labels               map[string]string  `json:"labels,omitempty"`              // @bingen:field[version=1]
	Annotations          map[string]string  `json:"annotations,omitempty"`         // @bingen:field[version=1]
	Start                time.Time          `json:"start"`                         // @bingen:field[version=1]
	End                  time.Time          `json:"end"`                           // @bingen:field[version=1]
	CpuCores             uint64             `json:"cpuCores"`                      // @bingen:field[version=1]
	RamBytes             uint64             `json:"ramBytes"`                      // @bingen:field[version=1]
	CpuCost              float64            `json:"cpuCost"`                       // @bingen:field[version=1]
	RamCost              float64            `json:"ramCost"`                       // @bingen:field[version=1]
	GpuCost              float64            `json:"gpuCost"`                       // @bingen:field[version=1]
	CpuCoreUsageAverage  float64            `json:"cpuCoreUsageAverage"`           // @bingen:field[version=1]
	CpuCoreUsageMax      float64            `json:"cpuCoreUsageMax"`               // @bingen:field[version=1]
	RamBytesUsageAverage uint64             `json:"ramBytesUsageAverage"`          // @bingen:field[version=1]
	RamBytesUsageMax     uint64             `json:"ramBytesUsageMax"`              // @bingen:field[version=1]
	Pods                 map[string]*Pod    `json:"pods"`                          // @bingen:field[version=1]
	EphemeralVolumes     map[string]*Volume `json:"ephemeralVolumes,omitempty"`    // @bingen:field[version=1]
	Diagnostic           *DiagnosticResult  `json:"diagnostic,omitempty"`          // @bingen:field[version=1]
}