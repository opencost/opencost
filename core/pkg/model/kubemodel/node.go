package kubemodel

import "time"

// @bingen:generate:Node
type Node struct {
	UID                      string            `json:"uid"`                      // @bingen:field[version=1]
	ClusterUID               string            `json:"clusterUid"`               // @bingen:field[version=1]
	ProviderResourceUID      string            `json:"providerResourceUid"`      // @bingen:field[version=1]
	Name                     string            `json:"name"`                     // @bingen:field[version=1]
	Labels                   map[string]string `json:"labels,omitempty"`         // @bingen:field[version=1]
	Annotations              map[string]string `json:"annotations,omitempty"`    // @bingen:field[version=1]
	Start                    time.Time         `json:"start"`                    // @bingen:field[version=1]
	End                      time.Time         `json:"end"`                      // @bingen:field[version=1]
	CpuMillicoreSeconds      uint64            `json:"cpuMillicoreSeconds"`      // @bingen:field[version=1]
	RAMByteSeconds           uint64            `json:"ramByteSeconds"`           // @bingen:field[version=1]
	CpuMillicoreUsageAverage uint64            `json:"cpuMillicoreUsageAverage"` // @bingen:field[version=1]
	CpuMillicoreUsageMax     uint64            `json:"cpuMillicoreUsageMax"`     // @bingen:field[version=1]
	RAMByteUsageAverage      uint64            `json:"ramByteUsageAverage"`      // @bingen:field[version=1]
	RAMByteUsageMax          uint64            `json:"ramByteUsageMax"`          // @bingen:field[version=1]
	Diagnostic               *DiagnosticResult `json:"diagnostic,omitempty"`     // @bingen:field[version=1]
}
