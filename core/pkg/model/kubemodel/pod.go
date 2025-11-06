package kubemodel

import (
	"time"
)

// Pod represents a Kubernetes pod
type Pod struct {
	ID                         string            `json:"id"`
	NamespaceID                string            `json:"namespaceId"`
	ControllerID               string            `json:"controllerId"`
	NodeID                     string            `json:"nodeId"`
	Name                       string            `json:"name"`
	Labels                     map[string]string `json:"labels,omitempty"`
	Annotations                map[string]string `json:"annotations,omitempty"`
	CreationTime               *time.Time        `json:"creationTime,omitempty"`
	DeletionTime               *time.Time        `json:"deletionTime,omitempty"`
	CpuMillicoreSeconds        uint64            `json:"cpuMillicoreSeconds"`
	CpuMillicoreRequestAverage uint64            `json:"cpuMillicoreRequestAverage"`
	CpuMillicoreUsageAverage   uint64            `json:"cpuMillicoreUsageAverage"`
	CpuMillicoreUsageMax       uint64            `json:"cpuMillicoreUsageMax"`
	RamByteSeconds             uint64            `json:"ramByteSeconds"`
	RamBytesRequestAverage     uint64            `json:"ramBytesRequestAverage"`
	RamBytesUsageAverage       uint64            `json:"ramBytesUsageAverage"`
	RamBytesUsageMax           uint64            `json:"ramBytesUsageMax"`
	StorageByteSeconds         uint64            `json:"storageByteSeconds"`
	NetworkTransferBytes       uint64            `json:"networkTransferBytes"`
	NetworkReceiveBytes        uint64            `json:"networkReceiveBytes"`
	Diagnostic                 *DiagnosticResult `json:"diagnostic,omitempty"`
}
