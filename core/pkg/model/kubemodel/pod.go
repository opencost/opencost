package kubemodel

import "time"

// @bingen:generate:Pod
type Pod struct {
	ID                         string                            `json:"id"`                               // @bingen:field[version=1]
	NamespaceID                string                            `json:"namespaceId"`                      // @bingen:field[version=1]
	ControllerID               string                            `json:"controllerId"`                     // @bingen:field[version=1]
	NodeID                     string                            `json:"nodeId"`                           // @bingen:field[version=1]
	Name                       string                            `json:"name"`                             // @bingen:field[version=1]
	Labels                     map[string]string                 `json:"labels,omitempty"`                 // @bingen:field[version=1]
	Annotations                map[string]string                 `json:"annotations,omitempty"`            // @bingen:field[version=1]
	Start                      time.Time                         `json:"start"`                            // @bingen:field[version=1]
	End                        time.Time                         `json:"end"`                              // @bingen:field[version=1]
	CpuMillicoreSeconds        uint64                            `json:"cpuMillicoreSeconds"`              // @bingen:field[version=1]
	CpuMillicoreRequestAverage uint64                            `json:"cpuMillicoreRequestAverage"`       // @bingen:field[version=1]
	CpuMillicoreUsageAverage   uint64                            `json:"cpuMillicoreUsageAverage"`         // @bingen:field[version=1]
	CpuMillicoreUsageMax       uint64                            `json:"cpuMillicoreUsageMax"`             // @bingen:field[version=1]
	RamByteSeconds             uint64                            `json:"ramByteSeconds"`                   // @bingen:field[version=1]
	RamBytesRequestAverage     uint64                            `json:"ramBytesRequestAverage"`           // @bingen:field[version=1]
	RamBytesUsageAverage       uint64                            `json:"ramBytesUsageAverage"`             // @bingen:field[version=1]
	RamBytesUsageMax           uint64                            `json:"ramBytesUsageMax"`                 // @bingen:field[version=1]
	StorageByteSeconds         uint64                            `json:"storageByteSeconds"`               // @bingen:field[version=1]
	NetworkTransferBytes       uint64                            `json:"networkTransferBytes"`             // @bingen:field[version=1]
	NetworkReceiveBytes        uint64                            `json:"networkReceiveBytes"`              // @bingen:field[version=1]
	Containers                 map[string]*Container             `json:"containers"`                       // @bingen:field[version=1]
	AttachedGPUDevices         map[string]*GPUDevice             `json:"attachedGpuDevices,omitempty"`     // @bingen:field[version=1]
	PersistentVolumeClaims     map[string]*PersistentVolumeClaim `json:"persistentVolumeClaims,omitempty"` // @bingen:field[version=1]
	Diagnostic                 *DiagnosticResult                 `json:"diagnostic,omitempty"`             // @bingen:field[version=1]
}
