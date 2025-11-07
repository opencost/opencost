package kubemodel

import "time"

// @bingen:generate:Container
type Container struct {
	PodID                      string                  `json:"podId"`                      // @bingen:field[version=1]
	Name                       string                  `json:"name"`                       // @bingen:field[version=1]
	Start                      time.Time               `json:"start"`                      // @bingen:field[version=1]
	End                        time.Time               `json:"end"`                        // @bingen:field[version=1]
	CpuMillicoreSeconds        uint64                  `json:"cpuMillicoreSeconds"`        // @bingen:field[version=1]
	CpuMillicoreRequestAverage uint64                  `json:"cpuMillicoreRequestAverage"` // @bingen:field[version=1]
	CpuMillicoreUsageAverage   uint64                  `json:"cpuMillicoreUsageAverage"`   // @bingen:field[version=1]
	CpuMillicoreUsageMax       uint64                  `json:"cpuMillicoreUsageMax"`       // @bingen:field[version=1]
	RamByteSeconds             uint64                  `json:"ramByteSeconds"`             // @bingen:field[version=1]
	RamBytesRequestAverage     uint64                  `json:"ramBytesRequestAverage"`     // @bingen:field[version=1]
	RamBytesUsageAverage       uint64                  `json:"ramBytesUsageAverage"`       // @bingen:field[version=1]
	RamBytesUsageMax           uint64                  `json:"ramBytesUsageMax"`           // @bingen:field[version=1]
	StorageByteSeconds         uint64                  `json:"storageByteSeconds"`         // @bingen:field[version=1]
	StorageBytesRequestAverage uint64                  `json:"storageBytesRequestAverage"` // @bingen:field[version=1]
	StorageBytesUsageAverage   uint64                  `json:"storageBytesUsageAverage"`   // @bingen:field[version=1]
	StorageBytesUsageMax       uint64                  `json:"storageBytesUsageMax"`       // @bingen:field[version=1]
	Devices                    map[string]*DeviceUsage `json:"devices,omitempty"`          // @bingen:field[version=1]
	Diagnostic                 *DiagnosticResult       `json:"diagnostic,omitempty"`       // @bingen:field[version=1]
}
