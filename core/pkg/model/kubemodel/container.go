package kubemodel

import "time"

// @bingen:generate:Container
type Container struct {
	PodUID                     string    `json:"podUid"`                     // @bingen:field[version=1]
	Name                       string    `json:"name"`                       // @bingen:field[version=1]
	Start                      time.Time `json:"start"`                      // @bingen:field[version=1]
	End                        time.Time `json:"end"`                        // @bingen:field[version=1]
	CpuMillicoreSeconds        uint64    `json:"cpuMillicoreSeconds"`        // @bingen:field[version=1]
	CpuMillicoreRequestAverage uint64    `json:"cpuMillicoreRequestAverage"` // @bingen:field[version=1]
	CpuMillicoreUsageAverage   uint64    `json:"cpuMillicoreUsageAverage"`   // @bingen:field[version=1]
	CpuMillicoreUsageMax       uint64    `json:"cpuMillicoreUsageMax"`       // @bingen:field[version=1]
	RAMByteSeconds             uint64    `json:"ramByteSeconds"`             // @bingen:field[version=1]
	RAMByteRequestAverage      uint64    `json:"ramByteRequestAverage"`      // @bingen:field[version=1]
	RAMByteUsageAverage        uint64    `json:"ramByteUsageAverage"`        // @bingen:field[version=1]
	RAMByteUsageMax            uint64    `json:"ramByteUsageMax"`            // @bingen:field[version=1]
	StorageByteSeconds         uint64    `json:"storageByteSeconds"`         // @bingen:field[version=1]
	StorageByteRequestAverage  uint64    `json:"storageByteRequestAverage"`  // @bingen:field[version=1]
	StorageByteUsageAverage    uint64    `json:"storageByteUsageAverage"`    // @bingen:field[version=1]
	StorageByteUsageMax        uint64    `json:"storageByteUsageMax"`        // @bingen:field[version=1]
}
