package kubemodel

import "time"

// @bingen:generate:Container
type Container struct {
	PodUID                              string    `json:"podUid"`                              // @bingen:field[version=1]
	Name                                string    `json:"name"`                                // @bingen:field[version=1]
	Start                               time.Time `json:"start"`                               // @bingen:field[version=1]
	End                                 time.Time `json:"end"`                                 // @bingen:field[version=1]
	CpuMillicoreSecondsAllocated        uint64    `json:"cpuMillicoreSecondsAllocated"`        // @bingen:field[version=1]
	CpuMillicoreRequestAverageAllocated uint64    `json:"cpuMillicoreRequestAverageAllocated"` // @bingen:field[version=1]
	CpuMillicoreUsageAverage            uint64    `json:"cpuMillicoreUsageAverage"`            // @bingen:field[version=1]
	CpuMillicoreUsageMax                uint64    `json:"cpuMillicoreUsageMax"`                // @bingen:field[version=1]
	RAMByteSecondsAllocated             uint64    `json:"ramByteSecondsAllocated"`             // @bingen:field[version=1]
	RAMByteRequestAverageAllocated      uint64    `json:"ramByteRequestAverageAllocated"`      // @bingen:field[version=1]
	RAMByteUsageAverage                 uint64    `json:"ramByteUsageAverage"`                 // @bingen:field[version=1]
	RAMByteUsageMax                     uint64    `json:"ramByteUsageMax"`                     // @bingen:field[version=1]
	StorageByteSecondsAllocated         uint64    `json:"storageByteSecondsAllocated"`         // @bingen:field[version=1]
	StorageByteRequestAverageAllocated  uint64    `json:"storageByteRequestAverageAllocated"`  // @bingen:field[version=1]
	StorageByteUsageAverage             uint64    `json:"storageByteUsageAverage"`             // @bingen:field[version=1]
	StorageByteUsageMax                 uint64    `json:"storageByteUsageMax"`                 // @bingen:field[version=1]
}
