package kubemodel

import "time"

// TODO complete
// TODO (start, end) here, or defer to Pod?
// TODO how to handle network bytes?
// TODO how to handle GPUs?
// TODO how to handle storage?
// TODO how to handle devices?
// TODO uint64 or float64 for numbers?
type Container struct {
	UID                           string
	PodUID                        string
	Name                          string
	Start                         time.Time
	End                           time.Time
	CPUAllocationMillicoreSeconds uint64
	CPURequestMillicoreAverage    uint64
	CPULimitMillicoreAverage      uint64
	CPUUsageMillicoreAverage      uint64
	CPUUsageMillicoreMax          uint64
	RAMAllocationByteSeconds      uint64
	RAMRequestByteAverage         uint64
	RAMLimitByteAverage           uint64
	RAMUsageByteAverage           uint64
	RAMUsageByteMax               uint64
	PVCMounts                     map[string]*MountedVolume
}

type MountedVolume struct {
	PersistentVolumeClaimUID string
	Name                     string
	StorageCapacityBytes     uint64
}
