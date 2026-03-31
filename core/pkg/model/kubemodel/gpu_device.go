package kubemodel

import "time"

// GPUVendor identifies the GPU hardware vendor.
// Using a typed string for forward compatibility with catalog ingestion.
type GPUVendor string

const (
	GPUVendorNVIDIA  GPUVendor = "nvidia"
	GPUVendorAMD     GPUVendor = "amd"
	GPUVendorIntel   GPUVendor = "intel"
	GPUVendorUnknown GPUVendor = "unknown"
)

// @bingen:generate:GPUDevice
type GPUDevice struct {
	// UID is the unique identifier for this GPU device (e.g. from node labels
	// "nvidia.com/gpu.uuid" or equivalent).
	UID               string    `json:"uid"`
	// NodeUID links this device to its parent Node in the KubeModelSet.
	NodeUID           string    `json:"nodeUid"`
	// Name is the human-readable device name, e.g. "NVIDIA A100 80GB".
	Name              string    `json:"name"`
	Vendor            GPUVendor `json:"vendor"`
	Index             int       `json:"index"`
	Start             time.Time `json:"start"`
	End               time.Time `json:"end"`
	MemoryByte        uint64    `json:"memoryByte"`
	MemoryUsedByte    uint64    `json:"memoryUsedByte"`
	MemoryUsedMaxByte uint64    `json:"memoryUsedMaxByte"`
	CoreUsageSeconds  float64   `json:"coreUsageSeconds"`
	PowerWattSeconds  float64   `json:"powerWattSeconds"`
	PowerLimitWatt    float64   `json:"powerLimitWatt"`
}

// @bingen:generate:GPUUsage
type GPUUsage struct {
	ContainerUID        string    `json:"containerUid"`
	DeviceUID           string    `json:"deviceUid"`
	AllocatedFraction   float64   `json:"allocatedFraction"`
	AllocatedMemoryByte uint64    `json:"allocatedMemoryByte"`
	CoreUsageSeconds    float64   `json:"coreUsageSeconds"`
	MemoryUsedByte      uint64    `json:"memoryUsedByte"`
	Start               time.Time `json:"start"`
	End                 time.Time `json:"end"`
}

// RegisterGPUDevice adds or replaces a GPUDevice in the KubeModelSet, indexed by UID.
func (kms *KubeModelSet) RegisterGPUDevice(device *GPUDevice) {
	if device == nil || device.UID == "" {
		return
	}
	if kms.GPUDevices == nil {
		kms.GPUDevices = make(map[string]*GPUDevice)
	}
	kms.GPUDevices[device.UID] = device
	kms.Metadata.ObjectCount++
}

// RegisterGPUUsage adds or replaces a GPUUsage record, keyed by ContainerUID+DeviceUID.
func (kms *KubeModelSet) RegisterGPUUsage(usage *GPUUsage) {
	if usage == nil || usage.ContainerUID == "" || usage.DeviceUID == "" {
		return
	}
	if kms.GPUUsages == nil {
		kms.GPUUsages = make(map[string]*GPUUsage)
	}
	key := usage.ContainerUID + "/" + usage.DeviceUID
	kms.GPUUsages[key] = usage
	kms.Metadata.ObjectCount++
}
