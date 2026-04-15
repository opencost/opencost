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
	// Vendor is the GPU hardware manufacturer (nvidia, amd, intel, unknown).
	Vendor            GPUVendor `json:"vendor"`
	// Index is the zero-based device ordinal on the node.
	Index             int       `json:"index"`
	// Start and End define the observation window for this device's metrics.
	Start             time.Time `json:"start"`
	End               time.Time `json:"end"`
	// MemoryByte is the total on-device memory capacity in bytes.
	MemoryByte        uint64    `json:"memoryByte"`
	// MemoryUsedByte is the time-average used memory in bytes over the window.
	MemoryUsedByte    uint64    `json:"memoryUsedByte"`
	// MemoryUsedMaxByte is the peak used memory in bytes observed during the window.
	MemoryUsedMaxByte uint64    `json:"memoryUsedMaxByte"`
	// CoreUsageSeconds is the cumulative GPU core utilisation integral
	// (utilisation_ratio × duration_seconds) over the observation window.
	CoreUsageSeconds  float64   `json:"coreUsageSeconds"`
	// PowerWattSeconds is the cumulative energy consumed in joules (W × s)
	// over the observation window.
	PowerWattSeconds  float64   `json:"powerWattSeconds"`
	// PowerLimitWatt is the configured TDP power limit for this device in watts.
	PowerLimitWatt    float64   `json:"powerLimitWatt"`
}

// @bingen:generate:GPUUsage
type GPUUsage struct {
	// ContainerUID links this usage record to its owning Container.
	ContainerUID        string    `json:"containerUid"`
	// DeviceUID links this usage record to the physical GPUDevice.
	DeviceUID           string    `json:"deviceUid"`
	// AllocatedFraction is the fraction of the GPU allocated to this container,
	// in [0, 1]; 1.0 = whole-GPU, 0.5 = MIG half-slice or time-slice share.
	AllocatedFraction   float64   `json:"allocatedFraction"`
	// AllocatedMemoryByte is the GPU memory (in bytes) reserved/requested by the
	// container's device limit—distinct from MemoryUsedByte which is actual runtime consumption.
	AllocatedMemoryByte uint64    `json:"allocatedMemoryByte"`
	// CoreUsageSeconds is the cumulative GPU core utilisation integral
	// (utilisation_ratio × duration_seconds) attributed to this container.
	CoreUsageSeconds    float64   `json:"coreUsageSeconds"`
	// MemoryUsedByte is the time-average GPU memory actively used by this
	// container in bytes (runtime consumption, not reservation).
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
	if _, exists := kms.GPUDevices[device.UID]; !exists {
		kms.Metadata.ObjectCount++
	}
	kms.GPUDevices[device.UID] = device
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
	if _, exists := kms.GPUUsages[key]; !exists {
		kms.Metadata.ObjectCount++
	}
	kms.GPUUsages[key] = usage
}
