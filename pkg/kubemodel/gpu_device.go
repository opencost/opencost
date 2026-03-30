// Package kubemodel defines the OpenCost KubeModel data structures.
//
// This file implements GPU resource tracking, which was explicitly scoped OUT
// of the initial PR #3472 ("Introduce kubemodel with core Kubernetes resources")
// and is targeted for a follow-up PR per the contributor's own description:
//
//   "Intentionally excluded (to be added in separate PRs):
//    GPU resources (GPUDevice, GPUUsage)"
//
// Contribution angle: implement GPUDevice and GPUUsage structs consistent
// with the flat map-based architecture and bingen annotations established
// by PR #3472. This is the highest-priority follow-up identified from reading
// the merged PR.
//
// References:
//   - Merged PR: https://github.com/opencost/opencost/pull/3472
//   - OpenCost 2026 roadmap: AI usage cost tracking

package kubemodel

import "time"

// GPUVendor identifies the GPU hardware vendor.
// Using a typed string (not iota) for forward compatibility with catalog ingestion.
type GPUVendor string

const (
	GPUVendorNVIDIA  GPUVendor = "nvidia"
	GPUVendorAMD     GPUVendor = "amd"
	GPUVendorIntel   GPUVendor = "intel"
	GPUVendorUnknown GPUVendor = "unknown"
)

// GPUDevice represents a physical GPU attached to a Kubernetes Node.
//
// Metrics follow the same unit conventions established in PR #3472:
//   - time-based: cumulative seconds (float64)
//   - memory: bytes (uint64)
//   - utilisation: ratio 0.0-1.0 (float64)
//
// @bingen:generate
// @bingen:version=1
type GPUDevice struct {
	// UID is the unique identifier for this GPU device (e.g. from node labels
	// "nvidia.com/gpu.uuid" or equivalent).
	UID string `json:"uid"`

	// NodeUID links this device to its parent Node in the KubeModelSet.
	NodeUID string `json:"nodeUID"`

	// Name is the human-readable device name, e.g. "NVIDIA A100 80GB".
	Name string `json:"name"`

	// Vendor identifies the GPU manufacturer.
	Vendor GPUVendor `json:"vendor"`

	// Index is the device index on the node (0-based), matching the
	// CUDA_VISIBLE_DEVICES / ROCm ordinal.
	Index int `json:"index"`

	// Start is when this device first became visible to the kubelet.
	Start time.Time `json:"start"`

	// End is when this device was last seen (zero value = still present).
	End time.Time `json:"end"`

	// MemoryByte is the total GPU VRAM in bytes.
	MemoryByte uint64 `json:"memoryByte"`

	// MemoryUsedByte is the average used VRAM over the window (bytes).
	MemoryUsedByte uint64 `json:"memoryUsedByte"`

	// MemoryUsedMaxByte is the peak VRAM usage over the window (bytes).
	MemoryUsedMaxByte uint64 `json:"memoryUsedMaxByte"`

	// CoreUtilisationSeconds is the cumulative GPU core utilisation
	// (utilisation_ratio x duration_seconds) over the window.
	CoreUtilisationSeconds float64 `json:"coreUtilisationSeconds"`

	// PowerWattSeconds is the cumulative power draw (watts x seconds) over the window.
	PowerWattSeconds float64 `json:"powerWattSeconds"`

	// PowerLimitWatt is the configured TDP / power limit for this device.
	PowerLimitWatt float64 `json:"powerLimitWatt"`
}

// GPUUsage represents GPU resource consumption attributed to a specific Container.
//
// GPU allocation in Kubernetes is all-or-nothing per device (whole-GPU scheduling),
// but fractional GPU via MIG (Multi-Instance GPU) or time-slicing is increasingly
// common in AI/ML workloads. This struct covers both modes.
//
// @bingen:generate
// @bingen:version=1
type GPUUsage struct {
	// ContainerUID links this usage record to its owning Container.
	ContainerUID string `json:"containerUID"`

	// DeviceUID links to the physical GPUDevice on the node.
	DeviceUID string `json:"deviceUID"`

	// AllocatedFraction is the fraction of the GPU allocated to this container.
	// 1.0 = exclusive whole-GPU, 0.5 = half via MIG or time-slice, etc.
	AllocatedFraction float64 `json:"allocatedFraction"`

	// AllocatedMemoryByte is the VRAM allocated to this container (bytes).
	AllocatedMemoryByte uint64 `json:"allocatedMemoryByte"`

	// CoreUtilisationSeconds is the cumulative attributed GPU core utilisation.
	CoreUtilisationSeconds float64 `json:"coreUtilisationSeconds"`

	// MemoryUsedByte is the average VRAM used by this container over the window.
	MemoryUsedByte uint64 `json:"memoryUsedByte"`

	// Start / End mirror the container's scheduling window for this device.
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// RegisterGPUDevice adds a GPUDevice to the KubeModelSet, indexed by UID.
// Safe to call multiple times for the same UID - later calls overwrite.
func (k *KubeModelSet) RegisterGPUDevice(device *GPUDevice) {
	if device == nil || device.UID == "" {
		return
	}
	if k.GPUDevices == nil {
		k.GPUDevices = make(map[string]*GPUDevice)
	}
	k.GPUDevices[device.UID] = device
}

// RegisterGPUUsage appends a GPUUsage record, keyed by ContainerUID+DeviceUID.
func (k *KubeModelSet) RegisterGPUUsage(usage *GPUUsage) {
	if usage == nil || usage.ContainerUID == "" || usage.DeviceUID == "" {
		return
	}
	if k.GPUUsages == nil {
		k.GPUUsages = make(map[string]*GPUUsage)
	}
	key := usage.ContainerUID + "/" + usage.DeviceUID
	k.GPUUsages[key] = usage
}
