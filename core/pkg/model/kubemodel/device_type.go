package kubemodel

// @bingen:generate:DeviceType
type DeviceType string

const (
	// DeviceTypeGPU represents GPU devices (NVIDIA, AMD, Intel)
	DeviceTypeGPU DeviceType = "gpu"

	// Future device types (reserved for future implementation)

	// DeviceTypeTPU represents Tensor Processing Unit devices (Google TPU)
	DeviceTypeTPU DeviceType = "tpu"

	// DeviceTypeFPGA represents Field-Programmable Gate Array devices
	DeviceTypeFPGA DeviceType = "fpga"

	// DeviceTypeNPU represents Neural Processing Unit devices
	DeviceTypeNPU DeviceType = "npu"

	// DeviceTypeSmartNIC represents SmartNIC network accelerator devices
	DeviceTypeSmartNIC DeviceType = "smart-nic"

	// DeviceTypeStorageAccel represents storage controller accelerator devices
	DeviceTypeStorageAccel DeviceType = "storage-accel"

	// DeviceTypeCustomAccel represents custom accelerator devices
	DeviceTypeCustomAccel DeviceType = "custom-accel"
)

// String returns the string representation of the DeviceType
func (dt DeviceType) String() string {
	return string(dt)
}

// IsValid returns true if the DeviceType is a valid/known type
func (dt DeviceType) IsValid() bool {
	switch dt {
	case DeviceTypeGPU,
		DeviceTypeTPU,
		DeviceTypeFPGA,
		DeviceTypeNPU,
		DeviceTypeSmartNIC,
		DeviceTypeStorageAccel,
		DeviceTypeCustomAccel:
		return true
	default:
		return false
	}
}