package kubemodel

import (
	"fmt"
	"time"

	"maps"
)

// DCGMDevice holds recording from the DCGM exporter which provides identification and usage metrics for
// Nvidia gpu. These Nvidia devices can be incorporated into the cluster via k8s Device Plugin API or DRAs.
// While the DCGM exporter does provide unique identifiers for the containers that it is reporting metrics on,
// It is split out here to provide some isolate from the rest of the KubeModel which represent universal structures
// from the k8s API. It is left to the end user to interpret the relationships to the rest of the cluster based on
// container unique identifiers
// @bingen:generate:DCGMDevice
type DCGMDevice struct {
	UUID      string             `json:"uuid"`
	Start     time.Time          `json:"start"`
	End       time.Time          `json:"end"`
	Device    string             `json:"device"`
	ModelName string             `json:"modelName"`
	PodUsages map[string]DCGMPod `json:"podUsages"`

	// USE-method saturation signals for this device, nil when no
	// saturation metrics were available (see DCGMDeviceSaturation)
	Saturation *DCGMDeviceSaturation `json:"saturation,omitempty"` // @bingen:field[version=3]
}

var (
	_ DeviceInfo       = (*DCGMDevice)(nil)
	_ DeviceSaturation = (*DCGMDevice)(nil)
)

// DeviceInfo implementation. Power draw and MIG parentage are not yet
// recorded from DCGM, so GetPower reports 0 and GetParent reports empty
// until that collection lands.
func (d *DCGMDevice) GetIdentifier() string { return d.UUID }
func (d *DCGMDevice) GetType() string       { return "GPU" }
func (d *DCGMDevice) GetName() string       { return d.ModelName }
func (d *DCGMDevice) GetPower() float64     { return 0 }
func (d *DCGMDevice) GetStart() time.Time   { return d.Start }
func (d *DCGMDevice) GetEnd() time.Time     { return d.End }
func (d *DCGMDevice) GetParent() string     { return "" }

// DeviceSaturation implementation. The vendor-neutral getters map onto
// DCGM concepts: throttle violation counters DCGM_FI_DEV_*_VIOLATION,
// throttle reason bitmask DCGM_FI_DEV_CLOCK_THROTTLE_REASONS, framebuffer
// occupancy FB_USED/(FB_USED+FB_FREE), error events = XID errors, memory
// bandwidth = DRAM_ACTIVE, compute active/occupancy = SM_ACTIVE /
// SM_OCCUPANCY, host link = PCIe, peer link = NVLink. ok=false / nil means
// the DCGM field was unavailable, never zero.

// saturationValue adapts the saturation struct's pointer fields to the
// interface's (value, ok) presence contract, including when Saturation is
// entirely nil.
func (d *DCGMDevice) saturationValue(get func(*DCGMDeviceSaturation) *float64) (float64, bool) {
	if d.Saturation == nil {
		return 0, false
	}
	v := get(d.Saturation)
	if v == nil {
		return 0, false
	}
	return *v, true
}

func (d *DCGMDevice) GetThrottleViolationRatios() map[string]float64 {
	if d.Saturation == nil {
		return nil
	}
	return maps.Clone(d.Saturation.ThrottleViolationRatios)
}

func (d *DCGMDevice) GetThrottleReasonRatios() map[string]float64 {
	if d.Saturation == nil {
		return nil
	}
	return maps.Clone(d.Saturation.ThrottleReasonRatios)
}

func (d *DCGMDevice) GetMemoryUsedRatioAvg() (float64, bool) {
	return d.saturationValue(func(s *DCGMDeviceSaturation) *float64 { return s.MemoryUsedRatioAvg })
}

func (d *DCGMDevice) GetMemoryUsedRatioMax() (float64, bool) {
	return d.saturationValue(func(s *DCGMDeviceSaturation) *float64 { return s.MemoryUsedRatioMax })
}

func (d *DCGMDevice) GetMemoryPressureRatio() (float64, bool) {
	return d.saturationValue(func(s *DCGMDeviceSaturation) *float64 { return s.MemoryPressureRatio })
}

func (d *DCGMDevice) GetErrorEventCount() (float64, bool) {
	return d.saturationValue(func(s *DCGMDeviceSaturation) *float64 { return s.XIDErrorCount })
}

func (d *DCGMDevice) GetMemoryBandwidthActiveAvg() (float64, bool) {
	return d.saturationValue(func(s *DCGMDeviceSaturation) *float64 { return s.DRAMActiveAvg })
}

func (d *DCGMDevice) GetMemoryBandwidthActiveMax() (float64, bool) {
	return d.saturationValue(func(s *DCGMDeviceSaturation) *float64 { return s.DRAMActiveMax })
}

func (d *DCGMDevice) GetComputeActiveAvg() (float64, bool) {
	return d.saturationValue(func(s *DCGMDeviceSaturation) *float64 { return s.SMActiveAvg })
}

func (d *DCGMDevice) GetComputeOccupancyAvg() (float64, bool) {
	return d.saturationValue(func(s *DCGMDeviceSaturation) *float64 { return s.SMOccupancyAvg })
}

func (d *DCGMDevice) GetHostLinkTxBytesAvg() (float64, bool) {
	return d.saturationValue(func(s *DCGMDeviceSaturation) *float64 { return s.PCIeTxBytesAvg })
}

func (d *DCGMDevice) GetHostLinkRxBytesAvg() (float64, bool) {
	return d.saturationValue(func(s *DCGMDeviceSaturation) *float64 { return s.PCIeRxBytesAvg })
}

func (d *DCGMDevice) GetPeerLinkTxBytesAvg() (float64, bool) {
	return d.saturationValue(func(s *DCGMDeviceSaturation) *float64 { return s.NVLinkTxBytesAvg })
}

func (d *DCGMDevice) GetPeerLinkRxBytesAvg() (float64, bool) {
	return d.saturationValue(func(s *DCGMDeviceSaturation) *float64 { return s.NVLinkRxBytesAvg })
}

// @bingen:generate:DCGMPod
type DCGMPod struct {
	ContainerUsages map[string]DCGMContainer `json:"container-usages"`
}

// @bingen:generate:DCGMContainer
type DCGMContainer struct {
	UsageAvg float64 `json:"usageAvg"`
	UsageMax float64 `json:"usageMax"`
}

func (d *DCGMDevice) ValidateDCGMDevice(window Window) error {
	if d.UUID == "" {
		return fmt.Errorf("UUID is missing for DCGMDevice with device '%s'", d.Device)
	}

	if err := checkWindow(window, d.Start, d.End); err != nil {
		return err
	}

	if err := d.Saturation.Validate(); err != nil {
		return fmt.Errorf("invalid Saturation for DCGMDevice '%s': %w", d.UUID, err)
	}

	return nil
}

// RegisterDCGMDevice validates and adds a DCGMDevice to the set, keyed by UUID.
func (kms *KubeModelSet) RegisterDCGMDevice(device *DCGMDevice) error {
	if err := device.ValidateDCGMDevice(kms.Window); err != nil {
		err = fmt.Errorf("RegisterDCGMDevice: invalid dcgm device: %w", err)
		kms.Error(err)
		return err
	}

	if _, ok := kms.DCGMDevices[device.UUID]; !ok {
		if kms.Cluster == nil {
			kms.Warnf("RegisterDCGMDevice: Cluster is nil")
		}

		kms.DCGMDevices[device.UUID] = device

		kms.Metadata.ObjectCount++
	}

	return nil
}
