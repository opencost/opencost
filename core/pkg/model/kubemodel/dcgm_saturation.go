package kubemodel

import (
	"fmt"
	"math"

	"maps"
)

// DCGMDeviceSaturation carries USE-method saturation signals for an NVIDIA
// GPU recorded from the DCGM exporter: where utilization reports how busy
// the device was, saturation reports work queued, rejected, or slowed
// because the device could not service demand. It lives on DCGMDevice
// because these signals are NVIDIA/DCGM-specific; the vendor-neutral
// surface is the DeviceSaturation interface.
//
// Every field is an independent primitive; no composite score is computed.
// A nil field means the underlying source metric was not available in the
// window (no dcgm-exporter, field disabled in its config, or no DCP
// profiling support), never that the value was zero. Ratios are fractions
// of the observed window in [0, 1] unless noted.
// @bingen:generate:DCGMDeviceSaturation
type DCGMDeviceSaturation struct {
	// ThrottleViolationRatios maps a throttle reason (power, thermal,
	// sync_boost, board_limit) to the fraction of the window the device
	// spent throttled for that reason, from the cumulative
	// DCGM_FI_DEV_*_VIOLATION microsecond counters.
	ThrottleViolationRatios map[string]float64 `json:"throttleViolationRatios,omitempty"`
	// ThrottleReasonRatios maps a throttle reason (sw_power_cap,
	// hw_slowdown, sync_boost, sw_thermal, hw_thermal, hw_power_brake) to
	// the fraction of the window the corresponding bit of
	// DCGM_FI_DEV_CLOCK_THROTTLE_REASONS was set.
	ThrottleReasonRatios map[string]float64 `json:"throttleReasonRatios,omitempty"`
	// MemoryUsedRatioAvg/Max are framebuffer occupancy over the window:
	// FB_USED / (FB_USED + FB_FREE).
	MemoryUsedRatioAvg *float64 `json:"memoryUsedRatioAvg,omitempty"`
	MemoryUsedRatioMax *float64 `json:"memoryUsedRatioMax,omitempty"`
	// MemoryPressureRatio is the fraction of the window framebuffer
	// occupancy exceeded the configured threshold.
	MemoryPressureRatio *float64 `json:"memoryPressureRatio,omitempty"`
	// XIDErrorCount counts XID error events observed in the window.
	XIDErrorCount *float64 `json:"xidErrorCount,omitempty"`
	// DRAMActiveAvg/Max are the ratio of cycles the device memory interface
	// was active (DCP profiling).
	DRAMActiveAvg *float64 `json:"dramActiveAvg,omitempty"`
	DRAMActiveMax *float64 `json:"dramActiveMax,omitempty"`
	// SMActiveAvg and SMOccupancyAvg distinguish compute-bound from
	// bandwidth- or latency-bound saturation (DCP profiling).
	SMActiveAvg    *float64 `json:"smActiveAvg,omitempty"`
	SMOccupancyAvg *float64 `json:"smOccupancyAvg,omitempty"`
	// Interconnect throughput in bytes/sec (DCP profiling). Link capacity
	// is not derivable from DCGM, so these are raw rates, not ratios.
	PCIeTxBytesAvg   *float64 `json:"pcieTxBytesAvg,omitempty"`
	PCIeRxBytesAvg   *float64 `json:"pcieRxBytesAvg,omitempty"`
	NVLinkTxBytesAvg *float64 `json:"nvlinkTxBytesAvg,omitempty"`
	NVLinkRxBytesAvg *float64 `json:"nvlinkRxBytesAvg,omitempty"`
}

// ratioFields returns name/value pairs for every field constrained to
// [0, 1], for validation.
func (s *DCGMDeviceSaturation) ratioFields() map[string]*float64 {
	return map[string]*float64{
		"MemoryUsedRatioAvg":  s.MemoryUsedRatioAvg,
		"MemoryUsedRatioMax":  s.MemoryUsedRatioMax,
		"MemoryPressureRatio": s.MemoryPressureRatio,
		"DRAMActiveAvg":       s.DRAMActiveAvg,
		"DRAMActiveMax":       s.DRAMActiveMax,
		"SMActiveAvg":         s.SMActiveAvg,
		"SMOccupancyAvg":      s.SMOccupancyAvg,
	}
}

// Validate validates the DCGMDeviceSaturation fields.
func (s *DCGMDeviceSaturation) Validate() error {
	if s == nil {
		return nil
	}
	for _, m := range []map[string]float64{s.ThrottleViolationRatios, s.ThrottleReasonRatios} {
		for reason, ratio := range m {
			if math.IsNaN(ratio) || ratio < 0 || ratio > 1 {
				return fmt.Errorf("throttle ratio for %q must be 0-1, got %v", reason, ratio)
			}
		}
	}
	for name, value := range s.ratioFields() {
		if value != nil && (math.IsNaN(*value) || *value < 0 || *value > 1) {
			return fmt.Errorf("%s must be 0-1, got %v", name, *value)
		}
	}
	for name, value := range map[string]*float64{
		"XIDErrorCount":    s.XIDErrorCount,
		"PCIeTxBytesAvg":   s.PCIeTxBytesAvg,
		"PCIeRxBytesAvg":   s.PCIeRxBytesAvg,
		"NVLinkTxBytesAvg": s.NVLinkTxBytesAvg,
		"NVLinkRxBytesAvg": s.NVLinkRxBytesAvg,
	} {
		if value != nil && (math.IsNaN(*value) || *value < 0) {
			return fmt.Errorf("%s cannot be negative, got %v", name, *value)
		}
	}
	return nil
}

// Clone creates a deep copy of the DCGMDeviceSaturation.
func (s *DCGMDeviceSaturation) Clone() *DCGMDeviceSaturation {
	if s == nil {
		return nil
	}

	cloneFloat := func(v *float64) *float64 {
		if v == nil {
			return nil
		}
		c := *v
		return &c
	}

	return &DCGMDeviceSaturation{
		ThrottleViolationRatios: maps.Clone(s.ThrottleViolationRatios),
		ThrottleReasonRatios:    maps.Clone(s.ThrottleReasonRatios),
		MemoryUsedRatioAvg:      cloneFloat(s.MemoryUsedRatioAvg),
		MemoryUsedRatioMax:      cloneFloat(s.MemoryUsedRatioMax),
		MemoryPressureRatio:     cloneFloat(s.MemoryPressureRatio),
		XIDErrorCount:           cloneFloat(s.XIDErrorCount),
		DRAMActiveAvg:           cloneFloat(s.DRAMActiveAvg),
		DRAMActiveMax:           cloneFloat(s.DRAMActiveMax),
		SMActiveAvg:             cloneFloat(s.SMActiveAvg),
		SMOccupancyAvg:          cloneFloat(s.SMOccupancyAvg),
		PCIeTxBytesAvg:          cloneFloat(s.PCIeTxBytesAvg),
		PCIeRxBytesAvg:          cloneFloat(s.PCIeRxBytesAvg),
		NVLinkTxBytesAvg:        cloneFloat(s.NVLinkTxBytesAvg),
		NVLinkRxBytesAvg:        cloneFloat(s.NVLinkRxBytesAvg),
	}
}
