package opencost

import (
	"maps"
	"math"
)

// GPU throttle reason bits as reported by the DCGM field
// DCGM_FI_DEV_CLOCK_THROTTLE_REASONS (renamed DCGM_FI_DEV_CLOCKS_EVENT_REASONS
// in DCGM 3.3+). The bit positions are defined by NVML's
// nvmlClocksThrottleReasons constants:
// https://docs.nvidia.com/deploy/nvml-api/group__nvmlClocksThrottleReasons.html
//
// Only saturation-relevant bits are enumerated here. The remaining bits
// (gpu_idle 0x1, applications_clocks_setting 0x2, display_clock_setting 0x100)
// describe configured operating states rather than the GPU being unable to
// service demand, so they are intentionally excluded.
const (
	GPUThrottleBitSwPowerCap   uint64 = 0x4  // clocks reduced by software power cap
	GPUThrottleBitHwSlowdown   uint64 = 0x8  // hardware slowdown (thermal or power brake) engaged
	GPUThrottleBitSyncBoost    uint64 = 0x10 // clocks lowered to match a sync-boost group
	GPUThrottleBitSwThermal    uint64 = 0x20 // software thermal slowdown
	GPUThrottleBitHwThermal    uint64 = 0x40 // hardware thermal slowdown
	GPUThrottleBitHwPowerBrake uint64 = 0x80 // hardware power brake slowdown
)

// Canonical names for saturation-relevant GPU throttle reasons. These are the
// keys used in GPUSaturation.ThrottleReasonRatios.
const (
	GPUThrottleReasonSwPowerCap   = "sw_power_cap"
	GPUThrottleReasonHwSlowdown   = "hw_slowdown"
	GPUThrottleReasonSyncBoost    = "sync_boost"
	GPUThrottleReasonSwThermal    = "sw_thermal"
	GPUThrottleReasonHwThermal    = "hw_thermal"
	GPUThrottleReasonHwPowerBrake = "hw_power_brake"
)

// Canonical names for GPU throttle violation counters reported by DCGM. These
// are the keys used in GPUSaturation.ThrottleViolationRatios. Each maps to a
// cumulative microsecond counter in the default dcgm-exporter configuration:
// DCGM_FI_DEV_POWER_VIOLATION, DCGM_FI_DEV_THERMAL_VIOLATION,
// DCGM_FI_DEV_SYNC_BOOST_VIOLATION, and DCGM_FI_DEV_BOARD_LIMIT_VIOLATION.
const (
	GPUThrottleViolationPower      = "power"
	GPUThrottleViolationThermal    = "thermal"
	GPUThrottleViolationSyncBoost  = "sync_boost"
	GPUThrottleViolationBoardLimit = "board_limit"
)

// GPUThrottleReason pairs a canonical throttle reason name with its bit in the
// DCGM clock throttle reasons bitmask.
type GPUThrottleReason struct {
	Name string
	Bit  uint64
}

// GPUThrottleReasons enumerates every saturation-relevant throttle reason, in
// ascending bit order. It is the single source of truth for bitmask decoding
// and for generating per-reason Prometheus queries.
var GPUThrottleReasons = []GPUThrottleReason{
	{Name: GPUThrottleReasonSwPowerCap, Bit: GPUThrottleBitSwPowerCap},
	{Name: GPUThrottleReasonHwSlowdown, Bit: GPUThrottleBitHwSlowdown},
	{Name: GPUThrottleReasonSyncBoost, Bit: GPUThrottleBitSyncBoost},
	{Name: GPUThrottleReasonSwThermal, Bit: GPUThrottleBitSwThermal},
	{Name: GPUThrottleReasonHwThermal, Bit: GPUThrottleBitHwThermal},
	{Name: GPUThrottleReasonHwPowerBrake, Bit: GPUThrottleBitHwPowerBrake},
}

// GPUThrottleReasonsFromMask decodes a DCGM clock throttle reasons bitmask
// into the canonical names of the active saturation-relevant reasons.
// Non-saturation bits are ignored. A zero mask decodes to an empty slice.
func GPUThrottleReasonsFromMask(mask uint64) []string {
	reasons := make([]string, 0, len(GPUThrottleReasons))
	for _, reason := range GPUThrottleReasons {
		if mask&reason.Bit != 0 {
			reasons = append(reasons, reason.Name)
		}
	}
	return reasons
}

// GPUSaturation carries per-GPU saturation signals derived from DCGM exporter
// metrics, following the USE method: where utilization reports how busy the
// GPU was, saturation reports work that was queued, rejected, or slowed
// because the GPU could not service demand.
//
// Every field is an independent primitive; no composite score is computed.
// A nil field means the underlying DCGM metric was not available in the
// window (no dcgm-exporter, field disabled in its config, or no DCP
// profiling support), never that the value was zero.
//
// Ratios are fractions of the queried window in [0, 1] unless noted.
type GPUSaturation struct {
	// ThrottleViolationRatios maps a GPUThrottleViolation* name to the
	// fraction of the window the GPU spent throttled for that reason,
	// derived from the cumulative DCGM_FI_DEV_*_VIOLATION microsecond
	// counters. These counters are part of the default dcgm-exporter
	// configuration.
	ThrottleViolationRatios map[string]float64 `json:"throttleViolationRatios,omitempty"`
	// ThrottleReasonRatios maps a GPUThrottleReason* name to the fraction
	// of the window the corresponding DCGM_FI_DEV_CLOCK_THROTTLE_REASONS
	// bit was set. That field is not in the default dcgm-exporter
	// configuration and must be enabled explicitly. Reported for the whole
	// physical GPU, even when MIG or time-slicing is in use.
	ThrottleReasonRatios map[string]float64 `json:"throttleReasonRatios,omitempty"`
	// MemoryUsedRatioAvg/Max are framebuffer occupancy over the window:
	// DCGM_FI_DEV_FB_USED / (DCGM_FI_DEV_FB_USED + DCGM_FI_DEV_FB_FREE).
	// Default dcgm-exporter configuration.
	MemoryUsedRatioAvg *float64 `json:"memoryUsedRatioAvg,omitempty"`
	MemoryUsedRatioMax *float64 `json:"memoryUsedRatioMax,omitempty"`
	// MemoryPressureRatio is the fraction of the window the framebuffer
	// occupancy was above the configured threshold (default 0.9).
	MemoryPressureRatio *float64 `json:"memoryPressureRatio,omitempty"`
	// XIDErrorCount counts distinct XID error events observed in the window
	// via changes in DCGM_FI_DEV_XID_ERRORS. That field reports the last
	// XID code seen, so repeats of the same error code are undercounted.
	XIDErrorCount *float64 `json:"xidErrorCount,omitempty"`
	// DRAMActiveAvg/Max are the ratio of cycles the device memory interface
	// was active (DCGM_FI_PROF_DRAM_ACTIVE). Requires DCP profiling
	// (Volta+). Sustained values near 1.0 with low SMOccupancyAvg indicate
	// a memory-bandwidth-bound workload.
	DRAMActiveAvg *float64 `json:"dramActiveAvg,omitempty"`
	DRAMActiveMax *float64 `json:"dramActiveMax,omitempty"`
	// SMActiveAvg (DCGM_FI_PROF_SM_ACTIVE) and SMOccupancyAvg
	// (DCGM_FI_PROF_SM_OCCUPANCY) are provided so consumers can
	// distinguish compute-bound from bandwidth- or latency-bound
	// saturation. Requires DCP profiling and is commented out of the
	// default dcgm-exporter configuration.
	SMActiveAvg    *float64 `json:"smActiveAvg,omitempty"`
	SMOccupancyAvg *float64 `json:"smOccupancyAvg,omitempty"`
	// PCIe/NVLink average throughput in bytes/sec over the window, from
	// rate() of DCGM_FI_PROF_PCIE_TX/RX_BYTES and
	// DCGM_FI_PROF_NVLINK_TX/RX_BYTES counters. Requires DCP profiling;
	// the NVLink fields are commented out of the default dcgm-exporter
	// configuration. Link capacity is not derivable from DCGM, so these
	// are raw rates rather than ratios.
	PCIeTxBytesAvg   *float64 `json:"pcieTxBytesAvg,omitempty"`
	PCIeRxBytesAvg   *float64 `json:"pcieRxBytesAvg,omitempty"`
	NVLinkTxBytesAvg *float64 `json:"nvlinkTxBytesAvg,omitempty"`
	NVLinkRxBytesAvg *float64 `json:"nvlinkRxBytesAvg,omitempty"`
}

// scalarFields returns pointers to every scalar field, so SanitizeNaN,
// Equal, and IsEmpty cannot silently miss a newly added field.
func (orig *GPUSaturation) scalarFields() []**float64 {
	return []**float64{
		&orig.MemoryUsedRatioAvg,
		&orig.MemoryUsedRatioMax,
		&orig.MemoryPressureRatio,
		&orig.XIDErrorCount,
		&orig.DRAMActiveAvg,
		&orig.DRAMActiveMax,
		&orig.SMActiveAvg,
		&orig.SMOccupancyAvg,
		&orig.PCIeTxBytesAvg,
		&orig.PCIeRxBytesAvg,
		&orig.NVLinkTxBytesAvg,
		&orig.NVLinkRxBytesAvg,
	}
}

// SanitizeNaN removes NaN values: NaN scalars become nil and NaN map entries
// are deleted, so absence is always represented the same way.
func (orig *GPUSaturation) SanitizeNaN() {
	if orig == nil {
		return
	}
	for _, field := range orig.scalarFields() {
		if *field != nil && math.IsNaN(**field) {
			*field = nil
		}
	}
	for _, m := range []map[string]float64{orig.ThrottleViolationRatios, orig.ThrottleReasonRatios} {
		for k, v := range m {
			if math.IsNaN(v) {
				delete(m, k)
			}
		}
	}
}

// Clone returns a deep copy of the GPUSaturation.
func (orig *GPUSaturation) Clone() *GPUSaturation {
	if orig == nil {
		return nil
	}

	clone := &GPUSaturation{
		ThrottleViolationRatios: maps.Clone(orig.ThrottleViolationRatios),
		ThrottleReasonRatios:    maps.Clone(orig.ThrottleReasonRatios),
	}

	origFields := orig.scalarFields()
	cloneFields := clone.scalarFields()
	for i := range origFields {
		if *origFields[i] != nil {
			v := **origFields[i]
			*cloneFields[i] = &v
		}
	}
	return clone
}

// Equal compares two GPUSaturation values field by field. Scalar fields are
// equal when both are nil or both point to the same value.
func (orig *GPUSaturation) Equal(that *GPUSaturation) bool {
	if orig == nil && that == nil {
		return true
	}
	if orig == nil || that == nil {
		return false
	}

	if !maps.Equal(orig.ThrottleViolationRatios, that.ThrottleViolationRatios) {
		return false
	}
	if !maps.Equal(orig.ThrottleReasonRatios, that.ThrottleReasonRatios) {
		return false
	}

	origFields := orig.scalarFields()
	thatFields := that.scalarFields()
	for i := range origFields {
		a, b := *origFields[i], *thatFields[i]
		if (a == nil) != (b == nil) {
			return false
		}
		if a != nil && *a != *b {
			return false
		}
	}
	return true
}

// IsEmpty reports whether no saturation signal is present at all, in which
// case the GPUSaturation should be omitted rather than serialized.
func (orig *GPUSaturation) IsEmpty() bool {
	if orig == nil {
		return true
	}
	if len(orig.ThrottleViolationRatios) > 0 || len(orig.ThrottleReasonRatios) > 0 {
		return false
	}
	for _, field := range orig.scalarFields() {
		if *field != nil {
			return false
		}
	}
	return true
}
