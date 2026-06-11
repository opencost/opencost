package opencost

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
