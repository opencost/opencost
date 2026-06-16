package kubemodel

import "time"

// Accelerator devices are modeled as per-vendor concrete types (e.g.
// DCGMDevice for NVIDIA) behind small interfaces rather than one generic
// concrete struct. A generic type forces every vendor's changes into shared
// code, implies relational fields not all vendors can provide, and blurs
// semantics that differ between vendors (e.g. what "power" means for AMD vs
// NVIDIA). Overly specific shared models have the opposite failure:
// collecting more than is used and looking half-supported for vendors
// missing a feature. Vendor structs keep agent-side transformation minimal
// (data ships close to its source shape, e.g. DCGM for NVIDIA), while these
// interfaces provide the common surface for identification, utilization,
// performance, and saturation.
//
// Capacity is intentionally absent: it is not readily available from vendor
// telemetry (e.g. DCGM) and is inferred via device plugins / DRA, which are
// assumed prerequisites and provide the request half of device allocation.
// Fan speed, PCIe topology detail, and topology are out of scope.

// DeviceInfo identifies a physical or virtualized accelerator device and
// its lifecycle within the observed window.
type DeviceInfo interface {
	// GetIdentifier returns the device UUID. For virtualized devices
	// (e.g. a MIG instance) this is the UUID of the virtual device.
	GetIdentifier() string

	// GetType returns the human-readable device type, e.g. "GPU".
	GetType() string

	// GetName returns the marketing/model name, e.g. "NVIDIA A100 80GB".
	GetName() string

	// GetPower returns the device power draw in watts.
	GetPower() float64

	// GetStart returns the time the device came online in the observed
	// window.
	GetStart() time.Time

	// GetEnd returns the time the device's observation ended.
	GetEnd() time.Time

	// GetParent returns the UUID of the parent device, or empty if none.
	// For a MIG instance this is the UUID of the physical GPU.
	GetParent() string
}

// DevicePerformance exposes utilization and performance measures for an
// accelerator device.
type DevicePerformance interface {
	// GetComputeUtilizationAverage returns the average percentage of
	// compute used over the observed window.
	GetComputeUtilizationAverage() float64

	// GetComputeUtilizationMax returns the max percentage of compute used.
	GetComputeUtilizationMax() float64

	// GetMemoryUtilizationAverage returns the average absolute memory used
	// in bytes.
	GetMemoryUtilizationAverage() float64

	// GetMemoryUtilizationMax returns the max absolute memory used in
	// bytes.
	GetMemoryUtilizationMax() float64

	// GetTemp returns the current device temperature in degrees Celsius.
	GetTemp() float64
}

// DeviceSaturation exposes USE-method saturation measures for an
// accelerator device: where DevicePerformance reports how busy the device
// was, saturation reports work queued, rejected, or slowed because the
// device could not service demand.
//
// Absence is meaningful and distinct from zero: scalar getters return
// ok=false and map getters return nil when the vendor telemetry for that
// signal was unavailable, so consumers never mistake "not measured" for
// "not saturated". Names are vendor-neutral; the mapping to vendor
// telemetry (e.g. DCGM fields for NVIDIA) lives in each implementation.
// Ratios are fractions of the observed window in [0, 1].
type DeviceSaturation interface {
	// GetThrottleViolationRatios returns the fraction of the window the
	// device spent throttled, keyed by vendor-defined reason name, from
	// cumulative time-in-violation counters. Nil when unavailable.
	GetThrottleViolationRatios() map[string]float64

	// GetThrottleReasonRatios returns the fraction of the window each
	// vendor-defined throttle reason state was active, from sampled state
	// flags. Nil when unavailable.
	GetThrottleReasonRatios() map[string]float64

	// GetMemoryUsedRatioAvg returns average device-memory occupancy.
	GetMemoryUsedRatioAvg() (float64, bool)

	// GetMemoryUsedRatioMax returns peak device-memory occupancy.
	GetMemoryUsedRatioMax() (float64, bool)

	// GetMemoryPressureRatio returns the fraction of the window
	// device-memory occupancy exceeded the configured threshold.
	GetMemoryPressureRatio() (float64, bool)

	// GetErrorEventCount returns the number of device error events
	// (rejected work) observed in the window.
	GetErrorEventCount() (float64, bool)

	// GetMemoryBandwidthActiveAvg returns the average ratio of cycles the
	// device memory interface was active.
	GetMemoryBandwidthActiveAvg() (float64, bool)

	// GetMemoryBandwidthActiveMax returns the peak ratio of cycles the
	// device memory interface was active.
	GetMemoryBandwidthActiveMax() (float64, bool)

	// GetComputeActiveAvg returns the average ratio of cycles compute
	// units had work resident. Together with GetComputeOccupancyAvg and
	// GetMemoryBandwidthActiveAvg, consumers can distinguish
	// compute-bound from bandwidth- or latency-bound saturation.
	GetComputeActiveAvg() (float64, bool)

	// GetComputeOccupancyAvg returns the average ratio of resident work
	// to the compute units' maximum.
	GetComputeOccupancyAvg() (float64, bool)

	// GetHostLinkTxBytesAvg/RxBytesAvg return average host-interconnect
	// throughput (e.g. PCIe) in bytes/sec. Link capacity is not derivable
	// from vendor telemetry, so these are rates, not ratios.
	GetHostLinkTxBytesAvg() (float64, bool)
	GetHostLinkRxBytesAvg() (float64, bool)

	// GetPeerLinkTxBytesAvg/RxBytesAvg return average device-to-device
	// fabric throughput (e.g. NVLink) in bytes/sec.
	GetPeerLinkTxBytesAvg() (float64, bool)
	GetPeerLinkRxBytesAvg() (float64, bool)
}
