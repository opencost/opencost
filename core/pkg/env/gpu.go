package env

const (
	// GPUMemorySaturationThresholdEnvVar configures the framebuffer
	// occupancy ratio above which a GPU counts as memory-pressured.
	GPUMemorySaturationThresholdEnvVar = "GPU_MEMORY_SATURATION_THRESHOLD"
)

// DefaultGPUMemorySaturationThreshold is the framebuffer occupancy ratio
// above which a GPU is considered memory-pressured when no valid override
// is configured.
const DefaultGPUMemorySaturationThreshold = 0.9

// GetGPUMemorySaturationThreshold returns the configured framebuffer
// occupancy threshold for GPU memory pressure. Values outside (0, 1] are
// rejected in favor of the default.
//
// This lives in core (rather than per data-source module) because both the
// prometheus-source query builder and the collector-source aggregator must
// apply the identical threshold; two copies of the validation logic drifted
// during development and were consolidated here. (Code review finding.)
func GetGPUMemorySaturationThreshold() float64 {
	threshold := GetFloat64(GPUMemorySaturationThresholdEnvVar, DefaultGPUMemorySaturationThreshold)
	if threshold <= 0.0 || threshold > 1.0 {
		return DefaultGPUMemorySaturationThreshold
	}
	return threshold
}
