package synthetic

import (
	"math"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
)

// gpuFramebufferSample pairs the framebuffer used/free updates for one GPU
// (or MIG instance) and container within a single scrape.
type gpuFramebufferSample struct {
	used *metric.Update
	free *metric.Update
}

// GPUMemoryUsedRatioSynthesizer joins DCGM_FI_DEV_FB_USED and
// DCGM_FI_DEV_FB_FREE within each scrape and synthesizes a per-sample
// framebuffer occupancy ratio metric, used / (used + free). Joining per
// scrape is what makes time-over-threshold memory pressure computable
// downstream; post-aggregation joins cannot recover the per-sample ratio.
type GPUMemoryUsedRatioSynthesizer struct {
	byDevice map[string]*gpuFramebufferSample
}

// NewGPUMemoryUsedRatioSynthesizer creates a synthesizer producing
// OpencostGPUMemoryUsedRatio updates from the DCGM framebuffer metrics.
func NewGPUMemoryUsedRatioSynthesizer() *GPUMemoryUsedRatioSynthesizer {
	return &GPUMemoryUsedRatioSynthesizer{
		byDevice: make(map[string]*gpuFramebufferSample),
	}
}

// gpuDeviceKey identifies one GPU (or MIG instance) attached to one
// container: dcgm-exporter emits one used/free series per such pairing.
func gpuDeviceKey(labels map[string]string) string {
	return strings.Join([]string{
		labels[source.UUIDLabel],
		labels[source.MIGInstanceLabel],
		labels[source.UIDLabel],
		labels[source.ContainerLabel],
	}, "|")
}

// Process records framebuffer used/free updates; all other metrics are
// ignored.
func (s *GPUMemoryUsedRatioSynthesizer) Process(t time.Time, update *metric.Update) {
	var sample *gpuFramebufferSample
	switch update.Name {
	case metric.DCGMFIDEVFBUSED, metric.DCGMFIDEVFBFREE:
		key := gpuDeviceKey(update.Labels)
		if _, ok := s.byDevice[key]; !ok {
			s.byDevice[key] = &gpuFramebufferSample{}
		}
		sample = s.byDevice[key]
	default:
		return
	}

	if update.Name == metric.DCGMFIDEVFBUSED {
		sample.used = update
	} else {
		sample.free = update
	}
}

// Synthesize emits one occupancy ratio update per device that reported both
// framebuffer metrics this scrape. Devices missing either half, or
// reporting a non-positive or non-finite total, emit nothing.
func (s *GPUMemoryUsedRatioSynthesizer) Synthesize() []metric.Update {
	var updates []metric.Update

	for _, sample := range s.byDevice {
		if sample.used == nil || sample.free == nil {
			continue
		}
		used := sample.used.Value
		total := used + sample.free.Value
		if math.IsNaN(total) || math.IsInf(total, 0) || total <= 0 || used < 0 {
			continue
		}

		labels := make(map[string]string, len(sample.used.Labels))
		for k, v := range sample.used.Labels {
			labels[k] = v
		}

		updates = append(updates, metric.Update{
			Name:   metric.OpencostGPUMemoryUsedRatio,
			Labels: labels,
			Value:  used / total,
		})
	}

	return updates
}

// Clear resets the per-scrape state.
func (s *GPUMemoryUsedRatioSynthesizer) Clear() {
	s.byDevice = make(map[string]*gpuFramebufferSample)
}
