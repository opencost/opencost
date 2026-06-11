package synthetic

import (
	"math"
	"testing"
	"time"

	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
)

func gpuFBUpdate(name string, uuid, migInstance string, value float64) *metric.Update {
	return &metric.Update{
		Name: name,
		Labels: map[string]string{
			"UUID":      uuid,
			"GPU_I_ID":  migInstance,
			"uid":       "pod-uuid1",
			"container": "container1",
			"namespace": "namespace1",
			"pod":       "pod1",
		},
		Value: value,
	}
}

func TestGPUMemoryUsedRatioSynthesizer(t *testing.T) {
	now := time.Now()

	t.Run("joins used and free into a ratio", func(t *testing.T) {
		s := NewGPUMemoryUsedRatioSynthesizer()
		s.Process(now, gpuFBUpdate(metric.DCGMFIDEVFBUSED, "GPU-1", "", 12000))
		s.Process(now, gpuFBUpdate(metric.DCGMFIDEVFBFREE, "GPU-1", "", 4000))

		updates := s.Synthesize()
		if len(updates) != 1 {
			t.Fatalf("expected 1 synthetic update, got %d", len(updates))
		}
		got := updates[0]
		if got.Name != metric.OpencostGPUMemoryUsedRatio {
			t.Errorf("Name = %q, want %q", got.Name, metric.OpencostGPUMemoryUsedRatio)
		}
		if got.Value != 0.75 {
			t.Errorf("Value = %v, want 0.75", got.Value)
		}
		if got.Labels["UUID"] != "GPU-1" || got.Labels["container"] != "container1" {
			t.Errorf("labels not carried through: %v", got.Labels)
		}
	})

	t.Run("MIG instances synthesize independently", func(t *testing.T) {
		s := NewGPUMemoryUsedRatioSynthesizer()
		s.Process(now, gpuFBUpdate(metric.DCGMFIDEVFBUSED, "GPU-1", "1", 5000))
		s.Process(now, gpuFBUpdate(metric.DCGMFIDEVFBFREE, "GPU-1", "1", 5000))
		s.Process(now, gpuFBUpdate(metric.DCGMFIDEVFBUSED, "GPU-1", "2", 2000))
		s.Process(now, gpuFBUpdate(metric.DCGMFIDEVFBFREE, "GPU-1", "2", 8000))

		updates := s.Synthesize()
		if len(updates) != 2 {
			t.Fatalf("expected 2 synthetic updates, got %d", len(updates))
		}
		byInstance := map[string]float64{}
		for _, u := range updates {
			byInstance[u.Labels["GPU_I_ID"]] = u.Value
		}
		if byInstance["1"] != 0.5 || byInstance["2"] != 0.2 {
			t.Errorf("per-instance ratios = %v, want {1:0.5, 2:0.2}", byInstance)
		}
	})

	t.Run("missing half emits nothing", func(t *testing.T) {
		s := NewGPUMemoryUsedRatioSynthesizer()
		s.Process(now, gpuFBUpdate(metric.DCGMFIDEVFBUSED, "GPU-1", "", 12000))
		if updates := s.Synthesize(); len(updates) != 0 {
			t.Errorf("expected no updates without FB_FREE, got %v", updates)
		}
	})

	t.Run("invalid totals emit nothing", func(t *testing.T) {
		cases := map[string][2]float64{
			"zero total":     {0, 0},
			"negative used":  {-1, 100},
			"NaN free":       {100, math.NaN()},
			"infinite total": {math.Inf(1), 100},
			// negative free with positive total would yield ratio > 1
			// (100/(100-50) = 2.0) if only the total were validated
			"negative free": {100, -50},
		}
		for name, values := range cases {
			s := NewGPUMemoryUsedRatioSynthesizer()
			s.Process(now, gpuFBUpdate(metric.DCGMFIDEVFBUSED, "GPU-1", "", values[0]))
			s.Process(now, gpuFBUpdate(metric.DCGMFIDEVFBFREE, "GPU-1", "", values[1]))
			if updates := s.Synthesize(); len(updates) != 0 {
				t.Errorf("%s: expected no updates, got %v", name, updates)
			}
		}
	})

	t.Run("unrelated metrics are ignored", func(t *testing.T) {
		s := NewGPUMemoryUsedRatioSynthesizer()
		s.Process(now, gpuFBUpdate(metric.DCGMFIPROFGRENGINEACTIVE, "GPU-1", "", 0.9))
		if updates := s.Synthesize(); len(updates) != 0 {
			t.Errorf("expected no updates for unrelated metric, got %v", updates)
		}
	})

	t.Run("Clear resets state between scrapes", func(t *testing.T) {
		s := NewGPUMemoryUsedRatioSynthesizer()
		s.Process(now, gpuFBUpdate(metric.DCGMFIDEVFBUSED, "GPU-1", "", 12000))
		s.Clear()
		s.Process(now, gpuFBUpdate(metric.DCGMFIDEVFBFREE, "GPU-1", "", 4000))
		if updates := s.Synthesize(); len(updates) != 0 {
			t.Errorf("expected no join across Clear, got %v", updates)
		}
	})
}
