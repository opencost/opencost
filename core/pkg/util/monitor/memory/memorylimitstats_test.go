package memory_test

import (
	"math/rand"
	"testing"

	"github.com/opencost/opencost/core/pkg/util/monitor/memory"
)

func TestObservationMode(t *testing.T) {
	config := memory.DefaultMemoryLimitConfig()
	m := memory.NewMemoryLimitStats(config)

	// Feed fewer than MinSamples — should never return updated=true.
	for i := 0; i < config.MinSamples-1; i++ {
		_, updated := m.Record(100 * 1024 * 1024) // 100 MiB
		if updated {
			t.Fatalf("sample %d: got updated=true before MinSamples reached", i)
		}
	}
	if got := m.SoftMemoryLimit(); got != 0 {
		t.Fatalf("expected SoftLimit 0 during observation, got %d", got)
	}
}

func TestLimitCommittedAfterMinSamples(t *testing.T) {
	config := memory.DefaultMemoryLimitConfig()
	m := memory.NewMemoryLimitStats(config)

	const alloc = 200 * 1024 * 1024 // 200 MiB, perfectly stable
	var lastLimit uint64
	var sawUpdate bool

	for i := 0; i < config.MinSamples+10; i++ {
		limit, updated := m.Record(alloc)
		if updated {
			sawUpdate = true
			lastLimit = limit
		}
	}

	if !sawUpdate {
		t.Fatal("expected at least one limit update after MinSamples")
	}

	// Soft limit should be ~90% of the stable allocation.
	expected := uint64(float64(alloc) * 0.90)
	delta := int64(lastLimit) - int64(expected)
	if delta < 0 {
		delta = -delta
	}
	// Allow 1% tolerance.
	if delta > int64(expected)/100 {
		t.Fatalf("soft limit %d too far from expected %d (delta %d)", lastLimit, expected, delta)
	}
}

func TestElasticRecalibrationOnGrowth(t *testing.T) {
	config := memory.DefaultMemoryLimitConfig()
	config.BreachWindowSize = 5
	config.BreachThreshold = 3
	m := memory.NewMemoryLimitStats(config)

	// Phase 1: stable at 100 MiB — establish a limit.
	for i := 0; i < config.MinSamples+20; i++ {
		m.Record(100 * 1024 * 1024)
	}
	limitBefore := m.SoftMemoryLimit()
	if limitBefore == 0 {
		t.Fatal("expected a non-zero limit after phase 1")
	}

	// Phase 2: spike to 300 MiB repeatedly — should trigger recalibration.
	for i := 0; i < config.BreachThreshold+1; i++ {
		m.Record(500 * 1024 * 1024)
	}

	// Phase 3: feed enough samples at new level to re-commit.
	var recalibrated bool
	for i := 0; i < config.MinSamples+20; i++ {
		limit, updated := m.Record(800 * 1024 * 1024)
		if updated && limit > limitBefore {
			recalibrated = true
			break
		}
	}
	if !recalibrated {
		t.Fatal("expected the soft limit to grow after sustained high usage")
	}
}

func TestReset(t *testing.T) {
	config := memory.DefaultMemoryLimitConfig()
	m := memory.NewMemoryLimitStats(config)

	for i := 0; i < config.MinSamples+5; i++ {
		m.Record(128 * 1024 * 1024)
	}
	if m.SoftMemoryLimit() == 0 {
		t.Fatal("expected non-zero soft limit before reset")
	}

	m.Reset()

	if m.SoftMemoryLimit() != 0 {
		t.Fatal("expected zero soft limit after reset")
	}
	if m.TotalSamples() != 0 {
		t.Fatal("expected zero sample count after reset")
	}
}

func TestNoisyInputStability(t *testing.T) {
	config := memory.DefaultMemoryLimitConfig()
	m := memory.NewMemoryLimitStats(config)

	rng := rand.New(rand.NewSource(42))
	base := float64(256 * 1024 * 1024) // 256 MiB

	var limits []uint64
	for i := 0; i < 200; i++ {
		// ±10% noise around base
		noise := (rng.Float64()*0.2 - 0.1) * base
		_, _ = m.Record(uint64(base + noise))
		if l := m.SoftMemoryLimit(); l > 0 {
			limits = append(limits, l)
		}
	}

	if len(limits) == 0 {
		t.Fatal("expected at least one committed limit")
	}

	// The final limit should be in a sensible range: 75–95% of base.
	last := limits[len(limits)-1]
	lo := uint64(base * 0.75)
	hi := uint64(base * 0.95)
	if last < lo || last > hi {
		t.Fatalf("final limit %d outside expected range [%d, %d]", last, lo, hi)
	}
}
