package anomaly

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

func TestStats(t *testing.T) {
	data := []float64{10.0, 12.0, 11.0, 9.0, 10.0}
	mean, stdDev := stats(data)

	expectedMean := 10.4
	if mean != expectedMean {
		t.Errorf("expected mean %f, got %f", expectedMean, mean)
	}

	expectedStdDev := 1.140175
	if mathAbs(stdDev-expectedStdDev) > 1e-5 {
		t.Errorf("expected stdDev ~%f, got %f", expectedStdDev, stdDev)
	}
}

func TestMadStats(t *testing.T) {
	data := []float64{10.0, 12.0, 11.0, 9.0, 100.0} // 100 is an outlier
	median, mad := madStats(data)

	expectedMedian := 11.0
	if median != expectedMedian {
		t.Errorf("expected median %f, got %f", expectedMedian, median)
	}

	// absolute deviations from 11.0: |10-11|=1, |12-11|=1, |11-11|=0, |9-11|=2, |100-11|=89
	// sorted deviations: 0, 1, 1, 2, 89
	// median of deviations: 1
	expectedMad := 1.0
	if mad != expectedMad {
		t.Errorf("expected MAD %f, got %f", expectedMad, mad)
	}
}

func TestDetect(t *testing.T) {
	// Create mock AllocationSetRange
	// We will create a time series of 10 daily points:
	// 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 100.0 (Spike on the last day)
	start := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	step := 24 * time.Hour
	lookback := 7 * 24 * time.Hour

	var allocs []*opencost.AllocationSet
	for i := 0; i < 10; i++ {
		s := start.Add(time.Duration(i) * step)
		e := s.Add(step)

		cost := 10.0
		if i == 9 {
			cost = 100.0 // spike
		}

		alloc := &opencost.Allocation{
			Name:    "test-namespace",
			Start:   s,
			End:     e,
			CPUCost: cost,
		}
		as := opencost.NewAllocationSet(s, e, alloc)
		allocs = append(allocs, as)
	}
	asr := opencost.NewAllocationSetRange(allocs...)

	// Run detection with MAD (default threshold 3.5)
	reports, err := Detect(asr, step, lookback, "mad", 3.5, 0.10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(reports) != 1 {
		t.Fatalf("expected 1 report, got %d", len(reports))
	}

	report := reports[0]
	if report.Key != "test-namespace" {
		t.Errorf("expected key test-namespace, got %s", report.Key)
	}

	if len(report.Anomalies) != 1 {
		t.Fatalf("expected 1 anomaly, got %d", len(report.Anomalies))
	}

	anomaly := report.Anomalies[0]
	if anomaly.Cost != 100.0 {
		t.Errorf("expected anomaly cost to be 100.0, got %f", anomaly.Cost)
	}

	// Run detection with Z-Score (threshold 3.0)
	reportsZ, err := Detect(asr, step, lookback, "zscore", 3.0, 0.10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(reportsZ) != 1 || len(reportsZ[0].Anomalies) != 1 {
		t.Fatalf("expected 1 Z-score anomaly, got %v", reportsZ)
	}

	if reportsZ[0].Anomalies[0].Cost != 100.0 {
		t.Errorf("expected Z-score anomaly cost to be 100.0, got %f", reportsZ[0].Anomalies[0].Cost)
	}
}

func TestMinCostThreshold(t *testing.T) {
	// Spiking from 0.0001 to 0.0010 (10x increase, but below minCost $0.10)
	start := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	step := 24 * time.Hour
	lookback := 7 * 24 * time.Hour

	var allocs []*opencost.AllocationSet
	for i := 0; i < 10; i++ {
		s := start.Add(time.Duration(i) * step)
		e := s.Add(step)

		cost := 0.0001
		if i == 9 {
			cost = 0.0010
		}

		alloc := &opencost.Allocation{
			Name:    "micro-namespace",
			Start:   s,
			End:     e,
			CPUCost: cost,
		}
		as := opencost.NewAllocationSet(s, e, alloc)
		allocs = append(allocs, as)
	}
	asr := opencost.NewAllocationSetRange(allocs...)

	reports, err := Detect(asr, step, lookback, "mad", 3.5, 0.10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(reports) != 0 {
		t.Errorf("expected 0 reports due to minCost threshold, got %d", len(reports))
	}
}

func mathAbs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
