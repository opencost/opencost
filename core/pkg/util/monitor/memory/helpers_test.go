package memory

import (
	"math"
	"testing"
)

const epsilon = 1e-9

type set[T comparable] struct {
	m map[T]struct{}
}

func newSet[T comparable](values ...T) *set[T] {
	m := make(map[T]struct{})
	for _, v := range values {
		m[v] = struct{}{}
	}
	return &set[T]{
		m: m,
	}
}

func (s *set[T]) add(value T) {
	s.m[value] = struct{}{}
}

func (s *set[T]) has(value T) bool {
	_, hasValue := s.m[value]
	return hasValue
}

func (s *set[T]) remove(value T) {
	delete(s.m, value)
}

// -------------------------------------------------------------------------
//  exponentialMovingAverage tests
// -------------------------------------------------------------------------

func TestEMA_InitialState(t *testing.T) {
	ema := newExponentialMovingAverage(0.5)
	if ema.set {
		t.Error("expected ema.set to be false on creation")
	}
	if ema.Current() != 0.0 {
		t.Errorf("expected initial Current() = 0.0, got %f", ema.Current())
	}
}

func TestEMA_FirstUpdateSetsValue(t *testing.T) {
	ema := newExponentialMovingAverage(0.5)
	got := ema.Update(42.0)
	if got != 42.0 {
		t.Errorf("expected first Update() = 42.0, got %f", got)
	}
	if !ema.set {
		t.Error("expected ema.set to be true after first update")
	}
}

func TestEMA_SubsequentUpdates(t *testing.T) {
	// With smoothing=0.5: EMA(n) = 0.5*sample + 0.5*EMA(n-1)
	ema := newExponentialMovingAverage(0.5)
	ema.Update(10.0)        // value = 10
	got := ema.Update(20.0) // value = 0.5*20 + 0.5*10 = 15
	want := 15.0
	if math.Abs(got-want) > epsilon {
		t.Errorf("expected %f, got %f", want, got)
	}
}

func TestEMA_SmoothingZero(t *testing.T) {
	// smoothing=0 means the value never changes after the first sample
	ema := newExponentialMovingAverage(0.0)
	ema.Update(5.0)
	ema.Update(100.0)
	ema.Update(999.0)
	if ema.Current() != 5.0 {
		t.Errorf("expected Current() = 5.0, got %f", ema.Current())
	}
}

func TestEMA_SmoothingOne(t *testing.T) {
	// smoothing=1 means the value is always the latest sample
	ema := newExponentialMovingAverage(1.0)
	ema.Update(5.0)
	ema.Update(99.0)
	if ema.Current() != 99.0 {
		t.Errorf("expected Current() = 99.0, got %f", ema.Current())
	}
}

func TestEMA_Reset(t *testing.T) {
	ema := newExponentialMovingAverage(0.5)
	ema.Update(10.0)
	ema.Reset()
	if ema.set {
		t.Error("expected ema.set to be false after Reset()")
	}
	if ema.Current() != 0.0 {
		t.Errorf("expected Current() = 0.0 after Reset(), got %f", ema.Current())
	}
	// First update after reset should treat as a fresh start
	got := ema.Update(7.0)
	if got != 7.0 {
		t.Errorf("expected first Update() after Reset() = 7.0, got %f", got)
	}
}

func TestEMA_MultipleUpdates(t *testing.T) {
	smoothing := 0.3
	ema := newExponentialMovingAverage(smoothing)

	samples := []float64{10, 20, 30, 40, 50}
	want := samples[0]
	for i, s := range samples {
		got := ema.Update(s)
		if i == 0 {
			want = s
		} else {
			want = smoothing*s + (1-smoothing)*want
		}
		if math.Abs(got-want) > epsilon {
			t.Errorf("step %d: expected %f, got %f", i, want, got)
		}
	}
}

// -------------------------------------------------------------------------
//  rollingWindow tests
// -------------------------------------------------------------------------

func TestRollingWindow_NewPanicsOnBadCapacity(t *testing.T) {
	cases := []int{0, -1, math.MaxInt}
	for _, cap := range cases {
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("expected panic for capacity %d", cap)
				}
			}()
			newRollingWindow(cap)
		}()
	}
}

func TestRollingWindow_InitialLen(t *testing.T) {
	rw := newRollingWindow(5)
	if rw.Len() != 0 {
		t.Errorf("expected Len() = 0, got %d", rw.Len())
	}
	if rw.Cap() != 5 {
		t.Errorf("expected Cap() = 5, got %d", rw.Cap())
	}
}

func TestRollingWindow_LenGrowsUpToCapacity(t *testing.T) {
	rw := newRollingWindow(3)
	rw.Push(1)
	if rw.Len() != 1 {
		t.Errorf("expected Len()=1, got %d", rw.Len())
	}
	rw.Push(2)
	if rw.Len() != 2 {
		t.Errorf("expected Len()=2, got %d", rw.Len())
	}
	rw.Push(3)
	if rw.Len() != 3 {
		t.Errorf("expected Len()=3, got %d", rw.Len())
	}
	// Pushing beyond capacity should not grow Len() past Cap()
	rw.Push(4)
	if rw.Len() != 3 {
		t.Errorf("expected Len()=3 after overflow push, got %d", rw.Len())
	}
}

func TestRollingWindow_Clear(t *testing.T) {
	rw := newRollingWindow(4)
	rw.Push(1)
	rw.Push(2)
	rw.Clear()
	if rw.Len() != 0 {
		t.Errorf("expected Len()=0 after Clear(), got %d", rw.Len())
	}
}

func TestRollingWindow_Mean_Empty(t *testing.T) {
	rw := newRollingWindow(4)
	if rw.Mean() != 0.0 {
		t.Errorf("expected Mean()=0 for empty window, got %f", rw.Mean())
	}
}

func TestRollingWindow_Mean_SingleValue(t *testing.T) {
	rw := newRollingWindow(4)
	rw.Push(7.0)
	if rw.Mean() != 7.0 {
		t.Errorf("expected Mean()=7.0, got %f", rw.Mean())
	}
}

func TestRollingWindow_Mean_MultipleValues(t *testing.T) {
	rw := newRollingWindow(5)
	for _, v := range []float64{1, 2, 3, 4, 5} {
		rw.Push(v)
	}
	want := 3.0
	if math.Abs(rw.Mean()-want) > epsilon {
		t.Errorf("expected Mean()=%f, got %f", want, rw.Mean())
	}
}

func TestRollingWindow_MeanStdDev_SingleValue(t *testing.T) {
	rw := newRollingWindow(4)
	rw.Push(10.0)
	mean, stddev := rw.MeanStdDev()
	if mean != 10.0 {
		t.Errorf("expected mean=10.0, got %f", mean)
	}
	if stddev != 0.0 {
		t.Errorf("expected stddev=0.0 for single value, got %f", stddev)
	}
}

func TestRollingWindow_MeanStdDev_KnownValues(t *testing.T) {
	rw := newRollingWindow(5)
	for _, v := range []float64{2, 4, 4, 4, 5, 5, 7, 9} {
		rw.Push(v)
	}
	// Window holds only the last 5: [5, 5, 7, 9, 9] — wait, cap=5.
	// Pushes: index 0=2,1=4,2=4,3=4,4=5 -> wraps: index 0=5,1=7,2=9
	// Use a simpler known case instead.
	rw2 := newRollingWindow(4)
	for _, v := range []float64{10, 20, 30, 40} {
		rw2.Push(v)
	}
	mean, stddev := rw2.MeanStdDev()
	wantMean := 25.0
	// sample stddev of {10,20,30,40} = sqrt(((−15)²+(−5)²+(5)²+(15)²)/3) = sqrt(500/3)
	wantStddev := math.Sqrt(500.0 / 3.0)
	if math.Abs(mean-wantMean) > epsilon {
		t.Errorf("expected mean=%f, got %f", wantMean, mean)
	}
	if math.Abs(stddev-wantStddev) > epsilon {
		t.Errorf("expected stddev=%f, got %f", wantStddev, stddev)
	}
}

func TestRollingWindow_Percentile_Empty(t *testing.T) {
	rw := newRollingWindow(4)
	if rw.Percentile(50) != 0.0 {
		t.Errorf("expected 0.0 for empty window percentile, got %f", rw.Percentile(50))
	}
}

func TestRollingWindow_Percentile_BoundaryValues(t *testing.T) {
	rw := newRollingWindow(5)
	for _, v := range []float64{3, 1, 4, 1, 5} {
		rw.Push(v)
	}
	if rw.Percentile(0) != 1.0 {
		t.Errorf("expected p0=1.0, got %f", rw.Percentile(0))
	}
	if rw.Percentile(100) != 5.0 {
		t.Errorf("expected p100=5.0, got %f", rw.Percentile(100))
	}
}

func TestRollingWindow_Percentile_Median(t *testing.T) {
	rw := newRollingWindow(5)
	for _, v := range []float64{1, 2, 3, 4, 5} {
		rw.Push(v)
	}
	got := rw.Percentile(50)
	want := 3.0
	if math.Abs(got-want) > epsilon {
		t.Errorf("expected p50=%f, got %f", want, got)
	}
}

func TestRollingWindow_Percentile_Interpolation(t *testing.T) {
	rw := newRollingWindow(4)
	for _, v := range []float64{0, 10, 20, 30} {
		rw.Push(v)
	}
	// rank = 0.25 * 3 = 0.75, lo=0(val=0), hi=1(val=10), frac=0.75 => 0*0.25 + 10*0.75 = 7.5
	got := rw.Percentile(25)
	want := 7.5
	if math.Abs(got-want) > epsilon {
		t.Errorf("expected p25=%f, got %f", want, got)
	}
}

func TestRollingWindow_IsConfidenceSatisfied(t *testing.T) {
	rw := newRollingWindow(100)
	// All the same value — stddev=0, margin=0, should always be satisfied
	for range 100 {
		rw.Push(50.0)
	}
	if !rw.IsConfidenceSatisfied(1.96, 0.05) {
		t.Error("expected confidence satisfied for zero-variance data")
	}
}

func TestRollingWindow_IsConfidenceSatisfied_HighVariance(t *testing.T) {
	rw := newRollingWindow(10)
	// High variance: alternating 1 and 1000
	for i := range 10 {
		if i%2 == 0 {
			rw.Push(1.0)
		} else {
			rw.Push(1000.0)
		}
	}
	// With high variance and small n, a tight margin should not be satisfied
	if rw.IsConfidenceSatisfied(1.96, 0.001) {
		t.Error("expected confidence NOT satisfied for high-variance data with tight margin")
	}
}

func TestRollingWindow_Each(t *testing.T) {
	rw := newRollingWindow(4)
	for _, v := range []float64{1, 2, 3, 4} {
		rw.Push(v)
	}
	sum := 0.0
	rw.Each(func(v float64) { sum += v })
	if math.Abs(sum-10.0) > epsilon {
		t.Errorf("expected Each() sum=10.0, got %f", sum)
	}
}

func TestRollingWindow_OverwritesOldestOnOverflow(t *testing.T) {
	rw := newRollingWindow(3)
	rw.Push(1)
	rw.Push(2)
	rw.Push(3)
	rw.Push(100) // Should evict 1, window = [2, 3, 100]
	mean := rw.Mean()
	want := (2.0 + 3.0 + 100.0) / 3.0
	if math.Abs(mean-want) > epsilon {
		t.Errorf("expected mean=%f after overflow, got %f", want, mean)
	}
}

func assertIndexLengthCap(t *testing.T, rw *rollingWindow, index int, length int, cap int) {
	t.Helper()

	if rw.index != index {
		t.Errorf("RollingWindow Index: %d. Expected %d", rw.index, index)
	}
	if rw.Len() != length {
		t.Errorf("RollingWindow Length: %d. Expected %d", rw.Len(), length)
	}
	if rw.Cap() != cap {
		t.Errorf("RollingWindow Capacity: %d. Expected %d", rw.Cap(), cap)
	}
}

func TestRollingWindow_BasicIndexingLengthCap(t *testing.T) {
	capacity := 3
	rw := newRollingWindow(capacity)

	assertIndexLengthCap(t, rw, 0, 0, 3)
	rw.Push(1)
	assertIndexLengthCap(t, rw, 1, 1, 3)
	rw.Push(2)
	assertIndexLengthCap(t, rw, 2, 2, 3)
	rw.Push(3)
	assertIndexLengthCap(t, rw, 0, 3, 3)

	rw.Push(1)
	assertIndexLengthCap(t, rw, 1, 3, 3)
	rw.Push(2)
	assertIndexLengthCap(t, rw, 2, 3, 3)
	rw.Push(3)
	assertIndexLengthCap(t, rw, 0, 3, 3)

	set := newSet(1, 2, 3)

	rw.Each(func(value float64) {
		v := int(value)
		if !set.has(v) {
			t.Errorf("Failed to find value: %d in set.\n", v)
		}

		set.remove(v)
	})

	// rewrite
	rw.Push(4)
	rw.Push(5)
	rw.Push(6)

	set = newSet(4, 5, 6)
	rw.Each(func(value float64) {
		v := int(value)
		if !set.has(v) {
			t.Errorf("Failed to find value: %d in set.\n", v)
		}

		set.remove(v)
	})
}

func TestRollingWindow_PartialCapacityMean(t *testing.T) {
	rw := newRollingWindow(10)
	for range 5 {
		rw.Push(5.0)
	}

	mean := rw.Mean()
	if mean != 5.0 {
		t.Errorf("Expected mean = 5.0. Got %f\n", mean)
	}
}
