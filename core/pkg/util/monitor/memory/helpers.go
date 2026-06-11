package memory

import (
	"fmt"
	"math"
	"slices"
)

//--------------------------------------------------------------------------
//  Helper Types
//--------------------------------------------------------------------------

// trackedValue maintains the state of an uninitialized value, a set value, and
// the previous value. The previous value is always used when the current value
// is unset. All values that are set become previous values when the current value
// changes, or Reset() is called.
type trackedValue struct {
	current  *float64
	previous float64
}

// newTrackedValue returns a new trackedValue instance for tracking unset, set, and previous
// values for a float64.
func newTrackedValue() *trackedValue {
	return new(trackedValue)
}

// Value returns the current value if it has not been reset. Otherwise, it returns
// the previous value
func (tv *trackedValue) Value() float64 {
	if tv.current == nil {
		return tv.previous
	}

	return *tv.current
}

// IsSet returns `true` if the current value has been set.
func (tv *trackedValue) IsSet() bool {
	return tv.current != nil
}

// Set updates the current value if it is different. If the value is updated, `true` is returned.
// Otherwise, `false` is returned.
func (tv *trackedValue) Set(value float64) bool {
	if tv.current == nil {
		tv.current = &value
		return true
	}

	curr := *tv.current
	if value != curr {
		tv.current = &value
		tv.previous = curr
		return true
	}

	return false
}

// Reset resets the current value to unset, moving it to the previous value if set.
func (tv *trackedValue) Reset() {
	if tv.current == nil {
		return
	}

	tv.previous = *tv.current
	tv.current = nil
}

// Clear resets the value and sets the previous to 0.
func (tv *trackedValue) Clear() {
	tv.current = nil
	tv.previous = 0.0
}

// The Cumulative Sum (CUSUM) is a statistical process control tool that plots
// the cumulative sums of deviations from a target mean to detect small, persistent
// shifts (0.5 to 2 sigma) in process performance quickly.
type cumulativeSum struct {
	slack float64
	sum   float64
	base  float64
}

// newCumulativeSum creates a new cumulativeSum instance with the provided slack
func newCumulativeSum(slack float64) *cumulativeSum {
	return &cumulativeSum{
		slack: slack,
		sum:   0.0,
		base:  0.0,
	}
}

// Calibrate initializes the baseline for the CUSUM. This is generally the mean
// of the samples once there are enough to consider the sample set as "stable."
func (cs *cumulativeSum) Calibrate(mean float64) {
	if cs.base != 0.0 {
		return
	}

	cs.base = mean
}

// Update supplies a new sample to update the internal sum.
func (cs *cumulativeSum) Update(value float64) {
	if cs.base == 0.0 {
		return
	}

	slack := cs.base * cs.slack
	cs.sum = max(0, cs.sum+(value-cs.base)-slack)
}

// Sum returns the current CUSUM value.
func (cs *cumulativeSum) Sum() float64 {
	return cs.sum
}

// IsRecalibrationRequired tests the current CUSUM against the base * thresholdMagnitude.
// If it has surpassed the magnitude provided, true is returned signalling a recalibration
// should be performed.
func (cs *cumulativeSum) IsRecalibrationRequired(thresholdMagnitude float64) bool {
	if cs.base == 0.0 {
		return false
	}

	threshold := cs.base * thresholdMagnitude
	//fmt.Printf("Testing: %f > %f = %t\n", cs.sum, threshold, cs.sum > threshold)
	return cs.sum > threshold
}

func (cs *cumulativeSum) Reset() {
	cs.base = 0.0
	cs.sum = 0.0
}

// exponentialMovingAverage is a helper type that tracks the current moving average
// value using a providing smoothing factor.
type exponentialMovingAverage struct {
	smoothing float64
	value     float64
	set       bool
}

// creates a new exponential moving average instance using the provided smoothing factor
func newExponentialMovingAverage(smoothing float64) *exponentialMovingAverage {
	return &exponentialMovingAverage{
		smoothing: smoothing,
		set:       false,
	}
}

// updates the moving average for the provided sample, and returns the updated
// value
func (ema *exponentialMovingAverage) Update(sample float64) float64 {
	if !ema.set {
		ema.set = true
		ema.value = sample
	} else {
		ema.value = ema.smoothing*sample + (1.0-ema.smoothing)*ema.value
	}
	return ema.value
}

// The current moving average value
func (ema *exponentialMovingAverage) Current() float64 {
	return ema.value
}

// Resets the moving average calculation
func (ema *exponentialMovingAverage) Reset() {
	ema.set = false
	ema.value = 0.0
}

// rollingWindow is a ring buffer helper type for tracking a set capacity number of
// the most recent values. It also provides helper methods for calculating mean,
// stddev, and percentiles of the contained data.
type rollingWindow struct {
	capacity int
	length   int
	window   []float64
	index    int
}

// creates a new rolling window instance with the provided static capacity.
func newRollingWindow(capacity int) *rollingWindow {
	if capacity <= 0 || capacity > (math.MaxInt/2) {
		panic(fmt.Sprintf("RollingWindow capacity limited to range 1-%d", math.MaxInt/2))
	}

	return &rollingWindow{
		capacity: capacity,
		window:   make([]float64, capacity),
		index:    0,
	}
}

// Pushes a new value into the rolling window, dropping the oldest value if
// the total length surpasses the capacity.
func (rw *rollingWindow) Push(value float64) {
	// advance index, handle overflow
	index := rw.index % rw.capacity
	rw.window[index] = value

	rw.index = (rw.index + 1) % rw.capacity
	rw.length = min(rw.length+1, rw.capacity)
}

// Clears the rolling window values
func (rw *rollingWindow) Clear() {
	rw.window = make([]float64, rw.capacity)
	rw.index = 0
	rw.length = 0
}

// The length of the rolling window. Will never be greater that the `Cap()`.
func (rw *rollingWindow) Len() int {
	return rw.length
}

// Cap returns the maximum capacity of the rolling window.
func (rw *rollingWindow) Cap() int {
	return rw.capacity
}

// Each iterates all values within the rolling window and calls `f` passing each value.
// NOTE: Ordering is _not_ guaranteed!
func (rw *rollingWindow) Each(f func(float64)) {
	total := rw.Len()
	for i := range total {
		f(rw.window[i])
	}
}

// Mean returns the average of the values in the window
func (rw *rollingWindow) Mean() float64 {
	length := rw.Len()
	if length == 0 {
		return 0.0
	}

	sum := 0.0
	for i := range length {
		sum += rw.window[i]
	}
	return sum / float64(length)
}

// MeanStdDev computes the mean and standard deviation of the window values.
func (rw *rollingWindow) MeanStdDev() (mean float64, stddev float64) {
	mean = rw.Mean()

	length := rw.Len()
	if length < 2 {
		return mean, 0
	}

	variance := 0.0
	for i := range length {
		d := rw.window[i] - mean
		variance += d * d
	}

	// sample variance (Bessel's correction)
	variance /= float64(length - 1)
	stddev = math.Sqrt(variance)
	return
}

// Percentile computes the p-th percentile of the values currently stored in the
// rolling window.
func (rw *rollingWindow) Percentile(p float64) float64 {
	length := rw.Len()
	if length == 0 {
		return 0
	}

	sorted := make([]float64, length)
	for i := range length {
		sorted[i] = rw.window[i]
	}
	slices.Sort(sorted)

	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}

	rank := (p / 100.0) * float64(len(sorted)-1)
	lo := int(math.Floor(rank))
	hi := int(math.Ceil(rank))
	frac := rank - float64(lo)

	return sorted[lo]*(1-frac) + sorted[hi]*frac
}

// IsConfidenceSatisfied checks the relative margin of error is within the provided
// `marginPercent` threshold using the provided z-score.
func (rw *rollingWindow) IsConfidenceSatisfied(z float64, marginPercent float64) bool {
	mean, stddev := rw.MeanStdDev()
	length := float64(rw.Len())
	marginOfError := z * (stddev / math.Sqrt(length))
	relative := marginOfError / mean

	return relative <= marginPercent
}
