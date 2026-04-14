package memory

import "sync"

// MemoryLimitConfig contains configuration values used to calculate the soft
// memory limit based on heap usage over time.
type MemoryLimitConfig struct {
	// LimitRatio is the ratio applied to memory limit values calculated. This
	// is generally set to 90% of the proposed limit. ie: 0.9
	LimitRatio float64

	// MinSamples is the required number of samples that must be collected before
	// calculating a memory limit.
	MinSamples int

	// WindowSize is the total number of smoothed samples to maintain when calculating
	// the proposed limit. This is generally set based on the timescale in which
	// samples are added.
	WindowSize int

	// SmoothingFactor is a value between 0 and 1 which defines how weight importance
	// should be distributed between a previous average value and the current observation.
	SmoothingFactor float64

	// BreachWindowSize is the total number of recent raw samples are maintained/used for
	// breach detection.
	BreachWindowSize int

	// BreachThreshold is a limit of raw samples, within the `BreachWindowSize`, allowed to
	// exceed the memory limit. If this threshold is reached, the samples are recalibrated.
	BreachThreshold int

	// CumulativeSumSlack is also known as the K-Factor (drift tolerance) in cumulative sum control
	// charts uses the allowable slack range in deviations. If the deviations exceed the allowable
	// slack, then they're used to calculate the sum. This is generally set from 0.5 to 1.0 standard
	// deviations to filter out process noise.
	CumulativeSumSlack float64

	// CumulativeSumThreshold is a scaler applied to the "baseline" mean (set once there are enough
	// samples to be considered "stable"). If the cumulative sum ever surpasses this baseline * threshold,
	// the samples will be recalibrated.
	CumulativeSumThreshold float64
}

// DefaultMemoryLimitConfig creates the recommended values to use for detecting soft memory limit updates
func DefaultMemoryLimitConfig() *MemoryLimitConfig {
	return &MemoryLimitConfig{
		LimitRatio:             0.90,
		MinSamples:             30,
		WindowSize:             60,
		SmoothingFactor:        0.30,
		BreachWindowSize:       10,
		BreachThreshold:        3,
		CumulativeSumSlack:     0.05,
		CumulativeSumThreshold: 5.0,
	}
}

// MemoryLimitStats is a run-time memory statistics collector that maintains a soft memory limit
// value based on configurable input parameters. It is designed to adjust the soft limit based on
// meaningful changes to overall heap allocation, leveraging exponential moving average windows,
// confidence interval gates, breach detection, and cumulative sum control chart to detect meaningful
// deviations from the mean.
type MemoryLimitStats struct {
	lock   sync.Mutex
	config *MemoryLimitConfig

	// expontential moving average calculation
	ema *exponentialMovingAverage

	// ring buffers for tracking exponential moving averages and raw samples
	window *rollingWindow
	raw    *rollingWindow
	breach *rollingWindow

	// cusum calculation for detecting positive shifts in memory usage
	cusum *cumulativeSum

	// tracked value storage for the soft memory limit proposal which
	// stores the previous limit as well as the current limit
	softLimit *trackedValue
}

// NewMemoryLimitStats creates a new `MemoryLimitStats` instance with the provided
// `MemoryLimitConfig`. If the provided config is `nil`, then the default configuration
// values are used.
func NewMemoryLimitStats(config *MemoryLimitConfig) *MemoryLimitStats {
	if config == nil {
		config = DefaultMemoryLimitConfig()
	}

	return &MemoryLimitStats{
		config:    config,
		ema:       newExponentialMovingAverage(config.SmoothingFactor),
		window:    newRollingWindow(config.WindowSize),
		raw:       newRollingWindow(config.MinSamples),
		breach:    newRollingWindow(config.BreachWindowSize),
		cusum:     newCumulativeSum(config.CumulativeSumSlack),
		softLimit: newTrackedValue(),
	}
}

// Record ingests the total heap memory usage (in bytes), and returns
// (newSoftLimit, true) when the soft limit has been updated, or
// (currentSoftLimit, false) when no change occurred.
//
// A return value of (0, false) means the monitor is still collecting samples
// and no limit has been committed yet.
func (mls *MemoryLimitStats) Record(heapBytes uint64) (softLimit uint64, updated bool) {
	mls.lock.Lock()
	defer mls.lock.Unlock()

	sample := float64(heapBytes)
	smoothed := mls.ema.Update(sample)

	mls.window.Push(smoothed)
	mls.raw.Push(sample)
	mls.breach.Push(sample)

	// Check that the minimum number of samples exist in the window before
	// calculating the memory limit
	totalSamples := mls.window.Len()
	if totalSamples < mls.config.MinSamples {
		return uint64(mls.softLimit.Value()), false
	}

	// NOTE: We could calculate the mean and stddev here, and determine if the data
	// NOTE: matches a confidence interval, but this might be too strict. See the
	// NOTE: method: mls.window.IsConfidenceSatisfied(...) method.

	// Pull the P99 sample from the smoothed sample window
	p99 := mls.window.Percentile(99)
	candidate := p99 * mls.config.LimitRatio

	// Ensure we've already set a soft limit before running breach
	// detection or CUSUM deviation tests.
	if mls.softLimit.IsSet() {
		if mls.isBreachDetected() {
			mls.recalibrate()
			return uint64(mls.softLimit.Value()), false
		}

		// update cumulative sum and check for recalibration
		mls.cusum.Update(sample)
		if mls.cusum.IsRecalibrationRequired(mls.config.CumulativeSumThreshold) {
			mls.recalibrate()
			return uint64(mls.softLimit.Value()), false
		}

		// this will only end up running once after the min samples threshold
		// is passed, and sets the baseline mean for the cusum calculations
		mean := mls.raw.Mean()
		mls.cusum.Calibrate(mean)
	}

	// update the soft limit to the candidate sample
	updated = mls.softLimit.Set(candidate)
	softLimit = uint64(mls.softLimit.Value())
	return
}

// SoftLimit returns the current soft limit without recording a sample.
// Returns 0 if the monitor is still collecting data samples.
func (mls *MemoryLimitStats) SoftMemoryLimit() uint64 {
	mls.lock.Lock()
	defer mls.lock.Unlock()

	return uint64(mls.softLimit.Value())
}

// TotalSamples returns the total number of samples _currently_ being used to
// calculate the memory limit. The samples will reset if a deviation threshold
// was reached in order to re-establish stability in the data set.
func (mls *MemoryLimitStats) TotalSamples() int {
	mls.lock.Lock()
	defer mls.lock.Unlock()

	return mls.window.Len()
}

// Reset clears all state and samples collected.
func (mls *MemoryLimitStats) Reset() {
	mls.lock.Lock()
	defer mls.lock.Unlock()

	mls.window.Clear()
	mls.raw.Clear()
	mls.breach.Clear()
	mls.softLimit.Clear()
	mls.ema.Reset()
	mls.cusum.Reset()
}

// isBreachDetected iterates through the breach sample window and tallies the
// total number of samples that exceed the p99 smoothed memory usage sample.
func (mls *MemoryLimitStats) isBreachDetected() bool {
	if !mls.softLimit.IsSet() {
		return false
	}

	// due to the nature of breach detection, we want to compare
	// against the smoothed p99 sample, so unroll the ratio
	p99 := mls.softLimit.Value() / mls.config.LimitRatio

	// Tally the total number of recent raw samples that are
	// greater than the p99 smoothed sample
	count := 0
	mls.breach.Each(func(value float64) {
		if value > p99 {
			count++
		}
	})

	return count >= mls.config.BreachThreshold
}

// recalibrate dumps the existing samples and calculations, but will preserve
// the previous softLimit value until a new soft limit is set.
func (mls *MemoryLimitStats) recalibrate() {
	mls.window.Clear()
	mls.raw.Clear()
	mls.breach.Clear()
	mls.ema.Reset()
	mls.cusum.Reset()
	mls.softLimit.Reset()
}
