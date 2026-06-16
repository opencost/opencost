package aggregator

import (
	"math"
	"sync"
	"time"
)

// bitSetRatioAggregator is a MetricAggregator which returns the fraction of
// unique-timestamp samples in which a specific bit was set in the sample
// value, treating the value as an integer bitmask. It is used to derive the
// fraction of a window a GPU spent throttled for a specific reason from the
// DCGM clock throttle reasons bitmask.
type bitSetRatioAggregator struct {
	lock        sync.Mutex
	labelValues []string
	bit         uint64
	count       int
	setCount    int
	currentTime *time.Time
	currentSet  bool
}

// BitSetRatio returns a MetricAggregatorFactory producing aggregators that
// report the fraction of samples with the given bit set.
func BitSetRatio(bit uint64) MetricAggregatorFactory {
	return func(labelValues []string) MetricAggregator {
		return &bitSetRatioAggregator{
			labelValues: labelValues,
			bit:         bit,
		}
	}
}

func (a *bitSetRatioAggregator) AdditionInfo() map[string]string {
	return nil
}

func (a *bitSetRatioAggregator) LabelValues() []string {
	return a.labelValues
}

func (a *bitSetRatioAggregator) Update(value float64, timestamp time.Time, additionalInfo map[string]string) {
	a.lock.Lock()
	defer a.lock.Unlock()

	// NaN or negative values cannot be valid bitmasks; count the sample as
	// bit-clear rather than guessing
	set := !math.IsNaN(value) && value >= 0 && uint64(value)&a.bit != 0

	if a.currentTime == nil || !timestamp.Equal(*a.currentTime) {
		a.currentTime = &timestamp
		a.currentSet = false
		a.count++
	}
	if set && !a.currentSet {
		a.currentSet = true
		a.setCount++
	}
}

func (a *bitSetRatioAggregator) Value() []MetricValue {
	a.lock.Lock()
	defer a.lock.Unlock()
	if a.count == 0 {
		return []MetricValue{{Value: 0}}
	}
	return []MetricValue{
		{Value: float64(a.setCount) / float64(a.count)},
	}
}
