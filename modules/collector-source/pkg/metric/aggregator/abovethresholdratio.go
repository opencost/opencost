package aggregator

import (
	"math"
	"sync"
	"time"
)

// aboveThresholdRatioAggregator is a MetricAggregator which returns the
// fraction of unique-timestamp samples whose value was at or above a fixed
// threshold. It is used to derive time-over-threshold pressure signals such
// as GPU memory pressure.
type aboveThresholdRatioAggregator struct {
	lock        sync.Mutex
	labelValues []string
	threshold   float64
	count       int
	aboveCount  int
	currentTime *time.Time
	currentHit  bool
}

// AboveThresholdRatio returns a MetricAggregatorFactory producing
// aggregators that report the fraction of samples >= threshold.
func AboveThresholdRatio(threshold float64) MetricAggregatorFactory {
	return func(labelValues []string) MetricAggregator {
		return &aboveThresholdRatioAggregator{
			labelValues: labelValues,
			threshold:   threshold,
		}
	}
}

func (a *aboveThresholdRatioAggregator) AdditionInfo() map[string]string {
	return nil
}

func (a *aboveThresholdRatioAggregator) LabelValues() []string {
	return a.labelValues
}

func (a *aboveThresholdRatioAggregator) Update(value float64, timestamp time.Time, additionalInfo map[string]string) {
	a.lock.Lock()
	defer a.lock.Unlock()

	hit := !math.IsNaN(value) && value >= a.threshold

	if a.currentTime == nil || !timestamp.Equal(*a.currentTime) {
		a.currentTime = &timestamp
		a.currentHit = false
		a.count++
	}
	if hit && !a.currentHit {
		a.currentHit = true
		a.aboveCount++
	}
}

func (a *aboveThresholdRatioAggregator) Value() []MetricValue {
	a.lock.Lock()
	defer a.lock.Unlock()
	if a.count == 0 {
		return []MetricValue{{Value: 0}}
	}
	return []MetricValue{
		{Value: float64(a.aboveCount) / float64(a.count)},
	}
}
