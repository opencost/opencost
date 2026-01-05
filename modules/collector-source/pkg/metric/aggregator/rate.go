package aggregator

import (
	"sync"
	"time"
)

// rateAggregator is a MetricAggregator which returns the average rate per second change of the samples that it tracks.
// to function properly calls to Update must have a timestamp greater than or equal to the last call to update.
type rateAggregator struct {
	lock         sync.Mutex
	labelValues  []string
	previousTime time.Time
	previous     float64
	currentTime  time.Time
	current      float64
	runningAvg   float64
	seconds      float64
}

func Rate(labelValues []string) MetricAggregator {
	return &rateAggregator{
		labelValues: labelValues,
	}
}

// getRunningAvgSeconds returns the running average without updating the state
func (a *rateAggregator) getRunningAvgSeconds() (float64, float64) {
	runningAvg := a.runningAvg
	seconds := a.seconds
	// ignore decreases and base case where only one sample has been recorded
	if a.previous < a.current && !a.previousTime.IsZero() {
		currentSeconds := a.currentTime.Sub(a.previousTime).Seconds()
		// ratio used to add the rate since the last recorded timestamp into the running average
		weightingRatio := currentSeconds / (currentSeconds + seconds)
		currentRate := (a.current - a.previous) / currentSeconds
		runningAvg = (runningAvg * (1 - weightingRatio)) + (currentRate * weightingRatio)
		seconds += currentSeconds
	}
	return runningAvg, seconds
}

func (a *rateAggregator) AdditionInfo() map[string]string {
	return nil
}

func (a *rateAggregator) LabelValues() []string {
	return a.labelValues
}

func (a *rateAggregator) Update(value float64, timestamp time.Time, additionalInfo map[string]string) {
	a.lock.Lock()
	defer a.lock.Unlock()
	// If samples from a new timestamp finalize current values by moving them to previous
	if timestamp.After(a.currentTime) {
		// update state and reset current
		a.runningAvg, a.seconds = a.getRunningAvgSeconds()
		a.previous = a.current
		a.previousTime = a.currentTime
		a.currentTime = timestamp
		a.current = 0
	}
	a.current += value
}

func (a *rateAggregator) Value() []MetricValue {
	a.lock.Lock()
	defer a.lock.Unlock()
	average, seconds := a.getRunningAvgSeconds()
	if seconds == 0 {
		return []MetricValue{
			{Value: 0},
		}
	}
	return []MetricValue{
		{Value: average},
	}
}
