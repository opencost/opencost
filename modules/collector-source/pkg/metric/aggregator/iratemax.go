package aggregator

import (
	"sync"
	"time"
)

// iRateMaxAggregator is a MetricAggregator which returns the max rate per second between any two samples.
// to function properly calls to Update must have a timestamp greater than or equal to the last call to update.
type iRateMaxAggregator struct {
	lock         sync.Mutex
	name         string
	labelValues  []string
	initialized  bool
	previousTime time.Time
	currentTime  time.Time
	previous     float64
	current      float64
	max          float64
}

func IRateMax(labelValues []string) MetricAggregator {
	return &iRateMaxAggregator{
		labelValues: labelValues,
	}
}

func (a *iRateMaxAggregator) AdditionInfo() map[string]string {
	return nil
}

func (a *iRateMaxAggregator) LabelValues() []string {
	return a.labelValues
}

func (a *iRateMaxAggregator) Update(value float64, timestamp time.Time, additionalInfo map[string]string) {
	a.lock.Lock()
	defer a.lock.Unlock()
	if !a.initialized {
		a.previousTime = timestamp
		a.currentTime = timestamp
		a.initialized = true
	}

	if a.currentTime.Before(timestamp) {
		a.previousTime = a.currentTime
		a.previous = a.current
		a.currentTime = timestamp
		a.current = 0
	}
	a.current += value

	seconds := a.currentTime.Sub(a.previousTime).Seconds()
	if seconds == 0 {
		return
	}
	increase := a.current - a.previous
	irate := increase / seconds
	if irate > a.max {
		a.max = irate
	}
}

func (a *iRateMaxAggregator) Value() []MetricValue {
	a.lock.Lock()
	defer a.lock.Unlock()
	return []MetricValue{
		{Value: a.max},
	}
}
