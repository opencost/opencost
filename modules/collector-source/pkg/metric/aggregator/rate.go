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
	increase     float64
	seconds      float64
}

func Rate(labelValues []string) MetricAggregator {
	return &rateAggregator{
		labelValues: labelValues,
	}
}
func (a *rateAggregator) getIncreaseSeconds() (float64, float64) {
	increase := a.increase
	seconds := a.seconds
	// ignore decreases
	if a.previous < a.current && a.previous != 0 {
		increase += a.current - a.previous
		seconds += a.currentTime.Sub(a.previousTime).Seconds()
	}
	return increase, seconds
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
	if timestamp.After(a.currentTime) {
		a.increase, a.seconds = a.getIncreaseSeconds()
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
	increase, seconds := a.getIncreaseSeconds()
	if seconds == 0 {
		return []MetricValue{
			{Value: 0},
		}
	}
	return []MetricValue{
		{Value: increase / seconds},
	}
}
