package aggregator

import (
	"sync"
	"time"
)

// rateAggregator is a MetricAggregator which returns the average rate per second change of the samples that it tracks.
// to function properly calls to Update must have a timestamp greater than or equal to the last call to update.
type rateAggregator struct {
	lock        sync.Mutex
	name        string
	labelValues []string
	initialized bool
	initialTime time.Time
	currentTime time.Time
	initial     float64
	current     float64
}

func Rate(name string, labelValues []string) MetricAggregator {
	return &rateAggregator{
		name:        name,
		labelValues: labelValues,
	}
}

func (a *rateAggregator) Name() string {
	return a.name
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
	if !a.initialized {
		a.initialTime = timestamp
		a.currentTime = timestamp
		a.initialized = true
	}
	if a.initialTime == timestamp {
		a.initial += value
	}

	if a.currentTime.Before(timestamp) {
		a.currentTime = timestamp
		a.current = 0
	}

	a.current += value
}

func (a *rateAggregator) Value() []MetricValue {
	a.lock.Lock()
	defer a.lock.Unlock()
	seconds := a.currentTime.Sub(a.initialTime).Seconds()
	if seconds == 0 {
		return []MetricValue{
			{Value: 0},
		}
	}
	increase := a.current - a.initial
	return []MetricValue{
		{Value: increase / seconds},
	}
}
