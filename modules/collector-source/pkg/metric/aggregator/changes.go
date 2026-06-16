package aggregator

import (
	"sync"
	"time"
)

// changesAggregator is a MetricAggregator which counts how many times the
// sample value changed between consecutive samples, equivalent to
// PromQL's changes(). Updates must arrive in timestamp order; out-of-order
// or duplicate timestamps are ignored.
type changesAggregator struct {
	lock        sync.Mutex
	labelValues []string
	initialized bool
	lastValue   float64
	lastTime    time.Time
	changes     float64
}

func Changes(labelValues []string) MetricAggregator {
	return &changesAggregator{
		labelValues: labelValues,
	}
}

func (a *changesAggregator) AdditionInfo() map[string]string {
	return nil
}

func (a *changesAggregator) LabelValues() []string {
	return a.labelValues
}

func (a *changesAggregator) Update(value float64, timestamp time.Time, additionalInfo map[string]string) {
	a.lock.Lock()
	defer a.lock.Unlock()

	if a.initialized && !timestamp.After(a.lastTime) {
		return
	}
	if a.initialized && value != a.lastValue {
		a.changes++
	}
	a.initialized = true
	a.lastValue = value
	a.lastTime = timestamp
}

func (a *changesAggregator) Value() []MetricValue {
	a.lock.Lock()
	defer a.lock.Unlock()
	return []MetricValue{
		{Value: a.changes},
	}
}
