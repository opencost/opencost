package aggregator

import (
	"sync"
	"time"
)

type increaseAggregator struct {
	lock        sync.Mutex
	name        string
	labelValues []string
	initialized bool
	initialTime time.Time
	currentTime time.Time
	initial     float64
	current     float64
}

func Increase(name string, labelValues []string) MetricAggregator {
	return &increaseAggregator{
		name:        name,
		labelValues: labelValues,
	}
}

func (a *increaseAggregator) Name() string {
	return a.name
}

func (a *increaseAggregator) AdditionInfo() map[string]string {
	return nil
}

func (a *increaseAggregator) LabelValues() []string {
	return a.labelValues
}

func (a *increaseAggregator) Update(value float64, timestamp time.Time, additionalInfo map[string]string) {
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

func (a *increaseAggregator) Value() []MetricValue {
	a.lock.Lock()
	defer a.lock.Unlock()
	return []MetricValue{
		{Value: a.current - a.initial},
	}
}
