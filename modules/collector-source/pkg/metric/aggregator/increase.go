package aggregator

import (
	"sync"
	"time"
)

type increaseAggregator struct {
	lock        sync.Mutex
	labelValues []string
	currentTime time.Time
	previous    float64
	current     float64
	increase    float64
}

func Increase(labelValues []string) MetricAggregator {
	return &increaseAggregator{
		labelValues: labelValues,
	}
}

func (a *increaseAggregator) getIncrease() float64 {
	increase := a.increase
	// ignore decreases
	if a.previous < a.current && a.previous != 0 {
		increase += a.current - a.previous
	}
	return increase
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
	if timestamp.After(a.currentTime) {
		a.currentTime = timestamp
		a.increase = a.getIncrease()
		a.previous = a.current
		a.current = 0
	}
	a.current += value
}

func (a *increaseAggregator) Value() []MetricValue {
	a.lock.Lock()
	defer a.lock.Unlock()
	return []MetricValue{
		{Value: a.getIncrease()},
	}
}
