package aggregator

import (
	"sync"
	"time"
)

// maxOverTimeAggregator is a MetricAggregator which returns the max value passed to it through the Update function
type maxOverTimeAggregator struct {
	lock        sync.Mutex
	name        string
	labelValues []string
	max         float64
}

func MaxOverTime(name string, labelValues []string) MetricAggregator {
	return &maxOverTimeAggregator{
		name:        name,
		labelValues: labelValues,
	}
}

func (a *maxOverTimeAggregator) Name() string {
	return a.name
}

func (a *maxOverTimeAggregator) AdditionInfo() map[string]string {
	return nil
}

func (a *maxOverTimeAggregator) LabelValues() []string {
	return a.labelValues
}

func (a *maxOverTimeAggregator) Update(value float64, timestamp time.Time, additionalInfo map[string]string) {
	a.lock.Lock()
	defer a.lock.Unlock()
	if value > a.max {
		a.max = value
	}
}

func (a *maxOverTimeAggregator) Value() []MetricValue {
	a.lock.Lock()
	defer a.lock.Unlock()
	return []MetricValue{
		{Value: a.max},
	}
}
