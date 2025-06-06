package aggregator

import (
	"sync"
	"time"
)

// maxOverTimeAggregator is a MetricAggregator which returns the max value passed to it through the Update function
type maxOverTimeAggregator struct {
	lock        sync.Mutex
	labelValues []string
	max         float64
}

func MaxOverTime(labelValues []string) MetricAggregator {
	return &maxOverTimeAggregator{
		labelValues: labelValues,
	}
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
