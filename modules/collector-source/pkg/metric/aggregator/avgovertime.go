package aggregator

import (
	"sync"
	"time"
)

// averageOverTimeAggregator is a MetricAggregator which returns the average of values it is aggregating by dividing the
// total of all values by the count of unique timestamps
type averageOverTimeAggregator struct {
	lock        sync.Mutex
	name        string
	labelValues []string
	total       float64
	count       int
	currentTime *time.Time
}

func AverageOverTime(name string, labelValues []string) MetricAggregator {
	return &averageOverTimeAggregator{
		name:        name,
		labelValues: labelValues,
	}
}

func (a *averageOverTimeAggregator) Name() string {
	return a.name
}

func (a *averageOverTimeAggregator) AdditionInfo() map[string]string {
	return nil
}

func (a *averageOverTimeAggregator) LabelValues() []string {
	return a.labelValues
}

func (a *averageOverTimeAggregator) Update(value float64, timestamp time.Time, additionalInfo map[string]string) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.total += value
	if a.currentTime == nil || !timestamp.Equal(*a.currentTime) {
		a.currentTime = &timestamp
		a.count++
	}
}

func (a *averageOverTimeAggregator) Value() []MetricValue {
	a.lock.Lock()
	defer a.lock.Unlock()
	if a.count == 0 {
		return []MetricValue{
			{
				Value: 0,
			},
		}
	}
	return []MetricValue{
		{a.total / float64(a.count), nil},
	}
}
