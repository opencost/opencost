package aggregator

import (
	"time"
)

type AverageOverTimeAggregator struct {
	name        string
	labelValues []string
	total       float64
	count       int
}

func AverageOverTime(name string, labelValues []string) MetricAggregator {
	return &AverageOverTimeAggregator{
		name:        name,
		labelValues: labelValues,
	}
}

func (m *AverageOverTimeAggregator) Name() string {
	return m.name
}

func (m *AverageOverTimeAggregator) AdditionInfo() map[string]string {
	return nil
}

func (m *AverageOverTimeAggregator) LabelValues() []string {
	return m.labelValues
}

func (m *AverageOverTimeAggregator) Update(value float64, timestamp *time.Time, additionalInfo map[string]string) {
	m.total += value
	m.count++
}

func (m *AverageOverTimeAggregator) Value() []MetricValue {
	return []MetricValue{
		{m.total / float64(m.count), nil},
	}
}
