package aggregator

import (
	"time"
)

type ActiveMinutesAggregator struct {
	name        string
	labelValues []string
	start       *time.Time
	end         *time.Time
}

func ActiveMinutes(name string, labelValues []string) MetricAggregator {
	return &ActiveMinutesAggregator{
		name:        name,
		labelValues: labelValues,
	}
}

func (m *ActiveMinutesAggregator) Name() string {
	return m.name
}

func (m *ActiveMinutesAggregator) AdditionInfo() map[string]string {
	return nil
}

func (m *ActiveMinutesAggregator) LabelValues() []string {
	return m.labelValues
}

func (m *ActiveMinutesAggregator) Update(value float64, timestamp *time.Time, additionalInfo map[string]string) {
	if timestamp == nil {
		return
	}
	if m.start == nil {
		m.start = timestamp
	}
	m.end = timestamp
}

func (m *ActiveMinutesAggregator) Value() []MetricValue {
	return []MetricValue{
		{Value: 1, Timestamp: m.start},
		{Value: 1, Timestamp: m.end},
	}
}
