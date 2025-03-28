package collector

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

func (m *ActiveMinutesAggregator) LabelValues() []string {
	return m.labelValues
}

func (m *ActiveMinutesAggregator) Update(value float64) {
	now := time.Now().UTC()
	if m.start == nil {
		m.start = &now
	}

	m.end = &now
}

func (m *ActiveMinutesAggregator) Value() float64 {
	if m.start == nil || m.end == nil {
		return 0.0
	}

	return m.end.Sub(*m.start).Minutes()
}
