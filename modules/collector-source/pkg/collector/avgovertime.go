package collector

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

func (m *AverageOverTimeAggregator) LabelValues() []string {
	return m.labelValues
}

func (m *AverageOverTimeAggregator) Update(value float64) {
	m.total += value
	m.count++
}

func (m *AverageOverTimeAggregator) Value() float64 {
	return m.total / float64(m.count)
}
