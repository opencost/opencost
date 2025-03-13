package collector

type AverageOverTimeTransformer struct {
	name        string
	labelValues []string
	total       float64
	count       int
}

func AverageOverTime(name string, labelValues []string) MetricAggregator {
	return &AverageOverTimeTransformer{
		name:        name,
		labelValues: labelValues,
	}
}

func (m *AverageOverTimeTransformer) Name() string {
	return m.name
}

func (m *AverageOverTimeTransformer) LabelValues() []string {
	return m.labelValues
}

func (m *AverageOverTimeTransformer) Update(value float64) {
	m.total += value
	m.count++
}

func (m *AverageOverTimeTransformer) Value() float64 {
	return m.total / float64(m.count)
}
