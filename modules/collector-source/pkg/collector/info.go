package collector

// InfoAggregator is metric aggregator meant to just record label values
type InfoAggregator struct {
	name        string
	labelValues []string
}

func Info(name string, labelValues []string) MetricAggregator {
	return &InfoAggregator{
		name:        name,
		labelValues: labelValues,
	}
}

func (m *InfoAggregator) Name() string {
	return m.name
}

func (m *InfoAggregator) LabelValues() []string {
	return m.labelValues
}

func (m *InfoAggregator) Update(value float64) {

}

func (m *InfoAggregator) Value() float64 {
	return 1
}
