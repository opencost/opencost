package aggregator

import (
	"testing"
	"time"
)

func TestMetricValueToVector(t *testing.T) {

	t.Run("with timestamp", func(t *testing.T) {
		timestamp := time.Now()
		mv := &MetricValue{
			Value:     42.0,
			Timestamp: &timestamp,
		}

		vector := mv.ToVector()
		if vector.Value != 42.0 {
			t.Errorf("Expected value 42.0, got %f", vector.Value)
		}
		if vector.Timestamp != float64(timestamp.Unix()) {
			t.Errorf("Expected timestamp %f, got %f", float64(timestamp.Unix()), vector.Timestamp)
		}
	})

	t.Run("without timestamp", func(t *testing.T) {
		mv := &MetricValue{
			Value:     42.0,
			Timestamp: nil,
		}

		vector := mv.ToVector()
		if vector.Value != 42.0 {
			t.Errorf("Expected value 42.0, got %f", vector.Value)
		}
		if vector.Timestamp != 0.0 {
			t.Errorf("Expected timestamp 0.0, got %f", vector.Timestamp)
		}
	})
}
