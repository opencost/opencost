package costmodel

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
)

func TestRecordNodePricingLookup(t *testing.T) {
	// Save original and restore after test
	original := nodePricingLookupTotal
	defer func() { nodePricingLookupTotal = original }()

	t.Run("nil counter does not panic", func(t *testing.T) {
		nodePricingLookupTotal = nil
		// Should not panic
		RecordNodePricingLookup("success")
		RecordNodePricingLookup("fallback")
	})

	t.Run("records success status", func(t *testing.T) {
		nodePricingLookupTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "test_node_pricing_lookup_total",
			Help: "Test metric",
		}, []string{"status"})

		RecordNodePricingLookup("success")
		RecordNodePricingLookup("success")

		metric := &dto.Metric{}
		err := nodePricingLookupTotal.WithLabelValues("success").(prometheus.Counter).Write(metric)
		assert.NoError(t, err)
		assert.Equal(t, float64(2), metric.GetCounter().GetValue())
	})

	t.Run("records fallback status", func(t *testing.T) {
		nodePricingLookupTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "test_node_pricing_lookup_total_2",
			Help: "Test metric",
		}, []string{"status"})

		RecordNodePricingLookup("fallback")

		metric := &dto.Metric{}
		err := nodePricingLookupTotal.WithLabelValues("fallback").(prometheus.Counter).Write(metric)
		assert.NoError(t, err)
		assert.Equal(t, float64(1), metric.GetCounter().GetValue())
	})

	t.Run("tracks success and fallback separately", func(t *testing.T) {
		nodePricingLookupTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "test_node_pricing_lookup_total_3",
			Help: "Test metric",
		}, []string{"status"})

		RecordNodePricingLookup("success")
		RecordNodePricingLookup("success")
		RecordNodePricingLookup("success")
		RecordNodePricingLookup("fallback")

		successMetric := &dto.Metric{}
		err := nodePricingLookupTotal.WithLabelValues("success").(prometheus.Counter).Write(successMetric)
		assert.NoError(t, err)
		assert.Equal(t, float64(3), successMetric.GetCounter().GetValue())

		fallbackMetric := &dto.Metric{}
		err = nodePricingLookupTotal.WithLabelValues("fallback").(prometheus.Counter).Write(fallbackMetric)
		assert.NoError(t, err)
		assert.Equal(t, float64(1), fallbackMetric.GetCounter().GetValue())
	})
}

func TestNodePricingLookupMetricDefinition(t *testing.T) {
	// Test that the metric can be created with the expected configuration
	// matching the production definition in initCostModelMetrics
	t.Run("metric created with correct name and labels", func(t *testing.T) {
		counter := prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "opencost_node_pricing_lookup_total",
			Help: "opencost_node_pricing_lookup_total Total node pricing lookups by status",
		}, []string{"status"})

		// Verify we can create counters with expected label values
		successCounter := counter.WithLabelValues("success")
		fallbackCounter := counter.WithLabelValues("fallback")

		assert.NotNil(t, successCounter)
		assert.NotNil(t, fallbackCounter)

		// Verify the counters work correctly
		successCounter.Inc()
		fallbackCounter.Inc()
		fallbackCounter.Inc()

		successMetric := &dto.Metric{}
		err := successCounter.(prometheus.Counter).Write(successMetric)
		assert.NoError(t, err)
		assert.Equal(t, float64(1), successMetric.GetCounter().GetValue())

		fallbackMetric := &dto.Metric{}
		err = fallbackCounter.(prometheus.Counter).Write(fallbackMetric)
		assert.NoError(t, err)
		assert.Equal(t, float64(2), fallbackMetric.GetCounter().GetValue())
	})

	t.Run("metric supports multiple increments", func(t *testing.T) {
		counter := prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "opencost_node_pricing_lookup_total_multi",
			Help: "Test metric",
		}, []string{"status"})

		// Simulate high-volume scenario
		for i := 0; i < 100; i++ {
			counter.WithLabelValues("success").Inc()
		}
		for i := 0; i < 25; i++ {
			counter.WithLabelValues("fallback").Inc()
		}

		successMetric := &dto.Metric{}
		err := counter.WithLabelValues("success").(prometheus.Counter).Write(successMetric)
		assert.NoError(t, err)
		assert.Equal(t, float64(100), successMetric.GetCounter().GetValue())

		fallbackMetric := &dto.Metric{}
		err = counter.WithLabelValues("fallback").(prometheus.Counter).Write(fallbackMetric)
		assert.NoError(t, err)
		assert.Equal(t, float64(25), fallbackMetric.GetCounter().GetValue())
	})
}

func TestRecordNodePricingLookupConcurrency(t *testing.T) {
	// Save original and restore after test
	original := nodePricingLookupTotal
	defer func() { nodePricingLookupTotal = original }()

	t.Run("concurrent calls are safe", func(t *testing.T) {
		nodePricingLookupTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "test_node_pricing_lookup_concurrent",
			Help: "Test metric",
		}, []string{"status"})

		done := make(chan bool)
		iterations := 100

		// Concurrent success recordings
		go func() {
			for i := 0; i < iterations; i++ {
				RecordNodePricingLookup("success")
			}
			done <- true
		}()

		// Concurrent fallback recordings
		go func() {
			for i := 0; i < iterations; i++ {
				RecordNodePricingLookup("fallback")
			}
			done <- true
		}()

		// Wait for both goroutines
		<-done
		<-done

		successMetric := &dto.Metric{}
		err := nodePricingLookupTotal.WithLabelValues("success").(prometheus.Counter).Write(successMetric)
		assert.NoError(t, err)
		assert.Equal(t, float64(iterations), successMetric.GetCounter().GetValue())

		fallbackMetric := &dto.Metric{}
		err = nodePricingLookupTotal.WithLabelValues("fallback").(prometheus.Counter).Write(fallbackMetric)
		assert.NoError(t, err)
		assert.Equal(t, float64(iterations), fallbackMetric.GetCounter().GetValue())
	})
}
