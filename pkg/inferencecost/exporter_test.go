package inferencecost

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestNewExporter(t *testing.T) {
	e := NewExporter()
	if e == nil {
		t.Fatal("NewExporter() returned nil")
	}
	if e.totalCost == nil {
		t.Error("totalCost metric is nil")
	}
	if e.costPerMillionTokens == nil {
		t.Error("costPerMillionTokens metric is nil")
	}
}

func TestExporter_Register(t *testing.T) {
	// Create a new registry for testing
	registry := prometheus.NewRegistry()

	e := NewExporter()

	// Register with custom registry
	err := registry.Register(e.totalCost)
	if err != nil {
		t.Errorf("Failed to register totalCost: %v", err)
	}

	err = registry.Register(e.costPerMillionTokens)
	if err != nil {
		t.Errorf("Failed to register costPerMillionTokens: %v", err)
	}

	// Set some values so metrics appear in Gather()
	e.totalCost.WithLabelValues("test", "v1", "ns").Set(1.0)
	e.costPerMillionTokens.WithLabelValues("test", "v1", "ns").Set(100.0)

	// Verify metrics are registered
	metricFamilies, err := registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	foundTotalCost := false
	foundCostPerMillion := false

	for _, mf := range metricFamilies {
		if mf.GetName() == "opencost_inference_total_cost" {
			foundTotalCost = true
		}
		if mf.GetName() == "opencost_inference_cost_per_million_tokens" {
			foundCostPerMillion = true
		}
	}

	if !foundTotalCost {
		t.Error("opencost_inference_total_cost metric not found in registry")
	}
	if !foundCostPerMillion {
		t.Error("opencost_inference_cost_per_million_tokens metric not found in registry")
	}
}

func TestExporter_Export(t *testing.T) {
	// Create a new registry for testing
	registry := prometheus.NewRegistry()

	e := NewExporter()
	registry.MustRegister(e.totalCost)
	registry.MustRegister(e.costPerMillionTokens)

	tests := []struct {
		name    string
		metrics []*ModelMetrics
		check   func(t *testing.T, registry *prometheus.Registry)
	}{
		{
			name: "single model",
			metrics: []*ModelMetrics{
				{
					ModelName:            "test-model",
					ModelVersion:         "v1.0",
					Namespace:            "test-ns",
					TotalCost:            1.50,
					CostPerMillionTokens: 500.0,
					Timestamp:            time.Now(),
				},
			},
			check: func(t *testing.T, registry *prometheus.Registry) {
				metricFamilies, err := registry.Gather()
				if err != nil {
					t.Fatalf("Failed to gather metrics: %v", err)
				}

				for _, mf := range metricFamilies {
					if mf.GetName() == "opencost_inference_total_cost" {
						if len(mf.GetMetric()) != 1 {
							t.Errorf("Expected 1 metric, got %d", len(mf.GetMetric()))
						}
						metric := mf.GetMetric()[0]
						if metric.GetGauge().GetValue() != 1.50 {
							t.Errorf("Expected value 1.50, got %v", metric.GetGauge().GetValue())
						}
						// Check labels
						labels := metric.GetLabel()
						checkLabel(t, labels, "model_name", "test-model")
						checkLabel(t, labels, "model_version", "v1.0")
						checkLabel(t, labels, "namespace", "test-ns")
					}
				}
			},
		},
		{
			name: "multiple models",
			metrics: []*ModelMetrics{
				{
					ModelName:            "model-1",
					ModelVersion:         "unknown",
					Namespace:            "ns-1",
					TotalCost:            2.00,
					CostPerMillionTokens: 1000.0,
					Timestamp:            time.Now(),
				},
				{
					ModelName:            "model-2",
					ModelVersion:         "unknown",
					Namespace:            "ns-2",
					TotalCost:            3.00,
					CostPerMillionTokens: 1500.0,
					Timestamp:            time.Now(),
				},
			},
			check: func(t *testing.T, registry *prometheus.Registry) {
				metricFamilies, err := registry.Gather()
				if err != nil {
					t.Fatalf("Failed to gather metrics: %v", err)
				}

				for _, mf := range metricFamilies {
					if mf.GetName() == "opencost_inference_total_cost" {
						if len(mf.GetMetric()) != 2 {
							t.Errorf("Expected 2 metrics, got %d", len(mf.GetMetric()))
						}
					}
				}
			},
		},
		{
			name: "empty model version defaults to unknown",
			metrics: []*ModelMetrics{
				{
					ModelName:            "test-model",
					ModelVersion:         "",
					Namespace:            "test-ns",
					TotalCost:            1.00,
					CostPerMillionTokens: 500.0,
					Timestamp:            time.Now(),
				},
			},
			check: func(t *testing.T, registry *prometheus.Registry) {
				metricFamilies, err := registry.Gather()
				if err != nil {
					t.Fatalf("Failed to gather metrics: %v", err)
				}

				for _, mf := range metricFamilies {
					if mf.GetName() == "opencost_inference_total_cost" {
						metric := mf.GetMetric()[0]
						labels := metric.GetLabel()
						checkLabel(t, labels, "model_version", "unknown")
					}
				}
			},
		},
		{
			name:    "empty metrics slice",
			metrics: []*ModelMetrics{},
			check: func(t *testing.T, registry *prometheus.Registry) {
				// Should not panic
			},
		},
		{
			name: "zero costs",
			metrics: []*ModelMetrics{
				{
					ModelName:            "zero-model",
					ModelVersion:         "unknown",
					Namespace:            "test-ns",
					TotalCost:            0,
					CostPerMillionTokens: 0,
					Timestamp:            time.Now(),
				},
			},
			check: func(t *testing.T, registry *prometheus.Registry) {
				metricFamilies, err := registry.Gather()
				if err != nil {
					t.Fatalf("Failed to gather metrics: %v", err)
				}

				for _, mf := range metricFamilies {
					if mf.GetName() == "opencost_inference_total_cost" {
						metric := mf.GetMetric()[0]
						if metric.GetGauge().GetValue() != 0 {
							t.Errorf("Expected value 0, got %v", metric.GetGauge().GetValue())
						}
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset metrics before each test
			e.totalCost.Reset()
			e.costPerMillionTokens.Reset()

			// Export metrics
			e.Export(tt.metrics)

			// Run checks
			if tt.check != nil {
				tt.check(t, registry)
			}
		})
	}
}

func TestExporter_Export_UpdatesExistingMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	e := NewExporter()
	registry.MustRegister(e.totalCost)
	registry.MustRegister(e.costPerMillionTokens)

	// First export
	metrics1 := []*ModelMetrics{
		{
			ModelName:            "test-model",
			ModelVersion:         "unknown",
			Namespace:            "test-ns",
			TotalCost:            1.00,
			CostPerMillionTokens: 500.0,
			Timestamp:            time.Now(),
		},
	}
	e.Export(metrics1)

	// Second export with updated values
	metrics2 := []*ModelMetrics{
		{
			ModelName:            "test-model",
			ModelVersion:         "unknown",
			Namespace:            "test-ns",
			TotalCost:            2.00,
			CostPerMillionTokens: 1000.0,
			Timestamp:            time.Now(),
		},
	}
	e.Export(metrics2)

	// Verify updated values
	metricFamilies, err := registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	for _, mf := range metricFamilies {
		if mf.GetName() == "opencost_inference_total_cost" {
			metric := mf.GetMetric()[0]
			if metric.GetGauge().GetValue() != 2.00 {
				t.Errorf("Expected updated value 2.00, got %v", metric.GetGauge().GetValue())
			}
		}
		if mf.GetName() == "opencost_inference_cost_per_million_tokens" {
			metric := mf.GetMetric()[0]
			if metric.GetGauge().GetValue() != 1000.0 {
				t.Errorf("Expected updated value 1000.0, got %v", metric.GetGauge().GetValue())
			}
		}
	}
}

// Helper function to check label values
func checkLabel(t *testing.T, labels []*dto.LabelPair, name, expectedValue string) {
	t.Helper()
	for _, label := range labels {
		if label.GetName() == name {
			if label.GetValue() != expectedValue {
				t.Errorf("Label %s: expected %s, got %s", name, expectedValue, label.GetValue())
			}
			return
		}
	}
	t.Errorf("Label %s not found", name)
}

func BenchmarkExporter_Export(b *testing.B) {
	e := NewExporter()
	registry := prometheus.NewRegistry()
	registry.MustRegister(e.totalCost)
	registry.MustRegister(e.costPerMillionTokens)

	// Create test data
	metrics := make([]*ModelMetrics, 10)
	for i := 0; i < 10; i++ {
		metrics[i] = &ModelMetrics{
			ModelName:            "benchmark-model",
			ModelVersion:         "unknown",
			Namespace:            "benchmark-ns",
			TotalCost:            1.50,
			CostPerMillionTokens: 500.0,
			Timestamp:            time.Now(),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Export(metrics)
	}
}

// Made with Bob
