package inferencecost

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/common/model"
)

func TestNewCollector(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: &Config{
				PrometheusURL:      "http://localhost:9090",
				CollectionInterval: 60 * time.Second,
				Enabled:            true,
			},
			wantErr: false,
		},
		{
			name: "empty prometheus URL - client accepts it",
			config: &Config{
				PrometheusURL:      "",
				CollectionInterval: 60 * time.Second,
				Enabled:            true,
			},
			wantErr: false, // Prometheus client doesn't validate empty URL at creation time
		},
		{
			name: "invalid prometheus URL",
			config: &Config{
				PrometheusURL:      "://invalid-url",
				CollectionInterval: 60 * time.Second,
				Enabled:            true,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collector, err := NewCollector(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewCollector() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && collector == nil {
				t.Error("NewCollector() returned nil collector without error")
			}
			if !tt.wantErr && collector.config != tt.config {
				t.Error("NewCollector() did not store config correctly")
			}
		})
	}
}

func Test_parsePrometheusResult(t *testing.T) {
	tests := []struct {
		name    string
		result  model.Value
		want    map[string]float64
		wantErr bool
	}{
		{
			name: "valid vector result",
			result: model.Vector{
				&model.Sample{
					Metric: model.Metric{
						"model_name": "test-model",
						"namespace":  "test-ns",
					},
					Value: 100.5,
				},
				&model.Sample{
					Metric: model.Metric{
						"model_name": "another-model",
						"namespace":  "prod-ns",
					},
					Value: 200.75,
				},
			},
			want: map[string]float64{
				"test-model:test-ns":    100.5,
				"another-model:prod-ns": 200.75,
			},
			wantErr: false,
		},
		{
			name:    "empty vector",
			result:  model.Vector{},
			want:    map[string]float64{},
			wantErr: false,
		},
		{
			name: "missing model_name label - skipped",
			result: model.Vector{
				&model.Sample{
					Metric: model.Metric{
						"namespace": "test-ns",
					},
					Value: 100.5,
				},
			},
			want:    map[string]float64{},
			wantErr: false,
		},
		{
			name: "missing namespace label - defaults to unknown",
			result: model.Vector{
				&model.Sample{
					Metric: model.Metric{
						"model_name": "test-model",
					},
					Value: 100.5,
				},
			},
			want: map[string]float64{
				"test-model:unknown": 100.5,
			},
			wantErr: false,
		},
		{
			name:    "non-vector result - returns empty map",
			result:  model.Matrix{},
			want:    map[string]float64{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePrometheusResult(tt.result)
			if (err != nil) != tt.wantErr {
				t.Errorf("parsePrometheusResult() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(got) != len(tt.want) {
				t.Errorf("parsePrometheusResult() got %d results, want %d", len(got), len(tt.want))
				return
			}
			for key, wantVal := range tt.want {
				gotVal, ok := got[key]
				if !ok {
					t.Errorf("parsePrometheusResult() missing key %s", key)
					continue
				}
				if gotVal != wantVal {
					t.Errorf("parsePrometheusResult() key %s = %v, want %v", key, gotVal, wantVal)
				}
			}
		})
	}
}

func TestCollector_combineMetrics(t *testing.T) {
	tests := []struct {
		name             string
		promptTokens     map[string]float64
		generationTokens map[string]float64
		gpuCosts         map[string]float64
		wantCount        int
		check            func(t *testing.T, metrics []*ModelMetrics)
	}{
		{
			name: "single model complete data",
			promptTokens: map[string]float64{
				"model1:ns1": 1000,
			},
			generationTokens: map[string]float64{
				"model1:ns1": 500,
			},
			gpuCosts: map[string]float64{
				"model1:ns1": 0.50,
			},
			wantCount: 1,
			check: func(t *testing.T, metrics []*ModelMetrics) {
				m := metrics[0]
				if m.ModelName != "model1" {
					t.Errorf("ModelName = %s, want model1", m.ModelName)
				}
				if m.Namespace != "ns1" {
					t.Errorf("Namespace = %s, want ns1", m.Namespace)
				}
				if m.PromptTokens != 1000 {
					t.Errorf("PromptTokens = %v, want 1000", m.PromptTokens)
				}
				if m.GenerationTokens != 500 {
					t.Errorf("GenerationTokens = %v, want 500", m.GenerationTokens)
				}
				if m.TotalTokens != 1500 {
					t.Errorf("TotalTokens = %v, want 1500", m.TotalTokens)
				}
				if m.GPUCost != 0.50 {
					t.Errorf("GPUCost = %v, want 0.50", m.GPUCost)
				}
				if m.TotalCost != 0.50 {
					t.Errorf("TotalCost = %v, want 0.50", m.TotalCost)
				}
			},
		},
		{
			name: "multiple models",
			promptTokens: map[string]float64{
				"model1:ns1": 1000,
				"model2:ns2": 2000,
			},
			generationTokens: map[string]float64{
				"model1:ns1": 500,
				"model2:ns2": 1000,
			},
			gpuCosts: map[string]float64{
				"model1:ns1": 0.50,
				"model2:ns2": 1.00,
			},
			wantCount: 2,
			check: func(t *testing.T, metrics []*ModelMetrics) {
				if len(metrics) != 2 {
					t.Errorf("Got %d metrics, want 2", len(metrics))
				}
			},
		},
		{
			name: "missing generation tokens",
			promptTokens: map[string]float64{
				"model1:ns1": 1000,
			},
			generationTokens: map[string]float64{},
			gpuCosts: map[string]float64{
				"model1:ns1": 0.50,
			},
			wantCount: 1,
			check: func(t *testing.T, metrics []*ModelMetrics) {
				m := metrics[0]
				if m.GenerationTokens != 0 {
					t.Errorf("GenerationTokens = %v, want 0", m.GenerationTokens)
				}
				if m.TotalTokens != 1000 {
					t.Errorf("TotalTokens = %v, want 1000 (prompt only)", m.TotalTokens)
				}
			},
		},
		{
			name: "missing GPU costs",
			promptTokens: map[string]float64{
				"model1:ns1": 1000,
			},
			generationTokens: map[string]float64{
				"model1:ns1": 500,
			},
			gpuCosts:  map[string]float64{},
			wantCount: 1,
			check: func(t *testing.T, metrics []*ModelMetrics) {
				m := metrics[0]
				if m.GPUCost != 0 {
					t.Errorf("GPUCost = %v, want 0", m.GPUCost)
				}
				if m.TotalCost != 0 {
					t.Errorf("TotalCost = %v, want 0", m.TotalCost)
				}
			},
		},
		{
			name:             "all empty",
			promptTokens:     map[string]float64{},
			generationTokens: map[string]float64{},
			gpuCosts:         map[string]float64{},
			wantCount:        0,
			check:            nil,
		},
		{
			name: "partial overlap",
			promptTokens: map[string]float64{
				"model1:ns1": 1000,
				"model2:ns2": 2000,
			},
			generationTokens: map[string]float64{
				"model1:ns1": 500,
			},
			gpuCosts: map[string]float64{
				"model1:ns1": 0.50,
				"model3:ns3": 1.50,
			},
			wantCount: 3,
			check: func(t *testing.T, metrics []*ModelMetrics) {
				// Should have metrics for all unique model:namespace combinations
				keys := make(map[string]bool)
				for _, m := range metrics {
					key := m.ModelName + ":" + m.Namespace
					keys[key] = true
				}
				if len(keys) != 3 {
					t.Errorf("Got %d unique models, want 3", len(keys))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Collector{}
			got := c.combineMetrics(tt.promptTokens, tt.generationTokens, tt.gpuCosts)
			if len(got) != tt.wantCount {
				t.Errorf("combineMetrics() returned %d metrics, want %d", len(got), tt.wantCount)
			}
			if tt.check != nil {
				tt.check(t, got)
			}
		})
	}
}

func TestCollector_CollectMetrics_Integration(t *testing.T) {
	// This is a placeholder for integration tests that would require
	// a real or mocked Prometheus server
	t.Skip("Integration test - requires Prometheus server")

	config := &Config{
		PrometheusURL:      "http://localhost:9090",
		CollectionInterval: 60 * time.Second,
		Enabled:            true,
	}

	collector, err := NewCollector(config)
	if err != nil {
		t.Fatalf("Failed to create collector: %v", err)
	}

	ctx := context.Background()
	metrics, err := collector.CollectMetrics(ctx)
	if err != nil {
		t.Fatalf("CollectMetrics() error = %v", err)
	}

	if metrics == nil {
		t.Error("CollectMetrics() returned nil metrics")
	}
}

func BenchmarkCollector_combineMetrics(b *testing.B) {
	// Create test data
	promptTokens := make(map[string]float64)
	generationTokens := make(map[string]float64)
	gpuCosts := make(map[string]float64)

	for i := 0; i < 100; i++ {
		key := "model:namespace"
		promptTokens[key] = 1000
		generationTokens[key] = 500
		gpuCosts[key] = 0.50
	}

	c := &Collector{}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = c.combineMetrics(promptTokens, generationTokens, gpuCosts)
	}
}

// Made with Bob
