package inferencecost

import (
	"testing"
	"time"
)

func TestCalculator_CalculateCosts(t *testing.T) {
	tests := []struct {
		name    string
		metrics []*ModelMetrics
		wantErr bool
		check   func(t *testing.T, metrics []*ModelMetrics)
	}{
		{
			name: "single model with valid data",
			metrics: []*ModelMetrics{
				{
					ModelName:        "test-model",
					Namespace:        "test-ns",
					PromptTokens:     1000,
					GenerationTokens: 500,
					TotalTokens:      1500,
					GPUCost:          0.50,
					TotalCost:        0.50,
					Timestamp:        time.Now(),
				},
			},
			wantErr: false,
			check: func(t *testing.T, metrics []*ModelMetrics) {
				m := metrics[0]
				expectedCostPerToken := 0.50 / 1500
				if m.CostPerToken != expectedCostPerToken {
					t.Errorf("CostPerToken = %v, want %v", m.CostPerToken, expectedCostPerToken)
				}
				expectedCostPerMillion := expectedCostPerToken * 1000000
				if m.CostPerMillionTokens != expectedCostPerMillion {
					t.Errorf("CostPerMillionTokens = %v, want %v", m.CostPerMillionTokens, expectedCostPerMillion)
				}
			},
		},
		{
			name: "multiple models",
			metrics: []*ModelMetrics{
				{
					ModelName:        "model-1",
					Namespace:        "ns-1",
					PromptTokens:     2000,
					GenerationTokens: 1000,
					TotalTokens:      3000,
					GPUCost:          1.00,
					TotalCost:        1.00,
					Timestamp:        time.Now(),
				},
				{
					ModelName:        "model-2",
					Namespace:        "ns-2",
					PromptTokens:     500,
					GenerationTokens: 250,
					TotalTokens:      750,
					GPUCost:          0.25,
					TotalCost:        0.25,
					Timestamp:        time.Now(),
				},
			},
			wantErr: false,
			check: func(t *testing.T, metrics []*ModelMetrics) {
				// Check first model
				m1 := metrics[0]
				expectedCostPerToken1 := 1.00 / 3000
				if m1.CostPerToken != expectedCostPerToken1 {
					t.Errorf("Model 1 CostPerToken = %v, want %v", m1.CostPerToken, expectedCostPerToken1)
				}

				// Check second model
				m2 := metrics[1]
				expectedCostPerToken2 := 0.25 / 750
				if m2.CostPerToken != expectedCostPerToken2 {
					t.Errorf("Model 2 CostPerToken = %v, want %v", m2.CostPerToken, expectedCostPerToken2)
				}
			},
		},
		{
			name: "zero tokens - division by zero handling",
			metrics: []*ModelMetrics{
				{
					ModelName:        "zero-model",
					Namespace:        "test-ns",
					PromptTokens:     0,
					GenerationTokens: 0,
					TotalTokens:      0,
					GPUCost:          0.50,
					TotalCost:        0.50,
					Timestamp:        time.Now(),
				},
			},
			wantErr: false,
			check: func(t *testing.T, metrics []*ModelMetrics) {
				m := metrics[0]
				if m.CostPerToken != 0 {
					t.Errorf("CostPerToken = %v, want 0 (division by zero)", m.CostPerToken)
				}
				if m.CostPerMillionTokens != 0 {
					t.Errorf("CostPerMillionTokens = %v, want 0 (division by zero)", m.CostPerMillionTokens)
				}
			},
		},
		{
			name: "high token count",
			metrics: []*ModelMetrics{
				{
					ModelName:        "high-volume",
					Namespace:        "prod",
					PromptTokens:     10000000,
					GenerationTokens: 5000000,
					TotalTokens:      15000000,
					GPUCost:          100.00,
					TotalCost:        100.00,
					Timestamp:        time.Now(),
				},
			},
			wantErr: false,
			check: func(t *testing.T, metrics []*ModelMetrics) {
				m := metrics[0]
				expectedCostPerToken := 100.00 / 15000000
				if m.CostPerToken != expectedCostPerToken {
					t.Errorf("CostPerToken = %v, want %v", m.CostPerToken, expectedCostPerToken)
				}
				expectedCostPerMillion := expectedCostPerToken * 1000000
				if m.CostPerMillionTokens != expectedCostPerMillion {
					t.Errorf("CostPerMillionTokens = %v, want %v", m.CostPerMillionTokens, expectedCostPerMillion)
				}
			},
		},
		{
			name: "very small cost",
			metrics: []*ModelMetrics{
				{
					ModelName:        "small-cost",
					Namespace:        "dev",
					PromptTokens:     100,
					GenerationTokens: 50,
					TotalTokens:      150,
					GPUCost:          0.0001,
					TotalCost:        0.0001,
					Timestamp:        time.Now(),
				},
			},
			wantErr: false,
			check: func(t *testing.T, metrics []*ModelMetrics) {
				m := metrics[0]
				expectedCostPerToken := 0.0001 / 150
				if m.CostPerToken != expectedCostPerToken {
					t.Errorf("CostPerToken = %v, want %v", m.CostPerToken, expectedCostPerToken)
				}
			},
		},
		{
			name:    "empty metrics slice",
			metrics: []*ModelMetrics{},
			wantErr: false,
			check: func(t *testing.T, metrics []*ModelMetrics) {
				if len(metrics) != 0 {
					t.Errorf("Expected empty metrics slice")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewCalculator()
			err := c.CalculateCosts(tt.metrics)
			if (err != nil) != tt.wantErr {
				t.Errorf("CalculateCosts() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.check != nil {
				tt.check(t, tt.metrics)
			}
		})
	}
}

func TestCalculator_calculateModelCosts(t *testing.T) {
	tests := []struct {
		name    string
		metric  *ModelMetrics
		wantErr bool
		check   func(t *testing.T, m *ModelMetrics)
	}{
		{
			name: "normal calculation",
			metric: &ModelMetrics{
				ModelName:        "test",
				Namespace:        "default",
				PromptTokens:     800,
				GenerationTokens: 200,
				TotalTokens:      1000,
				GPUCost:          1.00,
				TotalCost:        1.00,
			},
			wantErr: false,
			check: func(t *testing.T, m *ModelMetrics) {
				expectedCostPerToken := 1.00 / 1000
				if m.CostPerToken != expectedCostPerToken {
					t.Errorf("CostPerToken = %v, want %v", m.CostPerToken, expectedCostPerToken)
				}
				expectedCostPerMillion := expectedCostPerToken * 1000000
				if m.CostPerMillionTokens != expectedCostPerMillion {
					t.Errorf("CostPerMillionTokens = %v, want %v", m.CostPerMillionTokens, expectedCostPerMillion)
				}
			},
		},
		{
			name: "zero total tokens",
			metric: &ModelMetrics{
				ModelName:   "test",
				Namespace:   "default",
				TotalTokens: 0,
				TotalCost:   1.00,
			},
			wantErr: false,
			check: func(t *testing.T, m *ModelMetrics) {
				if m.CostPerToken != 0 {
					t.Errorf("CostPerToken = %v, want 0", m.CostPerToken)
				}
				if m.CostPerMillionTokens != 0 {
					t.Errorf("CostPerMillionTokens = %v, want 0", m.CostPerMillionTokens)
				}
			},
		},
		{
			name: "zero cost",
			metric: &ModelMetrics{
				ModelName:   "test",
				Namespace:   "default",
				TotalTokens: 1000,
				TotalCost:   0,
			},
			wantErr: false,
			check: func(t *testing.T, m *ModelMetrics) {
				if m.CostPerToken != 0 {
					t.Errorf("CostPerToken = %v, want 0", m.CostPerToken)
				}
				if m.CostPerMillionTokens != 0 {
					t.Errorf("CostPerMillionTokens = %v, want 0", m.CostPerMillionTokens)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewCalculator()
			err := c.calculateModelCosts(tt.metric)
			if (err != nil) != tt.wantErr {
				t.Errorf("calculateModelCosts() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.check != nil {
				tt.check(t, tt.metric)
			}
		})
	}
}

func BenchmarkCalculator_CalculateCosts(b *testing.B) {
	// Create test data
	metrics := make([]*ModelMetrics, 100)
	for i := 0; i < 100; i++ {
		metrics[i] = &ModelMetrics{
			ModelName:        "benchmark-model",
			Namespace:        "benchmark-ns",
			PromptTokens:     1000,
			GenerationTokens: 500,
			TotalTokens:      1500,
			GPUCost:          0.50,
			TotalCost:        0.50,
			Timestamp:        time.Now(),
		}
	}

	c := NewCalculator()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = c.CalculateCosts(metrics)
	}
}

// Made with Bob
