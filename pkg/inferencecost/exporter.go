package inferencecost

import (
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/prometheus/client_golang/prometheus"
)

// Exporter exports inference cost metrics to Prometheus
type Exporter struct {
	totalCost                  *prometheus.GaugeVec
	costPerMillionTokens       *prometheus.GaugeVec
	inputCostPerMillionTokens  *prometheus.GaugeVec
	outputCostPerMillionTokens *prometheus.GaugeVec
}

// NewExporter creates a new Prometheus exporter
func NewExporter() *Exporter {
	return &Exporter{
		totalCost: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "opencost_inference_total_cost",
				Help: "Total infrastructure cost attributed to inference for a specific model in a specific namespace",
			},
			[]string{"model_name", "model_version", "namespace"},
		),
		costPerMillionTokens: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "opencost_inference_cost_per_million_tokens",
				Help: "Cost per 1 million tokens processed (input + output) for a specific model in a specific namespace",
			},
			[]string{"model_name", "model_version", "namespace"},
		),
		inputCostPerMillionTokens: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "opencost_inference_input_cost_per_million_tokens",
				Help: "Cost per 1 million input (prompt) tokens. allocation_method label indicates calculation method: compute_time or multiplier",
			},
			[]string{"model_name", "model_version", "namespace", "allocation_method"},
		),
		outputCostPerMillionTokens: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "opencost_inference_output_cost_per_million_tokens",
				Help: "Cost per 1 million output (generation) tokens. allocation_method label indicates calculation method: compute_time or multiplier",
			},
			[]string{"model_name", "model_version", "namespace", "allocation_method"},
		),
	}
}

// Register registers metrics with Prometheus
func (e *Exporter) Register() error {
	if err := prometheus.Register(e.totalCost); err != nil {
		return err
	}
	if err := prometheus.Register(e.costPerMillionTokens); err != nil {
		return err
	}
	if err := prometheus.Register(e.inputCostPerMillionTokens); err != nil {
		return err
	}
	if err := prometheus.Register(e.outputCostPerMillionTokens); err != nil {
		return err
	}
	return nil
}

// Export exports metrics to Prometheus with namespace label
func (e *Exporter) Export(metrics []*ModelMetrics) {
	for _, m := range metrics {
		// Use "unknown" as default version for PoC
		modelVersion := m.ModelVersion
		if modelVersion == "" {
			modelVersion = "unknown"
		}

		// Determine which allocation method was actually used
		allocationMethod := "multiplier"
		if m.InputProcessingTime > 0 || m.OutputProcessingTime > 0 {
			allocationMethod = "compute_time"
		}

		// Export existing metrics (for backward compatibility)
		e.totalCost.WithLabelValues(m.ModelName, modelVersion, m.Namespace).Set(m.TotalCost)
		e.costPerMillionTokens.WithLabelValues(m.ModelName, modelVersion, m.Namespace).Set(m.CostPerMillionTokens)

		// Export differentiated cost metrics with allocation_method label
		e.inputCostPerMillionTokens.WithLabelValues(m.ModelName, modelVersion, m.Namespace, allocationMethod).Set(m.InputCostPerMillionTokens)
		e.outputCostPerMillionTokens.WithLabelValues(m.ModelName, modelVersion, m.Namespace, allocationMethod).Set(m.OutputCostPerMillionTokens)

		log.Debugf("Exported metrics for model %s in namespace %s: total_cost=%.6f, blended=%.2f/M, input=%.2f/M, output=%.2f/M, method=%s",
			m.ModelName, m.Namespace, m.TotalCost, m.CostPerMillionTokens, m.InputCostPerMillionTokens, m.OutputCostPerMillionTokens, allocationMethod)
	}
}

// Made with Bob
