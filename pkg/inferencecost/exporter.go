package inferencecost

import (
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/prometheus/client_golang/prometheus"
)

// Exporter exports inference cost metrics to Prometheus
type Exporter struct {
	totalCost            *prometheus.GaugeVec
	costPerMillionTokens *prometheus.GaugeVec
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

		// Export with namespace label
		e.totalCost.WithLabelValues(m.ModelName, modelVersion, m.Namespace).Set(m.TotalCost)
		e.costPerMillionTokens.WithLabelValues(m.ModelName, modelVersion, m.Namespace).Set(m.CostPerMillionTokens)

		log.Debugf("Exported metrics for model %s in namespace %s: total_cost=%.2f, cost_per_1m_tokens=%.2f",
			m.ModelName, m.Namespace, m.TotalCost, m.CostPerMillionTokens)
	}
}

// Made with Bob
