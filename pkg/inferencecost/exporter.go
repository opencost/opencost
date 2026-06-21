package inferencecost

import (
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/prometheus/client_golang/prometheus"
)

// Exporter registers and emits the three llm_* Prometheus metrics.
type Exporter struct {
	totalCost            *prometheus.GaugeVec
	costPerMillionTokens *prometheus.GaugeVec
}

// NewExporter creates an Exporter with all gauge vectors initialised.
func NewExporter() *Exporter {
	return &Exporter{
		totalCost: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "llm_total_cost",
				Help: "Hourly infrastructure cost attributed to an LLM model. " +
					"cost_basis=allocation reconciles to the infrastructure bill (includes idle and shared infra costs). " +
					"cost_basis=usage reflects active compute only; idle and shared infra costs are excluded and it does NOT reconcile to the bill.",
			},
			[]string{"model_name", "model_version", "namespace", "cost_basis"},
		),
		costPerMillionTokens: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "llm_cost_per_million_tokens",
				Help: "Infrastructure cost per 1M tokens. " +
					"Without phase label: blended cost (input + output combined). " +
					"phase=prompt: cost per 1M effective input tokens (cached tokens excluded when KV cache correction is active). " +
					"phase=generation: cost per 1M output tokens. " +
					"cost_basis=allocation includes idle and shared infra; reconciles to bill. " +
					"cost_basis=usage reflects active compute only; idle and shared infra costs excluded; does NOT reconcile to bill. " +
					"allocation_method=compute_time_with_cache_hits: split by vLLM timing with KV cache correction. " +
					"allocation_method=prefix_caching_off: prefix caching disabled; cache correction not applicable. " +
					"allocation_method=compute_time: split by vLLM timing without cache correction (block size unknown or no cache hits). " +
					"allocation_method=multiplier: fixed output/input ratio used (timing metrics unavailable).",
			},
			[]string{"model_name", "model_version", "namespace", "cost_basis", "phase", "allocation_method"},
		),
	}
}

// Register registers all gauge vectors with the default Prometheus registry.
// Returns an error if any registration fails (e.g. called twice).
func (e *Exporter) Register() error {
	for _, c := range []prometheus.Collector{
		e.totalCost,
		e.costPerMillionTokens,
	} {
		if err := prometheus.Register(c); err != nil {
			return err
		}
	}
	return nil
}

// Export sets gauge values for all metrics derived from the given InferenceCost slice.
func (e *Exporter) Export(metrics []*InferenceCost) {
	for _, m := range metrics {
		version := m.Properties.ModelVersion
		if version == "" {
			version = "unknown"
		}
		method := string(m.AllocationMethod)

		for _, basis := range []CostBasis{CostBasisUsage, CostBasisAllocation} {
			basisStr := string(basis)

			e.totalCost.WithLabelValues(
				m.Properties.ModelName, version, m.Properties.Namespace, basisStr,
			).Set(totalCostForBasis(m, basis))

			// Blended cost (no phase label)
			e.costPerMillionTokens.WithLabelValues(
				m.Properties.ModelName, version, m.Properties.Namespace, basisStr, "", "",
			).Set(m.CostPerMillionTokens[basis])

			// Input cost (phase=prompt)
			e.costPerMillionTokens.WithLabelValues(
				m.Properties.ModelName, version, m.Properties.Namespace, basisStr, "prompt", method,
			).Set(m.InputCostPerMillionTokens[basis])

			// Output cost (phase=generation)
			e.costPerMillionTokens.WithLabelValues(
				m.Properties.ModelName, version, m.Properties.Namespace, basisStr, "generation", method,
			).Set(m.OutputCostPerMillionTokens[basis])
		}

		log.Debugf("InferenceCost: exported model=%s ns=%s alloc_total=$%.4f usage_total=$%.4f method=%s",
			m.Properties.ModelName, m.Properties.Namespace,
			m.AllocationTotalCost, m.UsageTotalCost, method)
	}
}

func totalCostForBasis(m *InferenceCost, basis CostBasis) float64 {
	if basis == CostBasisUsage {
		return m.UsageTotalCost
	}
	return m.AllocationTotalCost
}
