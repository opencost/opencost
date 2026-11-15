package inferencecost

import (
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/prometheus/client_golang/prometheus"
)

// Exporter registers and emits the llm_* Prometheus metrics.
type Exporter struct {
	totalCost            *prometheus.GaugeVec
	costPerMillionTokens *prometheus.GaugeVec
	cacheSavingsFraction *prometheus.GaugeVec
	dimensionCost        *prometheus.GaugeVec
	unmatchedKeys        *prometheus.GaugeVec
	tokenCoverage        *prometheus.GaugeVec
	identityCoverage     *prometheus.GaugeVec
	estimatedTokenShare  *prometheus.GaugeVec
}

// NewExporter creates an Exporter with all gauge vectors initialised.
func NewExporter() *Exporter {
	return &Exporter{
		totalCost: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "llm_total_hourly_cost",
				Help: "Hourly infrastructure cost attributed to an LLM model. " +
					"cost_basis=allocation reconciles to the infrastructure bill (includes idle and shared infra costs). " +
					"cost_basis=usage reflects active compute only; idle and shared infra costs are excluded and it does NOT reconcile to the bill.",
			},
			[]string{"model_name", "model_version", "namespace", "cost_basis", "workload_type"},
		),
		costPerMillionTokens: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "llm_cost_per_million_tokens",
				Help: "Infrastructure cost per 1M tokens. " +
					"Without phase label: blended cost (input + output combined). " +
					"phase=prompt: cost per 1M delivered input tokens (promptTokens denominator; see llm_cache_savings_fraction for KV cache utilization). " +
					"phase=generation: cost per 1M output tokens. " +
					"cost_basis=allocation includes idle and shared infra; reconciles to bill. " +
					"cost_basis=usage reflects active compute only; idle and shared infra costs excluded; does NOT reconcile to bill. " +
					"allocation_method=compute_time: split proportionally by vLLM prefill/decode time; KV cache savings in llm_cache_savings_fraction. " +
					"allocation_method=prefix_caching_off: same time-based split; prefix caching explicitly disabled on vLLM instance. " +
					"allocation_method=multiplier: fixed output/input ratio used (timing metrics unavailable).",
			},
			[]string{"model_name", "model_version", "namespace", "cost_basis", "phase", "allocation_method", "workload_type"},
		),
		cacheSavingsFraction: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "llm_cache_savings_fraction",
				Help: "Fraction of prompt tokens served from the KV cache (range 0–1). " +
					"A value of 0.9 means 90% of prompt tokens were cache hits. " +
					"Zero when prefix caching is disabled (allocation_method=prefix_caching_off) or when no cache hits occurred in the window.",
			},
			[]string{"model_name", "model_version", "namespace", "workload_type"},
		),
		dimensionCost: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "llm_dimension_hourly_cost",
				Help: "Hourly infrastructure cost attributed to an inference request dimension (tenant, workload, user).",
			},
			[]string{"model_name", "namespace", "cost_basis", "tenant_id", "workload_id", "user_id"},
		),
		unmatchedKeys: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "llm_dimension_unmatched_keys",
				Help: "Attributed token series that could not be joined to a model entry, per producer mapping.",
			},
			[]string{"producer"},
		),
		tokenCoverage: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "llm_dimension_token_coverage",
				Help: "Attributed tokens divided by the engine's reference tokens, input and output, per model and namespace.",
			},
			[]string{"model_name", "namespace", "direction"},
		),
		identityCoverage: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "llm_dimension_identity_coverage",
				Help: "Share of attributed tokens carrying a real tenant_id and workload_id (non-empty, not a missing-identity sentinel), per model and namespace.",
			},
			[]string{"model_name", "namespace"},
		),
		estimatedTokenShare: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "llm_dimension_estimated_token_share",
				Help: "Estimated tokens divided by attributed tokens, per model and namespace.",
			},
			[]string{"model_name", "namespace"},
		),
	}
}

// Register registers all gauge vectors with the default Prometheus registry.
// Returns an error if any registration fails (e.g. called twice).
func (e *Exporter) Register() error {
	for _, c := range []prometheus.Collector{
		e.totalCost,
		e.costPerMillionTokens,
		e.cacheSavingsFraction,
		e.dimensionCost,
		e.unmatchedKeys,
		e.tokenCoverage,
		e.identityCoverage,
		e.estimatedTokenShare,
	} {
		if err := prometheus.Register(c); err != nil {
			return err
		}
	}
	return nil
}

// ExportUnmatchedKeys sets gauge values for unmatched keys per producer.
func (e *Exporter) ExportUnmatchedKeys(unmatched map[string]float64) {
	e.unmatchedKeys.Reset()
	for prod, val := range unmatched {
		e.unmatchedKeys.WithLabelValues(prod).Set(val)
	}
}

// Export sets gauge values for all metrics derived from model and dimension costs.
// Gauges are reset before each export so decommissioned models do not persist.
func (e *Exporter) Export(metrics []*InferenceCost, dimMetrics ...[]*InferenceCost) {
	e.totalCost.Reset()
	e.costPerMillionTokens.Reset()
	e.cacheSavingsFraction.Reset()
	e.dimensionCost.Reset()
	e.tokenCoverage.Reset()
	e.identityCoverage.Reset()
	e.estimatedTokenShare.Reset()

	for _, m := range metrics {
		version := m.Properties.ModelVersion
		if version == "" {
			version = "unknown"
		}
		method := string(m.AllocationMethod)
		workloadType := m.Properties.WorkloadType
		if workloadType == "" {
			workloadType = "unknown"
		}

		// Calculate window duration in hours for normalization to hourly rate
		windowDuration := m.Window.End.Sub(m.Window.Start)
		windowHours := windowDuration.Hours()
		if windowHours <= 0 {
			// Avoid division by zero; skip this metric if window is invalid
			log.Warnf("InferenceCost: skipping export for model=%s ns=%s (invalid window duration: %v)",
				m.Properties.ModelName, m.Properties.Namespace, windowDuration)
			continue
		}

		for _, basis := range []CostBasis{CostBasisUsage, CostBasisAllocation} {
			basisStr := string(basis)

			// Normalize total cost to hourly rate: totalCost / windowHours
			hourlyCost := totalCostForBasis(m, basis) / windowHours
			e.totalCost.WithLabelValues(
				m.Properties.ModelName, version, m.Properties.Namespace, basisStr, workloadType,
			).Set(hourlyCost)

			// Blended cost (no phase label)
			e.costPerMillionTokens.WithLabelValues(
				m.Properties.ModelName, version, m.Properties.Namespace, basisStr, "", "", workloadType,
			).Set(m.CostPerMillionTokens[basis])

			// Input cost (phase=prompt)
			e.costPerMillionTokens.WithLabelValues(
				m.Properties.ModelName, version, m.Properties.Namespace, basisStr, "prompt", method, workloadType,
			).Set(m.InputCostPerMillionTokens[basis])

			// Output cost (phase=generation)
			e.costPerMillionTokens.WithLabelValues(
				m.Properties.ModelName, version, m.Properties.Namespace, basisStr, "generation", method, workloadType,
			).Set(m.OutputCostPerMillionTokens[basis])
		}

		e.cacheSavingsFraction.WithLabelValues(
			m.Properties.ModelName, version, m.Properties.Namespace, workloadType,
		).Set(m.CacheSavingsFraction)

		log.Debugf("InferenceCost: exported model=%s ns=%s alloc_total=$%.4f usage_total=$%.4f method=%s workload_type=%s",
			m.Properties.ModelName, m.Properties.Namespace,
			m.AllocationTotalCost, m.UsageTotalCost, method, workloadType)
	}

	if len(dimMetrics) == 0 || len(dimMetrics[0]) == 0 {
		return
	}

	dims := dimMetrics[0]
	for _, dm := range dims {
		windowDuration := dm.Window.End.Sub(dm.Window.Start)
		windowHours := windowDuration.Hours()
		if windowHours <= 0 {
			continue
		}

		for _, basis := range []CostBasis{CostBasisUsage, CostBasisAllocation} {
			basisStr := string(basis)
			hourlyCost := totalCostForBasis(dm, basis) / windowHours
			e.dimensionCost.WithLabelValues(
				dm.Properties.ModelName,
				dm.Properties.Namespace,
				basisStr,
				dm.Properties.TenantID,
				dm.Properties.WorkloadID,
				dm.Properties.UserID,
			).Set(hourlyCost)
		}
	}

	// Calculate and set coverage and estimated share gauges per model and namespace
	for _, m := range metrics {
		var sumPrompt, sumComp, sumEstPrompt, sumEstComp, realTokens, allAttributed float64
		for _, dm := range dims {
			if dm.Properties.ModelName == m.Properties.ModelName && dm.Properties.Namespace == m.Properties.Namespace {
				if dm.Properties.TenantID != "__unmetered__" {
					sumPrompt += dm.PromptTokens
					sumComp += dm.GenerationTokens
					sumEstPrompt += dm.EstimatedPromptTokens
					sumEstComp += dm.EstimatedCompletionTokens
					tot := dm.PromptTokens + dm.GenerationTokens
					allAttributed += tot
					if dm.Properties.TenantID != "__unidentified__" && dm.Properties.WorkloadID != "__unidentified__" {
						realTokens += tot
					}
				}
			}
		}

		if m.PromptTokens > 0 {
			e.tokenCoverage.WithLabelValues(m.Properties.ModelName, m.Properties.Namespace, "input").Set(sumPrompt / m.PromptTokens)
		}
		if m.GenerationTokens > 0 {
			e.tokenCoverage.WithLabelValues(m.Properties.ModelName, m.Properties.Namespace, "output").Set(sumComp / m.GenerationTokens)
		}
		if allAttributed > 0 {
			e.identityCoverage.WithLabelValues(m.Properties.ModelName, m.Properties.Namespace).Set(realTokens / allAttributed)
			e.estimatedTokenShare.WithLabelValues(m.Properties.ModelName, m.Properties.Namespace).Set((sumEstPrompt + sumEstComp) / allAttributed)
		}
	}
}

func totalCostForBasis(m *InferenceCost, basis CostBasis) float64 {
	if basis == CostBasisUsage {
		return m.UsageTotalCost
	}
	return m.AllocationTotalCost
}
