package inferencecost

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/opencost/opencost/core/pkg/source"
)

func TestEnv_Defaults(t *testing.T) {
	t.Setenv("INFERENCE_ATTRIBUTION_ENABLED", "true")
	t.Setenv("INFERENCE_ATTRIBUTION_NAMESPACE_LABEL", "") // unset -> default

	cfg := DefaultConfig()
	if cfg.AttributionNamespaceLabel != "deployment_namespace" {
		t.Errorf("expected namespace label default to be 'deployment_namespace', got %q", cfg.AttributionNamespaceLabel)
	}
	if cfg.AttributionTenantLabel != "tenant_id" {
		t.Errorf("expected tenant label default to be 'tenant_id', got %q", cfg.AttributionTenantLabel)
	}
	if cfg.AttributionModelLabel != "serving_model" {
		t.Errorf("expected model label default to be 'serving_model', got %q", cfg.AttributionModelLabel)
	}
	if cfg.AttributionIdentitySentinel != "unattributed" {
		t.Errorf("expected identity sentinel default to be 'unattributed', got %q", cfg.AttributionIdentitySentinel)
	}
}

func TestEnv_TwoProducerMappings_MergedAndScoped(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "attribution.yaml")
	configYAML := "producers:\n" +
		"  - name: litellm-gateway\n" +
		"    inputTokensSeries: litellm_in\n" +
		"    outputTokensSeries: litellm_out\n" +
		"    tenantLabel: team_alias\n" +
		"    workloadLabel: \"\"\n" +
		"    modelLabel: model\n" +
		"    namespaceLabel: deployment_namespace\n" +
		"    scope:\n" +
		"      servingModels:\n" +
		"        - gemma-4-31B\n" +
		"  - name: llm-d-coordinator\n" +
		"    inputTokensSeries: llmd_in\n" +
		"    outputTokensSeries: llmd_out\n" +
		"    tenantLabel: tenant_id\n" +
		"    workloadLabel: workload_id\n" +
		"    modelLabel: model_name\n" +
		"    namespaceLabel: namespace\n" +
		"    scope:\n" +
		"      servingModels:\n" +
		"        - llama-3-70b\n"

	if err := os.WriteFile(configPath, []byte(configYAML), 0644); err != nil {
		t.Fatalf("writing config: %v", err)
	}

	cfg := baseConfig()
	cfg.AttributionEnabled = true
	cfg.AttributionConfigPath = configPath

	c := &Collector{config: cfg}

	mcGemma := makeTestModelCost("gemma-4-31B", "llm-prod", 500000, 500000, 10.0, 5.0, 10.0, 10.0, 5.0, 5.0)
	mcLlama := makeTestModelCost("llama-3-70b", "llm-prod", 1000000, 1000000, 20.0, 10.0, 10.0, 10.0, 5.0, 5.0)

	// One tenant ("acme-corp") is served on both gemma (via litellm-gateway) and llama (via coordinator)
	// Also litellm-gateway emits a llama-3-70b row which should be dropped by scope
	dimTokens := &source.InferenceDimensionResult{
		Values: map[source.InferenceDimensionKey]*source.InferenceDimensionTokens{
			{
				Producer:     "litellm-gateway",
				TenantID:     "acme-corp",
				ServingModel: "gemma-4-31B",
				Namespace:    "llm-prod",
			}: {
				PromptTokens:     200000,
				CompletionTokens: 200000,
			},
			{
				Producer:     "litellm-gateway",
				TenantID:     "acme-corp",
				ServingModel: "llama-3-70b", // Out of scope for litellm-gateway
				Namespace:    "llm-prod",
			}: {
				PromptTokens:     999999,
				CompletionTokens: 999999,
			},
			{
				Producer:     "llm-d-coordinator",
				TenantID:     "acme-corp",
				WorkloadID:   "w1",
				ServingModel: "llama-3-70b",
				Namespace:    "llm-prod",
			}: {
				PromptTokens:     400000,
				CompletionTokens: 400000,
			},
		},
	}

	results, _ := c.BuildDimensionCosts([]*InferenceCost{mcGemma, mcLlama}, dimTokens)

	var acmeGemma, acmeLlama *InferenceCost
	for _, r := range results {
		if r.Properties.TenantID == "acme-corp" {
			if r.Properties.ModelName == "gemma-4-31B" {
				acmeGemma = r
			} else if r.Properties.ModelName == "llama-3-70b" {
				acmeLlama = r
			}
		}
	}

	if acmeGemma == nil || acmeLlama == nil {
		t.Fatalf("expected acme-corp rows for both models: gemma=%v llama=%v", acmeGemma, acmeLlama)
	}

	// Gemma: 200k in, 200k out
	if acmeGemma.PromptTokens != 200000 || acmeGemma.GenerationTokens != 200000 {
		t.Errorf("acmeGemma tokens want 200k, got prompt=%f gen=%f", acmeGemma.PromptTokens, acmeGemma.GenerationTokens)
	}

	// Llama: 400k in, 400k out (out of scope row from gateway was dropped)
	if acmeLlama.PromptTokens != 400000 || acmeLlama.GenerationTokens != 400000 {
		t.Errorf("acmeLlama tokens want 400k (scoped to coordinator), got prompt=%f gen=%f", acmeLlama.PromptTokens, acmeLlama.GenerationTokens)
	}
}
