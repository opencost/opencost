package inferencecost

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

// Collector collects inference metrics from Prometheus
type Collector struct {
	promClient v1.API
	config     *Config
}

// NewCollector creates a new inference cost collector
func NewCollector(config *Config) (*Collector, error) {
	client, err := api.NewClient(api.Config{
		Address: config.PrometheusURL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Prometheus client: %w", err)
	}

	return &Collector{
		promClient: v1.NewAPI(client),
		config:     config,
	}, nil
}

// CollectMetrics queries Prometheus and calculates inference costs
func (c *Collector) CollectMetrics(ctx context.Context) ([]*ModelMetrics, error) {
	// Query token metrics from vLLM (grouped by model_name and namespace)
	promptTokens, err := c.queryPromptTokens(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query prompt tokens: %w", err)
	}
	log.Infof("Collected prompt tokens for %d model/namespace combinations", len(promptTokens))

	generationTokens, err := c.queryGenerationTokens(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query generation tokens: %w", err)
	}
	log.Infof("Collected generation tokens for %d model/namespace combinations", len(generationTokens))

	// Query GPU costs from OpenCost allocation (grouped by model_name and namespace)
	gpuCosts, err := c.queryGPUCosts(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query GPU costs: %w", err)
	}
	log.Infof("Collected GPU costs for %d model/namespace combinations", len(gpuCosts))
	for key, cost := range gpuCosts {
		log.Infof("GPU cost for %s: $%.6f/hour", key, cost)
	}

	// Combine metrics by model and namespace
	metrics := c.combineMetrics(promptTokens, generationTokens, gpuCosts)

	return metrics, nil
}

// queryPromptTokens queries vLLM prompt token metrics grouped by model_name and namespace
func (c *Collector) queryPromptTokens(ctx context.Context) (map[string]float64, error) {
	query := `sum by (model_name, namespace) (rate(vllm:prompt_tokens_total[5m]) * 300)`
	result, _, err := c.promClient.Query(ctx, query, time.Now())
	if err != nil {
		return nil, err
	}

	// Parse result and return map[model_name:namespace]tokens
	return parsePrometheusResult(result)
}

// queryGenerationTokens queries vLLM generation token metrics grouped by model_name and namespace
func (c *Collector) queryGenerationTokens(ctx context.Context) (map[string]float64, error) {
	query := `sum by (model_name, namespace) (rate(vllm:generation_tokens_total[5m]) * 300)`
	result, _, err := c.promClient.Query(ctx, query, time.Now())
	if err != nil {
		return nil, err
	}

	return parsePrometheusResult(result)
}

// queryGPUCosts queries GPU costs from OpenCost allocation grouped by model_name and namespace
func (c *Collector) queryGPUCosts(ctx context.Context) (map[string]float64, error) {
	// Calculate GPU costs by joining node costs with container allocations
	// Since opencost_allocation_gpu_cost doesn't exist, we need to calculate it ourselves
	//
	// Strategy:
	// 1. Calculate per-container GPU cost: container_gpu_allocation * node_gpu_hourly_cost
	// 2. Aggregate by pod/namespace/node to remove container label (needed for join)
	// 3. Join with vLLM metrics by pod/namespace to get model_name
	// 4. Aggregate by model_name and namespace
	//
	// Formula: pod_gpu_cost = sum(container_gpu_allocation * node_gpu_hourly_cost) by pod
	query := `
		sum by (model_name, namespace) (
			# Start with vLLM metrics to get model_name, multiply by 0 and add 1 to get a constant 1 with labels
			(vllm:prompt_tokens_total * 0 + 1)
			* on(pod, namespace) group_left()
			# Join with aggregated GPU cost per pod (removes container label)
			sum(
				container_gpu_allocation
				* on(node) group_left()
				node_gpu_hourly_cost
			) by (pod, namespace, node)
		)
	`
	result, _, err := c.promClient.Query(ctx, query, time.Now())
	if err != nil {
		return nil, err
	}

	return parsePrometheusResult(result)
}

// combineMetrics combines token and cost metrics by model and namespace
func (c *Collector) combineMetrics(
	promptTokens map[string]float64,
	generationTokens map[string]float64,
	gpuCosts map[string]float64,
) []*ModelMetrics {
	metricsMap := make(map[string]*ModelMetrics)

	// Combine all metrics by model name and namespace
	// Key format: "model_name:namespace"
	for key := range promptTokens {
		if _, exists := metricsMap[key]; !exists {
			modelName, namespace := parseKey(key)
			metricsMap[key] = &ModelMetrics{
				ModelName: modelName,
				Namespace: namespace,
				Timestamp: time.Now(),
			}
		}
		metricsMap[key].PromptTokens = promptTokens[key]
	}

	for key := range generationTokens {
		if _, exists := metricsMap[key]; !exists {
			modelName, namespace := parseKey(key)
			metricsMap[key] = &ModelMetrics{
				ModelName: modelName,
				Namespace: namespace,
				Timestamp: time.Now(),
			}
		}
		metricsMap[key].GenerationTokens = generationTokens[key]
	}

	for key := range gpuCosts {
		if _, exists := metricsMap[key]; !exists {
			modelName, namespace := parseKey(key)
			metricsMap[key] = &ModelMetrics{
				ModelName: modelName,
				Namespace: namespace,
				Timestamp: time.Now(),
			}
		}
		metricsMap[key].GPUCost = gpuCosts[key]
		metricsMap[key].TotalCost = gpuCosts[key] // For PoC, total = GPU cost
	}

	// Convert map to slice
	metrics := make([]*ModelMetrics, 0, len(metricsMap))
	for _, m := range metricsMap {
		m.TotalTokens = m.PromptTokens + m.GenerationTokens
		metrics = append(metrics, m)
	}

	return metrics
}

// parseKey parses "model_name:namespace" key format
func parseKey(key string) (modelName, namespace string) {
	parts := strings.Split(key, ":")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return key, "unknown"
}

// parsePrometheusResult parses Prometheus query result into map[model_name:namespace]value
func parsePrometheusResult(result model.Value) (map[string]float64, error) {
	resultMap := make(map[string]float64)

	// Check if result is a vector
	vector, ok := result.(model.Vector)
	if !ok {
		log.Warnf("Prometheus query result is not a vector, got type: %T", result)
		return resultMap, nil
	}

	// Parse each sample in the vector
	for _, sample := range vector {
		// Extract model_name and namespace labels
		modelName := string(sample.Metric["model_name"])
		namespace := string(sample.Metric["namespace"])

		if modelName == "" {
			log.Warnf("Sample missing model_name label: %v", sample.Metric)
			continue
		}

		if namespace == "" {
			log.Warnf("Sample missing namespace label: %v", sample.Metric)
			namespace = "unknown"
		}

		// Create key in format "model_name:namespace"
		key := fmt.Sprintf("%s:%s", modelName, namespace)

		// Get the value
		value := float64(sample.Value)

		resultMap[key] = value
	}

	return resultMap, nil
}

// Made with Bob
