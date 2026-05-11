
# Phase 1: Proof of Concept - Detailed Design

**Goal:** Implement two core inference cost metrics to demonstrate end-to-end cost calculation.

**Timeline:** Weeks 1-2  
**Status:** Design Document  
**Date:** 2026-05-03

---

## Table of Contents

1. [Overview](#1-overview)
2. [Metrics to Implement](#2-metrics-to-implement)
3. [Architecture](#3-architecture)
4. [OpenCost Changes](#4-opencost-changes)
5. [llm-d Changes](#5-llm-d-changes)
6. [Data Flow](#6-data-flow)
7. [Implementation Steps](#7-implementation-steps)
8. [Testing Strategy](#8-testing-strategy)
9. [Success Criteria](#9-success-criteria)

---

## 1. Overview

### Objectives

1. Export two Prometheus metrics from OpenCost:
   - `opencost_inference_total_cost{model_name, model_version, namespace}`
   - `opencost_inference_cost_per_million_tokens{model_name, model_version, namespace}`

2. Calculate costs from existing infrastructure and token metrics

3. Support multiple llm-d deployments and independent vLLM deployments via namespace label

4. Demonstrate feasibility of the approach

### Scope

**In Scope:**
- Basic cost calculation from GPU utilization
- Token counting from vLLM metrics
- Prometheus metric export with namespace label
- Per-namespace cost tracking
- Simple Grafana dashboard

**Out of Scope (Future Phases):**
- Cache-aware cost calculation
- Multi-dimensional aggregation (team, workload)
- API endpoints
- MCP server integration
- Network costs
- P/D disaggregation costs

### Dependencies

**Existing Metrics (Already Available):**
- `vllm:prompt_tokens_total{model_name, namespace}` - From vLLM
- `vllm:generation_tokens_total{model_name, namespace}` - From vLLM
- `DCGM_FI_DEV_GPU_UTIL` or `nvidia_gpu_duty_cycle` - From NVIDIA DCGM
- OpenCost allocation data (GPU costs per pod)

---

## 2. Metrics to Implement

### 2.1 opencost_inference_total_cost

**Type:** Gauge  
**Labels:**
- `model_name` (e.g., "llama3-8b-instruct")
- `model_version` (e.g., "v1.0")
- `namespace` (e.g., "llm-d-prod")

**Description:** Total infrastructure cost attributed to inference for a specific model in a specific namespace.

**Calculation:**
```
opencost_inference_total_cost = Sum of GPU costs for all pods running the model in the namespace
```

**Update Frequency:** Every 60 seconds

**Example:**
```
opencost_inference_total_cost{model_name="llama3-8b-instruct",model_version="v1.0",namespace="llm-d-prod"} 125.50
opencost_inference_total_cost{model_name="llama3-8b-instruct",model_version="v1.0",namespace="llm-d-staging"} 45.20
```

**Prometheus Query Examples:**
```promql
# Total cost across all namespaces for a model
sum by (model_name) (opencost_inference_total_cost{model_name="llama3-8b-instruct"})

# Cost for specific namespace
opencost_inference_total_cost{model_name="llama3-8b-instruct",namespace="llm-d-prod"}

# Cost breakdown by namespace
sum by (namespace) (opencost_inference_total_cost)
```

### 2.2 opencost_inference_cost_per_million_tokens

**Type:** Gauge  
**Labels:**
- `model_name` (e.g., "llama3-8b-instruct")
- `model_version` (e.g., "v1.0")
- `namespace` (e.g., "llm-d-prod")

**Description:** Cost per 1 million tokens processed (input + output) for a specific model in a specific namespace.

**Calculation:**
```
Total Tokens (per namespace) = sum(rate(vllm:prompt_tokens_total{namespace="X"}[5m])) + 
                               sum(rate(vllm:generation_tokens_total{namespace="X"}[5m]))
Cost Per Token = opencost_inference_total_cost{namespace="X"} / Total Tokens
Cost Per Million Tokens = Cost Per Token × 1,000,000
```

**Update Frequency:** Every 60 seconds

**Example:**
```
opencost_inference_cost_per_million_tokens{model_name="llama3-8b-instruct",model_version="v1.0",namespace="llm-d-prod"} 6.25
opencost_inference_cost_per_million_tokens{model_name="llama3-8b-instruct",model_version="v1.0",namespace="llm-d-staging"} 5.80
```

**Prometheus Query Examples:**
```promql
# Average cost per million tokens across all namespaces
avg by (model_name) (opencost_inference_cost_per_million_tokens{model_name="llama3-8b-instruct"})

# Cost per million tokens for specific namespace
opencost_inference_cost_per_million_tokens{model_name="llama3-8b-instruct",namespace="llm-d-prod"}

# Compare costs across namespaces
opencost_inference_cost_per_million_tokens{model_name="llama3-8b-instruct"}
```

---

## 3. Architecture

### 3.1 Component Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    llm-d Cluster                             │
│                                                              │
│  ┌──────────────┐         ┌──────────────┐                 │
│  │  vLLM Pods   │         │  DCGM/NVIDIA │                 │
│  │              │         │  GPU Metrics │                 │
│  │ namespace:   │         │              │                 │
│  │ llm-d-prod   │         │ GPU util     │                 │
│  │ model_name:  │         │              │                 │
│  │ llama3-8b    │         │              │                 │
│  └──────┬───────┘         └──────┬───────┘                 │
│         │                        │                          │
│         │ Token metrics          │ GPU metrics              │
│         │ (with namespace)       │                          │
│         │                        │                          │
│         └────────┬───────────────┘                          │
│                  │                                           │
│         ┌────────▼────────┐                                 │
│         │   Prometheus    │                                 │
│         │                 │                                 │
│         │ Stores:         │                                 │
│         │ - Token metrics │                                 │
│         │   (per ns)      │                                 │
│         │ - GPU metrics   │                                 │
│         └────────┬────────┘                                 │
└──────────────────┼──────────────────────────────────────────┘
                   │
                   │ PromQL queries
                   │
┌──────────────────▼──────────────────────────────────────────┐
│                    OpenCost                                  │
│                                                              │
│  ┌────────────────────────────────────────────────────┐    │
│  │  Inference Cost Collector (NEW)                    │    │
│  │                                                     │    │
│  │  1. Query vLLM token metrics from Prometheus       │    │
│  │     (grouped by model_name, namespace)             │    │
│  │  2. Query GPU utilization from Prometheus          │    │
│  │  3. Get GPU costs from OpenCost allocation         │    │
│  │     (grouped by model_name, namespace)             │    │
│  │  4. Calculate cost per token (per namespace)       │    │
│  │  5. Export Prometheus metrics with namespace label │    │
│  └────────────────────────────────────────────────────┘    │
│                           │                                  │
│                           │ Exports metrics                  │
│                           │                                  │
│                  ┌────────▼────────┐                         │
│                  │  /metrics       │                         │
│                  │  endpoint       │                         │
│                  └────────┬────────┘                         │
└───────────────────────────┼──────────────────────────────────┘
                            │
                            │ Scrapes metrics
                            │
                   ┌────────▼────────┐
                   │   Prometheus    │
                   │   (stores new   │
                   │    metrics)     │
                   └────────┬────────┘
                            │
                            │ Queries (can filter by namespace)
                            │
                   ┌────────▼────────┐
                   │    Grafana      │
                   │   Dashboard     │
                   │                 │
                   │ - Filter by ns  │
                   │ - Aggregate     │
                   │ - Compare       │
                   └─────────────────┘
```

### 3.2 Data Flow

1. **vLLM exports token metrics with namespace label** → Prometheus
2. **DCGM exports GPU metrics** → Prometheus
3. **OpenCost Inference Collector**:
   - Queries Prometheus for token metrics (grouped by model_name, namespace)
   - Queries Prometheus for GPU metrics
   - Gets GPU cost from OpenCost's existing allocation system (per namespace)
   - Calculates cost per token (per namespace)
   - Exports metrics with namespace label
4. **Prometheus scrapes** OpenCost's `/metrics` endpoint
5. **Grafana queries** Prometheus with namespace filtering/aggregation

---

## 4. OpenCost Changes

### 4.1 New Package Structure

```
opencost/pkg/inferencecost/
├── collector.go          # Prometheus metric collector
├── calculator.go         # Cost calculation logic
├── exporter.go          # Prometheus metric exporter
├── types.go             # Data structures
├── collector_test.go    # Unit tests
├── calculator_test.go   # Unit tests
└── exporter_test.go     # Unit tests
```

### 4.2 File: `opencost/pkg/inferencecost/types.go`

```go
package inferencecost

import "time"

// ModelMetrics holds metrics for a specific model in a specific namespace
type ModelMetrics struct {
    ModelName    string
    ModelVersion string
    Namespace    string
    
    // Token metrics
    PromptTokens     float64
    GenerationTokens float64
    TotalTokens      float64
    
    // Cost metrics
    GPUCost          float64
    TotalCost        float64
    
    // Calculated metrics
    CostPerToken           float64
    CostPerMillionTokens   float64
    
    // Metadata
    Timestamp time.Time
}

// Config holds configuration for the inference cost collector
type Config struct {
    PrometheusURL      string
    CollectionInterval time.Duration
    Enabled            bool
}
```

### 4.3 File: `opencost/pkg/inferencecost/collector.go`

```go
package inferencecost

import (
    "context"
    "fmt"
    "strings"
    "time"
    
    "github.com/prometheus/client_golang/api"
    v1 "github.com/prometheus/client_golang/api/prometheus/v1"
    "github.com/opencost/opencost/core/pkg/log"
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
    
    generationTokens, err := c.queryGenerationTokens(ctx)
    if err != nil {
        return nil, fmt.Errorf("failed to query generation tokens: %w", err)
    }
    
    // Query GPU costs from OpenCost allocation (grouped by model_name and namespace)
    gpuCosts, err := c.queryGPUCosts(ctx)
    if err != nil {
        return nil, fmt.Errorf("failed to query GPU costs: %w", err)
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
    // Query OpenCost's existing allocation metrics
    // This gets GPU cost per pod, which we'll aggregate by model_name and namespace
    query := `sum by (model_name, namespace) (opencost_allocation_gpu_cost)`
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

// parsePrometheusResult parses Prometheus query result
func parsePrometheusResult(result model.Value) (map[string]float64, error) {
    // Implementation to parse Prometheus result into map[model_name:namespace]value
    // Key format: "model_name:namespace"
    // This is a helper function that would parse the Prometheus vector result
    return nil, nil
}
```

### 4.4 File: `opencost/pkg/inferencecost/calculator.go`

```go
package inferencecost

import "fmt"

// Calculator calculates inference costs
type Calculator struct{}

// NewCalculator creates a new cost calculator
func NewCalculator() *Calculator {
    return &Calculator{}
}

// CalculateCosts calculates cost metrics for each model/namespace combination
func (c *Calculator) CalculateCosts(metrics []*ModelMetrics) error {
    for _, m := range metrics {
        if err := c.calculateModelCosts(m); err != nil {
            return fmt.Errorf("failed to calculate costs for model %s in namespace %s: %w", 
                m.ModelName, m.Namespace, err)
        }
    }
    return nil
}

// calculateModelCosts calculates costs for a single model/namespace
func (c *Calculator) calculateModelCosts(m *ModelMetrics) error {
    // Avoid division by zero
    if m.TotalTokens == 0 {
        m.CostPerToken = 0
        m.CostPerMillionTokens = 0
        return nil
    }
    
    // Calculate cost per token
    m.CostPerToken = m.TotalCost / m.TotalTokens
    
    // Calculate cost per million tokens
    m.CostPerMillionTokens = m.CostPerToken * 1000000
    
    return nil
}
```

### 4.5 File: `opencost/pkg/inferencecost/exporter.go`

```go
package inferencecost

import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/opencost/opencost/core/pkg/log"
)

// Exporter exports inference cost metrics to Prometheus
type Exporter struct {
    totalCost          *prometheus.GaugeVec
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
```

### 4.6 Integration into OpenCost Main

**File:** `opencost/cmd/costmodel/main.go`

Add initialization of inference cost collector:

```go
// Add to imports
import (
    "github.com/opencost/opencost/pkg/inferencecost"
)

// Add to main() function
func main() {
    // ... existing code ...
    
    // Initialize inference cost collector if enabled
    if env.GetInferenceCostEnabled() {
        inferenceConfig := &inferencecost.Config{
            PrometheusURL:      env.GetPrometheusServerEndpoint(),
            CollectionInterval: 60 * time.Second,
            Enabled:            true,
        }
        
        collector, err := inferencecost.NewCollector(inferenceConfig)
        if err != nil {
            log.Errorf("Failed to create inference cost collector: %v", err)
        } else {
            calculator := inferencecost.NewCalculator()
            exporter := inferencecost.NewExporter()
            
            if err := exporter.Register(); err != nil {
                log.Errorf("Failed to register inference cost metrics: %v", err)
            } else {
                // Start collection loop
                go runInferenceCostCollection(collector, calculator, exporter, inferenceConfig)
                log.Infof("Inference cost collection enabled")
            }
        }
    }
    
    // ... existing code ...
}

func runInferenceCostCollection(
    collector *inferencecost.Collector,
    calculator *inferencecost.Calculator,
    exporter *inferencecost.Exporter,
    config *inferencecost.Config,
) {
    ticker := time.NewTicker(config.CollectionInterval)
    defer ticker.Stop()
    
    for range ticker.C {
        ctx := context.Background()
        
        // Collect metrics (per namespace)
        metrics, err := collector.CollectMetrics(ctx)
        if err != nil {
            log.Errorf("Failed to collect inference metrics: %v", err)
            continue
        }
        
        // Calculate costs (per namespace)
        if err := calculator.CalculateCosts(metrics); err != nil {
            log.Errorf("Failed to calculate inference costs: %v", err)
            continue
        }
        
        // Export to Prometheus (with namespace label)
        exporter.Export(metrics)
    }
}
```

### 4.7 Environment Variables

**File:** `opencost/pkg/env/costmodel.go`

Add new environment variables:

```go
// GetInferenceCostEnabled returns whether inference cost tracking is enabled
func GetInferenceCostEnabled() bool {
    return GetBool("INFERENCE_COST_ENABLED", false)
}

// GetInferenceCostCollectionInterval returns the collection interval in seconds
func GetInferenceCostCollectionInterval() int {
    return GetInt("INFERENCE_COST_COLLECTION_INTERVAL", 60)
}
```

---

## 5. llm-d Changes

### 5.1 Required Changes

**Minimal changes needed** - llm-d already exports the required metrics with namespace labels.

### 5.2 Verification Steps

Verify that vLLM pods are exporting metrics with `model_name` and `namespace` labels:

```bash
# Check if metrics are available
kubectl port-forward -n llm-d-prod <vllm-pod> 8000:8000
curl http://localhost:8000/metrics | grep vllm:prompt_tokens_total
curl http://localhost:8000/metrics | grep vllm:generation_tokens_total
```

Expected output:
```
vllm:prompt_tokens_total{model_name="llama3-8b-instruct",namespace="llm-d-prod"} 50000000
vllm:generation_tokens_total{model_name="llama3-8b-instruct",namespace="llm-d-prod"} 25000000
```

**Note:** The `namespace` label is typically added by Prometheus during scraping based on the pod's namespace. Verify this in your Prometheus configuration.

### 5.3 Optional Enhancement: Add model_version Label

If `model_version` label is not present in vLLM metrics, add it via pod labels:

**File:** `llm-d/guides/inference-scheduling/values.yaml` (or similar)

```yaml
decode:
  podLabels:
    model_name: "llama3-8b-instruct"
    model_version: "v1.0"
```

This allows OpenCost to extract `model_version` from pod labels if not available in metrics.

### 5.4 Prometheus ServiceMonitor

Ensure Prometheus is scraping vLLM metrics and adding namespace label:

**File:** `llm-d/docs/monitoring/prometheus-rules/vllm-scrape.yaml`

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: vllm-metrics
  namespace: llm-d
spec:
  selector:
    matchLabels:
      app: vllm
  endpoints:
  - port: metrics
    interval: 15s
    path: /metrics
    # Prometheus automatically adds namespace label based on pod namespace
```

---

## 6. Data Flow

### 6.1 Detailed Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│ Step 1: vLLM exports metrics in multiple namespaces         │
│                                                              │
│ Namespace: llm-d-prod                                       │
│   vllm:prompt_tokens_total{model_name="llama3-8b"} 50M     │
│   vllm:generation_tokens_total{model_name="llama3-8b"} 25M │
│                                                              │
│ Namespace: llm-d-staging                                    │
│   vllm:prompt_tokens_total{model_name="llama3-8b"} 18M     │
│   vllm:generation_tokens_total{model_name="llama3-8b"} 9M  │
└────────────────────────┬────────────────────────────────────┘
                         │
                         │ Prometheus scrapes and adds namespace label
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ Step 2: Prometheus stores metrics with namespace label      │
│                                                              │
│ vllm:prompt_tokens_total{model_name="llama3-8b",           │
│                          namespace="llm-d-prod"} 50M        │
│ vllm:prompt_tokens_total{model_name="llama3-8b",           │
│                          namespace="llm-d-staging"} 18M     │
└────────────────────────┬────────────────────────────────────┘
                         │
                         │ OpenCost queries with grouping by namespace
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ Step 3: OpenCost Collector queries per namespace            │
│                                                              │
│ Query: sum by (model_name, namespace)                       │
│        (rate(vllm:prompt_tokens_total[5m]))                 │
│                                                              │
│ Results:                                                     │
│   {model_name="llama3-8b", namespace="llm-d-prod"}: 50M    │
│   {model_name="llama3-8b", namespace="llm-d-staging"}: 18M │
└────────────────────────┬────────────────────────────────────┘
                         │
                         │ Calculate costs per namespace
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ Step 4: Calculator computes costs per namespace             │
│                                                              │
│ llm-d-prod:                                                 │
│   total_tokens = 75M                                        │
│   gpu_cost = $125.50                                        │
│   cost_per_1m_tokens = $1.67                                │
│                                                              │
│ llm-d-staging:                                              │
│   total_tokens = 27M                                        │
│   gpu_cost = $45.20                                         │
│   cost_per_1m_tokens = $1.67                                │
└────────────────────────┬────────────────────────────────────┘
                         │
                         │ Export with namespace label
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ Step 5: Exporter creates metrics with namespace label       │
│                                                              │
│ opencost_inference_total_cost{                              │
│   model_name="llama3-8b",                                   │
│   model_version="v1.0",                                     │
│   namespace="llm-d-prod"} 125.50                            │
│                                                              │
│ opencost_inference_total_cost{                              │
│   model_name="llama3-8b",                                   │
│   model_version="v1.0",                                     │
│   namespace="llm-d-staging"} 45.20                          │
└────────────────────────┬────────────────────────────────────┘
                         │
                         │ Prometheus scrapes
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ Step 6: Users query with namespace filtering/aggregation    │
│                                                              │
│ # Total across all namespaces                               │
│ sum(opencost_inference_total_cost{                          │
│   model_name="llama3-8b"}) = $170.70                        │
│                                                              │
│ # Production only                                           │
│ opencost_inference_total_cost{                              │
│   model_name="llama3-8b",                                   │
│   namespace="llm-d-prod"} = $125.50                         │
└─────────────────────────────────────────────────────────────┘
```

### 6.2 Example Calculation

**Input Data (from Prometheus):**

**llm-d-prod namespace:**
- Model: `llama3-8b-instruct`
- Prompt tokens (5m rate): 50,000,000 tokens
- Generation tokens (5m rate): 25,000,000 tokens
- GPU cost (from OpenCost): $125.50

**llm-d-staging namespace:**
- Model: `llama3-8b-instruct`
- Prompt tokens (5m rate): 18,000,000 tokens
- Generation tokens (5m rate): 9,000,000 tokens
- GPU cost (from OpenCost): $45.20

**Calculation (per namespace):**

**llm-d-prod:**
```
Total Tokens = 50,000,000 + 25,000,000 = 75,000,000 tokens
Cost Per Token = $125.50 / 75,000,000 = $0.00000167 per token
Cost Per Million Tokens = $0.00000167 × 1,000,000 = $1.67
```

**llm-d-staging:**
```
Total Tokens = 18,000,000 + 9,000,000 = 27,000,000 tokens
Cost Per Token = $45.20 / 27,000,000 = $0.00000167 per token
Cost Per Million Tokens = $0.00000167 × 1,000,000 = $1.67
```

**Output Metrics:**
```
opencost_inference_total_cost{model_name="llama3-8b-instruct",model_version="v1.0",namespace="llm-d-prod"} 125.50
opencost_inference_total_cost{model_name="llama3-8b-instruct",model_version="v1.0",namespace="llm-d-staging"} 45.20

opencost_inference_cost_per_million_tokens{model_name="llama3-8b-instruct",model_version="v1.0",namespace="llm-d-prod"} 1.67
opencost_inference_cost_per_million_tokens{model_name="llama3-8b-instruct",model_version="v1.0",namespace="llm-d-staging"} 1.67
```

---

## 7. Implementation Steps

### Week 1: Core Implementation

#### Day 1-2: Package Setup and Types
- [ ] Create `opencost/pkg/inferencecost/` package
- [ ] Implement `types.go` with data structures (including namespace field)
- [ ] Add environment variables to `pkg/env/costmodel.go`
- [ ] Write unit tests for types

#### Day 3-4: Collector Implementation
- [ ] Implement `collector.go` with Prometheus queries (grouped by namespace)
- [ ] Test Prometheus queries against real llm-d cluster
- [ ] Handle error cases (missing metrics, connection failures)
- [ ] Write unit tests with mock Prometheus client

#### Day 5: Calculator Implementation
- [ ] Implement `calculator.go` with cost calculation logic (per namespace)
- [ ] Handle edge cases (zero tokens, missing data)
- [ ] Write unit tests with various scenarios

### Week 2: Integration and Testing

#### Day 6-7: Exporter and Integration
- [ ] Implement `exporter.go` with Prometheus metric export (with namespace label)
- [ ] Integrate into `cmd/costmodel/main.go`
- [ ] Test metric export locally
- [ ] Write integration tests

#### Day 8: Deployment and Validation
- [ ] Deploy OpenCost with inference cost enabled
- [ ] Verify metrics appear in Prometheus with namespace labels
- [ ] Check metric values are reasonable per namespace
- [ ] Test Prometheus queries with namespace filtering
- [ ] Debug any issues

#### Day 9: Grafana Dashboard
- [ ] Create Grafana dashboard with namespace variable
- [ ] Add panels for both metrics with namespace filtering
- [ ] Add panels for cross-namespace aggregation
- [ ] Test dashboard with real data

#### Day 10: Documentation and Cleanup
- [ ] Write README for inference cost package
- [ ] Document configuration options
- [ ] Create troubleshooting guide
- [ ] Document Prometheus query examples
- [ ] Code review and cleanup

---

## 8. Testing Strategy

### 8.1 Unit Tests

**File:** `opencost/pkg/inferencecost/calculator_test.go`

```go
package inferencecost

import (
    "testing"
)

func TestCalculateCosts(t *testing.T) {
    tests := []struct {
        name           string
        metrics        *ModelMetrics
        expectedCostPer1M float64
    }{
        {
            name: "normal case - prod namespace",
            metrics: &ModelMetrics{
                ModelName:        "llama3-8b",
                Namespace:        "llm-d-prod",
                TotalTokens:      75000000,
                TotalCost:        125.50,
            },
            expectedCostPer1M: 1.67,
        },
        {
            name: "normal case - staging namespace",
            metrics: &ModelMetrics{
                ModelName:        "llama3-8b",
                Namespace:        "llm-d-staging",
                TotalTokens:      27000000,
                TotalCost:        45.20,
            },
            expectedCostPer1M: 1.67,
        },
        {
            name: "zero tokens",
            metrics: &ModelMetrics{
                ModelName:        "llama3-8b",
                Namespace:        "llm-d-prod",
                TotalTokens:      0,
                TotalCost:        100.00,
            },
            expectedCostPer1M: 0,
        },
    }
    
    calc := NewCalculator()
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            err := calc.calculateModelCosts(tt.metrics)
            if err != nil {
                t.Fatalf("unexpected error: %v", err)
            }
            
            if abs(tt.metrics.CostPerMillionTokens - tt.expectedCostPer1M) > 0.01 {
                t.Errorf("expected cost per 1M tokens %.2f, got %.2f",
                    tt.expectedCostPer1M, tt.metrics.CostPerMillionTokens)
            }
        })
    }
}

func abs(x float64) float64 {
    if x < 0 {
        return -x
    }
    return x
}
```

### 8.2 Integration Tests

**File:** `opencost/pkg/inferencecost/integration_test.go`

```go
// +build integration

package inferencecost

import (
    "context"
    "testing"
    "time"
)

func TestEndToEndCollection(t *testing.T) {
    // Requires running Prometheus with test data
    config := &Config{
        PrometheusURL:      "http://localhost:9090",
        CollectionInterval: 60 * time.Second,
        Enabled:            true,
    }
    
    collector, err := NewCollector(config)
    if err != nil {
        t.Fatalf("failed to create collector: %v", err)
    }
    
    ctx := context.Background()
    metrics, err := collector.CollectMetrics(ctx)
    if err != nil {
        t.Fatalf("failed to collect metrics: %v", err)
    }
    
    if len(metrics) == 0 {
        t.Error("expected at least one model metric")
    }
    
    // Verify namespace labels are present
    namespacesSeen := make(map[string]bool)
    for _, m := range metrics {
        if m.ModelName == "" {
            t.Error("model name should not be empty")
        }
        if m.Namespace == "" {
            t.Error("namespace should not be empty")
        }
        if m.TotalTokens < 0 {
            t.Error("total tokens should not be negative")
        }
        namespacesSeen[m.Namespace] = true
    }
    
    // Should have metrics from multiple namespaces if they exist
    t.Logf("Found metrics from %d namespace(s)", len(namespacesSeen))
}
```

### 8.3 Manual Testing Checklist

**Prometheus Metrics:**
- [ ] Deploy OpenCost with `INFERENCE_COST_ENABLED=true`
- [ ] Verify OpenCost pod starts successfully
- [ ] Check OpenCost logs for inference cost collection messages
- [ ] Query Prometheus for new metrics:
  ```promql
  opencost_inference_total_cost
  opencost_inference_cost_per_million_tokens
  ```
- [ ] Verify metrics have correct labels (model_name, model_version, namespace)
- [ ] Verify metric values are reasonable (non-zero, positive) per namespace
- [ ] Check metrics update every 60 seconds
- [ ] Test with multiple models running simultaneously
- [ ] Test with multiple namespaces
- [ ] Test behavior when vLLM pods are stopped/started

**Namespace Filtering:**
- [ ] Query metrics for specific namespace:
  ```promql
  opencost_inference_total_cost{namespace="llm-d-prod"}
  ```
- [ ] Query aggregated across all namespaces:
  ```promql
  sum by (model_name) (opencost_inference_total_cost)
  ```
- [ ] Query comparison across namespaces:
  ```promql
  opencost_inference_total_cost{model_name="llama3-8b"}
  ```
- [ ] Verify each namespace has independent cost calculations

---

## 9. Success Criteria

### 9.1 Functional Requirements

- [ ] Two Prometheus metrics are exported by OpenCost with namespace label
- [ ] Metrics include `model_name`, `model_version`, and `namespace` labels
- [ ] Metrics are calculated independently per namespace
- [ ] Metrics update every 60 seconds
- [ ] Cost calculations are accurate (within 5% of expected) per namespace
- [ ] System handles missing metrics gracefully
- [ ] No performance impact on OpenCost (< 1% CPU increase)
- [ ] Users can filter by namespace in Prometheus queries
- [ ] Users can aggregate across namespaces in Prometheus queries

### 9.2 Quality Requirements

- [ ] Unit test coverage > 80%
- [ ] Integration tests pass
- [ ] Code passes linting (golangci-lint)
- [ ] Documentation is complete
- [ ] No memory leaks (tested over 24 hours)

### 9.3 Operational Requirements

- [ ] Metrics visible in Prometheus within 2 minutes of deployment
- [ ] Grafana dashboard displays metrics correctly with namespace filtering
- [ ] Error handling logs useful debugging information
- [ ] Configuration via environment variables works
- [ ] Can be disabled via `INFERENCE_COST_ENABLED=false`

### 9.4 Acceptance Criteria

**Scenario 1:** Deploy llm-d with llama3-8b-instruct model in multiple namespaces

**Given:**
- vLLM is running and processing requests in `llm-d-prod` and `llm-d-staging`
- Prometheus is scraping vLLM metrics
- OpenCost is deployed with inference cost enabled

**When:**
- Wait 2 minutes for metrics to populate

**Then:**
- Query Prometheus: `opencost_inference_total_cost{model_name="llama3-8b-instruct"}`
- Result shows metrics for both namespaces:
  ```
  opencost_inference_total_cost{model_name="llama3-8b-instruct",model_version="v1.0",namespace="llm-d-prod"} 125.50
  opencost_inference_total_cost{model_name="llama3-8b-instruct",model_version="v1.0",namespace="llm-d-staging"} 45.20
  ```
- Query for specific namespace works:
  ```promql
  opencost_inference_total_cost{namespace="llm-d-prod"}
  ```
- Aggregation across namespaces works:
  ```promql
  sum by (model_name) (opencost_inference_total_cost{model_name="llama3-8b-instruct"})
  # Returns: 170.70
  ```
- Grafana dashboard displays both metrics with namespace filtering
- Metrics update every 60 seconds

**Scenario 2:** Namespace isolation

**Given:**
- Same model running in two namespaces with different load

**When:**
- Production namespace processes 3x more tokens than staging

**Then:**
- Production namespace shows 3x higher total cost
- Cost per million tokens is similar (within 10%) between namespaces
- Metrics are independently calculated per namespace

---

## 10. Configuration

### 10.1 OpenCost Helm Values

**File:** `opencost-helm-chart/values.yaml`

```yaml
opencost:
  exporter:
    extraEnv:
      INFERENCE_COST_ENABLED: "true"
      INFERENCE_COST_COLLECTION_INTERVAL: "60"
```

### 10.2 Deployment Command

```bash
helm install opencost opencost/opencost \
  --set opencost.exporter.extraEnv.INFERENCE_COST_ENABLED=true \
  --set opencost.exporter.extraEnv.INFERENCE_COST_COLLECTION_INTERVAL=60
```

---

## 11. Grafana Dashboard

### 11.1 Dashboard with Namespace Variable

**File:** `opencost/docs/monitoring/grafana/dashboards/inference-cost-poc.json`

```json
{
  "dashboard": {
    "title": "Inference Cost PoC",
    "templating": {
      "list": [
        {
          "name": "namespace",
          "type": "query",
          "query": "label_values(opencost_inference_total_cost, namespace)",
          "multi": true,
          "includeAll": true
        }
      ]
    },
    "panels": [
      {
        "title": "Total Inference Cost by Model and Namespace",
        "targets": [
          {
            "expr": "opencost_inference_total_cost{namespace=~\"$namespace\"}",
            "legendFormat": "{{model_name}} ({{namespace}})"
          }
        ],
        "type": "graph"
      },
      {
        "title": "Cost per 1M Tokens by Model and Namespace",
        "targets": [
            "expr": "opencost_inference_cost_per_million_tokens{namespace=~\"$namespace\"}",
            "legendFormat": "{{model_name}} ({{namespace}})"
          }
        ],
        "type": "graph"
      },
      {
        "title": "Total Cost Across All Namespaces",
        "targets": [
          {
            "expr": "sum by (model_name) (opencost_inference_total_cost)",
            "legendFormat": "{{model_name}} (all namespaces)"
          }
        ],
        "type": "graph"
      },
      {
        "title": "Cost Comparison by Namespace",
        "targets": [
          {
            "expr": "opencost_inference_total_cost{model_name=\"llama3-8b-instruct\"}",
            "legendFormat": "{{namespace}}"
          }
        ],
        "type": "graph"
      }
    ]
  }
}
```

### 11.2 Dashboard Setup

1. Import dashboard JSON into Grafana
2. Select Prometheus data source
3. Use namespace variable dropdown to filter
4. Set refresh interval to 1 minute

### 11.3 Query Examples for Dashboard

**Filter by single namespace:**
```promql
opencost_inference_total_cost{namespace="llm-d-prod"}
```

**Filter by multiple namespaces:**
```promql
opencost_inference_total_cost{namespace=~"llm-d-prod|llm-d-staging"}
```

**Aggregate across all namespaces:**
```promql
sum by (model_name) (opencost_inference_total_cost)
```

**Compare namespaces side-by-side:**
```promql
opencost_inference_total_cost{model_name="llama3-8b-instruct"}
```

---

## 12. Prometheus Query Examples

### 12.1 Basic Queries

**Get all metrics:**
```promql
opencost_inference_total_cost
opencost_inference_cost_per_million_tokens
```

**Filter by model:**
```promql
opencost_inference_total_cost{model_name="llama3-8b-instruct"}
```

**Filter by namespace:**
```promql
opencost_inference_total_cost{namespace="llm-d-prod"}
```

**Filter by model and namespace:**
```promql
opencost_inference_total_cost{model_name="llama3-8b-instruct",namespace="llm-d-prod"}
```

### 12.2 Aggregation Queries

**Total cost across all namespaces for a model:**
```promql
sum by (model_name) (opencost_inference_total_cost{model_name="llama3-8b-instruct"})
```

**Total cost per namespace (all models):**
```promql
sum by (namespace) (opencost_inference_total_cost)
```

**Average cost per million tokens across namespaces:**
```promql
avg by (model_name) (opencost_inference_cost_per_million_tokens)
```

### 12.3 Comparison Queries

**Compare production vs staging costs:**
```promql
opencost_inference_total_cost{model_name="llama3-8b-instruct",namespace=~"llm-d-prod|llm-d-staging"}
```

**Cost difference between namespaces:**
```promql
opencost_inference_total_cost{model_name="llama3-8b-instruct",namespace="llm-d-prod"}
-
opencost_inference_total_cost{model_name="llama3-8b-instruct",namespace="llm-d-staging"}
```

### 12.4 Time-based Queries

**Cost over time for specific namespace:**
```promql
rate(opencost_inference_total_cost{namespace="llm-d-prod"}[5m])
```

**Daily cost trend:**
```promql
increase(opencost_inference_total_cost{namespace="llm-d-prod"}[1d])
```

---

## 13. Troubleshooting

### 13.1 Common Issues

**Issue:** Metrics not appearing in Prometheus

**Solutions:**
- Check OpenCost logs: `kubectl logs -n opencost <pod> | grep inference`
- Verify `INFERENCE_COST_ENABLED=true`
- Check Prometheus is scraping OpenCost: `kubectl get servicemonitor -n opencost`
- Verify vLLM metrics are available in Prometheus with namespace label

**Issue:** Metric values are zero

**Solutions:**
- Check vLLM is processing requests
- Verify token metrics are non-zero: `vllm:prompt_tokens_total`
- Check GPU cost allocation exists in OpenCost
- Review calculation logic in logs

**Issue:** Missing namespace label

**Solutions:**
- Verify Prometheus adds namespace label during scraping
- Check ServiceMonitor configuration
- Verify vLLM pods are in correct namespace
- Check Prometheus relabeling rules

**Issue:** Metrics from wrong namespace

**Solutions:**
- Use namespace label in Prometheus queries: `{namespace="llm-d-prod"}`
- Verify namespace label is correct in metrics
- Check pod namespace matches expected value

**Issue:** Cannot aggregate across namespaces

**Solutions:**
- Use Prometheus aggregation functions: `sum by (model_name)`
- Verify all namespaces have metrics
- Check for label mismatches

---

## 14. Use Cases

### 14.1 Single llm-d Deployment

**Scenario:** One llm-d deployment in `llm-d-prod` namespace

**Configuration:** No special configuration needed

**Queries:**
```promql
# All metrics
opencost_inference_total_cost

# Specific model
opencost_inference_total_cost{model_name="llama3-8b-instruct"}
```

### 14.2 Multiple llm-d Deployments (Prod/Staging)

**Scenario:** Production and staging deployments in separate namespaces

**Configuration:** No special configuration needed - namespace label automatically distinguishes them

**Queries:**
```promql
# Production only
opencost_inference_total_cost{namespace="llm-d-prod"}

# Staging only
opencost_inference_total_cost{namespace="llm-d-staging"}

# Compare both
opencost_inference_total_cost{namespace=~"llm-d-prod|llm-d-staging"}

# Total across both
sum by (model_name) (opencost_inference_total_cost{namespace=~"llm-d-prod|llm-d-staging"})
```

### 14.3 Independent vLLM Deployments

**Scenario:** Multiple teams running vLLM independently in different namespaces

**Configuration:** No special configuration needed

**Queries:**
```promql
# Team A's costs
opencost_inference_total_cost{namespace="team-a"}

# Team B's costs
opencost_inference_total_cost{namespace="team-b"}

# All teams
sum by (namespace) (opencost_inference_total_cost)
```

### 14.4 Cost Allocation by Namespace

**Scenario:** Chargeback to different teams/projects based on namespace

**Queries:**
```promql
# Monthly cost per namespace
sum by (namespace) (increase(opencost_inference_total_cost[30d]))

# Cost breakdown
topk(10, sum by (namespace, model_name) (opencost_inference_total_cost))
```

---

## 15. Next Steps (Phase 2)

After Phase 1 is complete and validated:

1. Add cache-aware cost calculation
2. Implement team/workload labels (in addition to namespace)
3. Separate GPU and memory costs
4. Add cache savings calculation
5. Add HTTP API endpoint for programmatic access
6. Add MCP server integration

---

**Document Version:** 2.0  
**Last Updated:** 2026-05-03  
**Status:** Design Document for Implementation  
**Approach:** Option 4 - Export metrics per-namespace with namespace label
          {
