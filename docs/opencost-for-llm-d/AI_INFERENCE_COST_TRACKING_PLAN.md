# AI Inference Cost Tracking for llm-d: Analysis and Implementation Plan

**Date:** 2026-05-03
**Author:** Planning Mode Analysis
**Status:** Proposal for Review

---

## Background

### Original Request

**User Task:**
> I would like to add the ability to track AI inference costs to models deployed using llm-d ((/Users/simanadler/Work/Dev/llm-d-060/llm-d). One option might be to extend opencost (./opencost), although I am open to other suggestions. Provide an analysis and plan for the best approach.

### Requirements Clarification

**User Requirements:**
> You will see when you review the llm-d code that it already collects lots of metrics - some on its own, some from vllm, some from nvidia gpu. I don't want to reinvent things that already exist in llm-d metrics nor in opencost metrics.
>
> **Cost Tracking Requirements:**
> - Cost of self hosting AI models
> - Usage Inference Costs per Team
> - Costs per Workload
> - Costs per Model
> - Costs per Model Variant - deployed with different hardware, LoRa, etc
> - Cost per Token - Per Model, Model Variant
> - KV Cache Costs and Savings
>
> Should integrate with existing cloud provider and support custom pricing

### Analysis Scope

This document provides:
1. Analysis of available metrics in llm-d and vLLM
2. Analysis of OpenCost architecture and extensibility
3. Identification of components requiring updates
4. Design of AI inference cost tracking data model
5. Recommended integration approach with justification
6. Detailed cost allocation methodology
7. Pricing configuration approach
8. 10-week implementation plan with 5 phases
9. Architecture diagrams and API specifications

---

## Executive Summary

This document proposes **extending OpenCost with a new AI Inference cost domain** to track inference costs for models deployed using llm-d. This approach leverages OpenCost's existing infrastructure (Prometheus integration, MCP server, cost allocation algorithms) while adding specialized AI inference cost tracking capabilities.

**Key Benefits:**
- Unified cost visibility across Kubernetes, cloud, and AI inference
- Reuses proven OpenCost infrastructure and allocation logic
- Comprehensive tracking: tokens, GPU time, KV cache, network costs
- Multi-dimensional aggregation: by team, workload, model, variant
- Native integration with existing llm-d monitoring stack

**Estimated Timeline:** 10 weeks (5 phases)

---

## Table of Contents

1. [Available Metrics in llm-d](#1-available-metrics-in-llm-d)
2. [OpenCost Architecture Analysis](#2-opencost-architecture-analysis)
3. [Component Analysis](#3-component-analysis)
4. [Recommended Integration Approach](#4-recommended-integration-approach)
5. [Data Model Design](#5-data-model-design)
6. [Cost Allocation Methodology](#6-cost-allocation-methodology)
7. [Pricing Configuration](#7-pricing-configuration)
8. [Implementation Plan](#8-implementation-plan)
9. [Architecture Diagram](#9-architecture-diagram)
10. [Key Benefits](#10-key-benefits)
11. [Alternative Approaches Considered](#11-alternative-approaches-considered)
12. [Success Metrics](#12-success-metrics)
13. [Risk Mitigation](#13-risk-mitigation)
14. [Next Steps](#14-next-steps)
15. [Open Questions](#15-open-questions)

---

## 1. Available Metrics in llm-d

### 1.1 vLLM Metrics (Already Collected)

**Token Metrics:**
- `vllm:prompt_tokens_total` - Input tokens processed
- `vllm:generation_tokens_total` - Output tokens generated
- Token rate: `rate(vllm:prompt_tokens_total[5m]) + rate(vllm:generation_tokens_total[5m])`

**Performance Metrics:**
- `vllm:time_to_first_token_seconds` - TTFT latency (histogram)
- `vllm:inter_token_latency_seconds` - ITL/time per output token (histogram)
- `vllm:num_requests_running` - Active requests (gauge)
- `vllm:num_requests_waiting` - Queued requests (gauge)
- `vllm:num_preemptions` - Request preemptions (counter)

**Resource Utilization:**
- `vllm:kv_cache_usage_perc` - KV cache utilization percentage
- `vllm:prefix_cache_hits_total` - Prefix cache hits (counter)
- `vllm:prefix_cache_queries_total` - Prefix cache queries (counter)
- Cache hit rate: `sum(rate(vllm:prefix_cache_hits_total[5m])) / sum(rate(vllm:prefix_cache_queries_total[5m]))`
- `DCGM_FI_DEV_GPU_UTIL` or `nvidia_gpu_duty_cycle` - GPU utilization

**KV Cache Residency (when `--kv-cache-metrics-enabled`):**
- `vllm:kv_block_lifetime_seconds` - Block lifetime
- `vllm:kv_block_idle_before_evict_seconds` - Idle time before eviction
- `vllm:kv_block_reuse_gap_seconds` - Reuse gap

### 1.2 EPP/Gateway Metrics (Already Collected)

**Request Tracking:**
- `inference_objective_request_total` - Total requests by model (counter)
- `inference_objective_request_error_total` - Failed requests (counter)
- `inference_objective_request_duration_seconds` - E2E latency (histogram)
- `inference_objective_running_requests` - Concurrent requests per model (gauge)
- `inference_objective_input_tokens` - Input token distribution (histogram)
- `inference_objective_output_tokens` - Output token distribution (histogram)

**Routing & Scheduling:**
- `inference_extension_scheduler_e2e_duration_seconds` - Scheduler latency (histogram)
- `inference_extension_plugin_duration_seconds` - Plugin processing time (histogram)
- `inference_extension_flow_control_queue_size` - Queue depth (gauge)
- `inference_extension_prefix_indexer_size` - Prefix indexer size (gauge)
- `inference_extension_prefix_indexer_hit_ratio` - EPP cache hit ratio (histogram)

**P/D Disaggregation:**
- `llm_d_inference_scheduler_pd_decision_total` - P/D decision counts by type

### 1.3 Metric Availability Assessment

✅ **Available:** Token counts, request counts, latency, GPU utilization, cache hit rates  
✅ **Available:** Model-level aggregation via labels  
✅ **Available:** Namespace/pod/container attribution  
⚠️ **Partially Available:** KV cache memory usage (percentage only, not absolute bytes)  
❌ **Missing:** Per-request cost attribution (requires correlation)  
❌ **Missing:** KV cache transfer times for P/D disaggregation

---

## 2. OpenCost Architecture Analysis

### 2.1 Cost Domains

OpenCost currently supports **four primary cost domains**:

1. **Allocation** ([`core/pkg/opencost/allocation.go`](opencost/core/pkg/opencost/allocation.go))
   - Kubernetes workload costs (pods, namespaces, deployments)
   - CPU, GPU, RAM, network, storage costs
   - Idle cost tracking and distribution

2. **Asset**
   - Infrastructure costs (nodes, disks, load balancers)
   - Cloud provider pricing integration

3. **CloudCost**
   - Cloud provider billing costs
   - Multi-cloud support (AWS, Azure, GCP, etc.)

4. **CustomCost** ([`pkg/customcost/types.go`](opencost/pkg/customcost/types.go))
   - External cost plugins (e.g., Datadog, external services)
   - Plugin architecture for extensibility

### 2.2 Key Components

**MCP Server** ([`pkg/mcp/server.go`](opencost/pkg/mcp/server.go))
- HTTP-based API on port 8081 (opt-in, disabled by default)
- Supports multiple query types via `QueryType` enum
- Structured query interface with `OpenCostQueryRequest`
- AI agent integration via Model Context Protocol

**Custom Cost System** ([`pkg/customcost/`](opencost/pkg/customcost/))
- Plugin architecture for external costs
- Supports custom domains and cost sources
- Time-series data with aggregation
- Cost types: blended, list, billed

**Cost Model** ([`pkg/costmodel/`](opencost/pkg/costmodel/))
- Prometheus integration for metrics
- Cost allocation algorithms
- API handlers for queries
- Multi-cluster support

### 2.3 Extensibility Points

OpenCost provides several extension points suitable for AI inference costs:

1. **New Cost Domain:** Add `InferenceQueryType` to MCP server
2. **New Package:** Create `pkg/inferencecost/` for inference-specific logic
3. **Pricing Config:** Extend `configs/` with inference pricing
4. **API Endpoints:** Add `/inference` endpoint to cost model router
5. **MCP Integration:** Extend MCP server with inference query support

---

## 3. Component Analysis

### 3.1 Existing llm-d Components Requiring Updates

| Component | Location | Required Changes | Priority |
|-----------|----------|------------------|----------|
| **Prometheus Monitoring** | `docs/monitoring/` | Add AI inference cost metric recording rules | High |
| **Grafana Dashboards** | `docs/monitoring/grafana/` | Add cost tracking panels to existing dashboards | High |
| **Helm Charts** | `guides/*/values.yaml` | Add OpenCost integration configuration | Medium |
| **Documentation** | `docs/` | Document cost tracking setup and usage | Medium |

### 3.2 Existing OpenCost Components Requiring Updates

| Component | Location | Required Changes | Priority |
|-----------|----------|------------------|----------|
| **MCP Server** | [`pkg/mcp/server.go`](opencost/pkg/mcp/server.go) | Add `InferenceQueryType` to QueryType enum | High |
| **Query Types** | [`pkg/mcp/server.go`](opencost/pkg/mcp/server.go) | Add `InferenceQuery` struct | High |
| **Cost Model Router** | `pkg/costmodel/router.go` | Add `/inference` API endpoint | High |
| **Prometheus Integration** | `pkg/costmodel/` | Add queries for vLLM and EPP metrics | High |

### 3.3 New Components Required (in OpenCost)

All new components will be added to the **OpenCost codebase** as part of extending it with AI inference cost tracking capabilities.

| Component | Purpose | Location (in OpenCost repo) | Priority |
|-----------|---------|------------------------------|----------|
| **AI Inference Cost Domain** | Core data model for inference costs | `opencost/pkg/inferencecost/types.go` | High |
| **Token Cost Calculator** | Calculate costs from token metrics | `opencost/pkg/inferencecost/calculator.go` | High |
| **Model Pricing Config** | Per-model pricing configuration | `opencost/configs/inference-pricing.json` | High |
| **Inference Cost Aggregator** | Aggregate costs by dimensions | `opencost/pkg/inferencecost/aggregator.go` | High |
| **KV Cache Cost Tracker** | Track cache utilization and savings | `opencost/pkg/inferencecost/kvcache.go` | Medium |
| **Prometheus Metric Collector** | Collect vLLM and EPP metrics | `opencost/pkg/inferencecost/collector.go` | High |
| **Cost Query Service** | Service layer for cost queries | `opencost/pkg/inferencecost/queryservice.go` | Medium |
| **Pricing Config Loader** | Load and validate pricing configs | `opencost/pkg/inferencecost/pricing.go` | Medium |

**Package Structure in OpenCost:**
```
opencost/pkg/inferencecost/
├── types.go              # InferenceCost, InferenceProperties data models
├── calculator.go         # Cost calculation logic
├── aggregator.go         # Cost aggregation by dimensions
├── collector.go          # Prometheus metric collection
├── kvcache.go           # KV cache cost tracking
├── pricing.go           # Pricing configuration management
├── queryservice.go      # Query service layer
├── repository.go        # Data persistence interface
├── memoryrepository.go  # In-memory implementation
└── types_test.go        # Unit tests
```

**Note:** These components extend OpenCost's existing architecture. No new components are required in llm-d itself - llm-d only needs configuration updates to integrate with OpenCost (see Section 3.1).

---

## 4. Recommended Integration Approach

### 4.1 Option A: Extend OpenCost (RECOMMENDED)

**Pros:**
✅ Unified cost view across K8s, cloud, and AI inference  
✅ Reuses proven Prometheus integration  
✅ Existing MCP server for AI agents  
✅ Proven cost allocation algorithms  

**Cons:**
⚠️ Requires changes to OpenCost codebase  
⚠️ Need to maintain compatibility with OpenCost releases  

**Decision:** Extend OpenCost with a new inference cost domain.

---

## 5. Inference Cost Metrics

This section defines the inference cost metrics that will be exposed and how they will be made available to users.

### 5.1 Prometheus Metrics (Exported by OpenCost)

OpenCost will export the following new Prometheus metrics for AI inference costs:

#### Cost Metrics

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `opencost_inference_token_cost` | Gauge | model_name, model_version, model_variant, namespace, team, workload | Cost per token (calculated from infrastructure) |
| `opencost_inference_total_cost` | Counter | model_name, model_version, model_variant, namespace, team, workload | Total inference cost over time |
| `opencost_inference_gpu_cost` | Counter | model_name, namespace, pod | GPU cost attributed to inference |
| `opencost_inference_cache_cost` | Counter | model_name, namespace, pod | KV cache infrastructure cost |
| `opencost_inference_cache_savings` | Counter | model_name, namespace, pod | Cost savings from cache hits |

#### Token Metrics (Derived from vLLM)

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `opencost_inference_input_tokens_total` | Counter | model_name, model_version, namespace | Total input tokens processed |
| `opencost_inference_output_tokens_total` | Counter | model_name, model_version, namespace | Total output tokens generated |
| `opencost_inference_cached_tokens_total` | Counter | model_name, namespace | Total tokens served from cache |

#### Efficiency Metrics

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `opencost_inference_cost_per_million_tokens` | Gauge | model_name, model_version, model_variant | Cost per 1M tokens |
| `opencost_inference_cache_hit_rate` | Gauge | model_name, namespace | Cache hit rate (0-1) |
| `opencost_inference_gpu_utilization` | Gauge | model_name, namespace, pod | GPU utilization for inference |

### 5.2 OpenCost API Endpoints

New REST API endpoints for querying inference costs:

#### `/inference` - Query Inference Costs

**Request:**
```
GET /inference?window=7d&aggregate=model,namespace&filter=namespace:"default"
```

**Response:**
```json
{
  "window": {
    "start": "2026-04-26T00:00:00Z",
    "end": "2026-05-03T00:00:00Z"
  },
  "totalCost": 1250.50,
  "inferenceCosts": [
    {
      "modelName": "llama3-8b-instruct",
      "modelVersion": "v1.0",
      "modelVariant": "h200-lora-v1",
      "namespace": "default",
      "team": "ml-team",
      "promptTokens": 50000000,
      "generationTokens": 25000000,
      "tokenCost": 7.50,
      "gpuCost": 500.00,
      "kvCacheCost": 50.00,
      "totalCost": 557.50,
      "cacheHitRate": 0.755,
      "cacheSavings": 125.00
    }
  ]
}
```

#### `/inference/summary` - Aggregated Summary

**Request:**
```
GET /inference/summary?window=30d&groupBy=model
```

**Response:**
```json
{
  "window": "30d",
  "summary": [
    {
      "modelName": "llama3-8b-instruct",
      "totalCost": 15000.00,
      "totalTokens": 2500000000,
      "costPerMillionTokens": 6.00,
      "averageCacheHitRate": 0.65
    }
  ]
}
```

### 5.3 MCP Server Integration

OpenCost's MCP server will expose inference cost queries to AI agents:

**Tool:** `get_inference_costs`

**Parameters:**
- `window` (required): Time window (e.g., "7d", "24h")
- `aggregate` (optional): Aggregation dimensions (e.g., "model,namespace,team")
- `modelName` (optional): Filter by model name
- `modelVersion` (optional): Filter by model version
- `modelVariant` (optional): Filter by model variant
- `namespace` (optional): Filter by namespace
- `team` (optional): Filter by team

**Example MCP Query:**
```json
{
  "tool": "get_inference_costs",
  "arguments": {
    "window": "7d",
    "aggregate": "model,namespace",
    "modelName": "llama3-8b-instruct"
  }
}
```

### 5.4 Grafana Dashboard Metrics

Pre-built Grafana dashboards will display:

**Cost Overview Panel:**
- Total inference cost by model
- Cost per 1M tokens trend
- Cost breakdown (GPU, cache, network)

**Efficiency Panel:**
- Cache hit rate by model
- Cache savings vs infrastructure cost
- GPU utilization vs cost

**Team/Workload Panel:**
- Cost by team/namespace
- Top cost consumers
- Cost allocation breakdown

### 5.5 Metric Availability Methods

**Method 1: Prometheus Scraping**
- OpenCost exports metrics on `/metrics` endpoint
- Prometheus scrapes metrics every 15-60 seconds
- Metrics available in Prometheus for querying

**Method 2: OpenCost API**
- REST API queries return real-time calculated costs
- Supports filtering, aggregation, and time windows
- JSON responses for programmatic access

**Method 3: MCP Server**
- AI agents query via Model Context Protocol
- HTTP-based transport on port 8081
- Structured query interface

**Method 4: Grafana Dashboards**
- Pre-built dashboards query Prometheus
- Visual representation of costs and trends
- Alerts based on cost thresholds

**Method 5: kubectl cost CLI**
- Command-line tool for cost queries
- Integrates with OpenCost API
- Human-readable output

### 5.6 Metric Calculation Frequency

| Metric Type | Calculation Frequency | Retention |
|-------------|----------------------|-----------|
| Real-time costs | Every 60 seconds | 90 days detailed |
| Aggregated costs | Every 5 minutes | 1 year |
| Cache metrics | Every 30 seconds | 90 days |
| Efficiency metrics | Every 5 minutes | 1 year |

---

## 6. Cost Allocation Methodology

### 6.1 Token-Based Costs

**Approach:** Calculate per-token costs from actual infrastructure costs rather than using pre-configured pricing.

**Formula:**
```
Input Token Price = Total Infrastructure Cost / Total Tokens Processed
Output Token Price = Total Infrastructure Cost / Total Tokens Generated

Token Cost = (Prompt Tokens × Input Token Price) + (Generation Tokens × Output Token Price)
```

**Calculation Method:**

**Step 1: Calculate Total Infrastructure Cost**
```
Total Infrastructure Cost = GPU Cost + Memory Cost + KV Cache Infrastructure Cost + Network Cost + Overhead
```

Where:
- **GPU Cost** = GPU Hours × GPU Hourly Rate × Utilization
  - GPU Hourly Rate from cloud provider API or OpenCost's existing pricing
  - Utilization from `DCGM_FI_DEV_GPU_UTIL` or `nvidia_gpu_duty_cycle`
  
- **Memory Cost** = Memory GB-Hours × Memory Hourly Rate
  - Base memory usage (non-cache)
  - Memory rate from cloud provider or custom pricing
  
- **KV Cache Infrastructure Cost** = Cache Memory GB-Hours × Memory Hourly Rate
  - Dedicated cost for KV cache memory allocation
  - Includes cost of maintaining and refreshing cache
  - From `vllm:kv_cache_usage_perc` × Total GPU Memory
  - This cost is amortized across ALL tokens (both cached and computed)
  
- **Network Cost** = Data Transfer GB × Network Rate
  - For P/D disaggregation transfers
  
- **Overhead** = Proportional share of cluster overhead costs
  - From OpenCost's existing overhead allocation

**Step 2: Calculate Effective Token Throughput (Accounting for KV Cache)**

KV cache hits mean tokens don't need to be recomputed, so they should cost less:

```
# Get cache metrics
Cache Hit Rate = sum(vllm:prefix_cache_hits_total) / sum(vllm:prefix_cache_queries_total)

# Calculate actual tokens processed (excluding cache hits)
Total Input Tokens = sum(vllm:prompt_tokens_total) over time window
Total Output Tokens = sum(vllm:generation_tokens_total) over time window

# Effective tokens = tokens that actually consumed compute
Effective Input Tokens = Total Input Tokens × (1 - Cache Hit Rate)
Cached Input Tokens = Total Input Tokens × Cache Hit Rate

# Output tokens are always computed (no cache for generation)
Effective Output Tokens = Total Output Tokens
```

**Step 3: Calculate Per-Token Costs with Cache Infrastructure Amortization**

```
# Separate costs into compute and cache infrastructure
Compute Cost = GPU Cost + Memory Cost (non-cache)
Cache Infrastructure Cost = KV Cache Infrastructure Cost

# Amortize cache infrastructure cost across ALL tokens
# This accounts for the cost of maintaining the cache, even for cache hits
Cache Cost Per Token = Cache Infrastructure Cost / Total Input Tokens

# Calculate cost per computed token (tokens that needed GPU compute)
Base Compute Cost Per Token = Compute Cost / (Effective Input Tokens + Effective Output Tokens)

# Final per-token prices
Input Token Price (computed) = (Base Compute Cost Per Token × Input Weight Factor) + Cache Cost Per Token
Input Token Price (cached) = Cache Cost Per Token  # Only cache infrastructure, no compute
Output Token Price = (Base Compute Cost Per Token × Output Weight Factor) + Cache Cost Per Token
```

**Key Insight:**
- **Cache infrastructure cost is amortized across ALL tokens** (both cached and computed)
- This reflects the reality that cache must be maintained regardless of hit rate
- Computed tokens pay: compute cost + cache infrastructure cost
- Cached tokens pay: only cache infrastructure cost (no compute)
- Higher cache hit rates reduce average cost per token by avoiding compute costs

**Weight Factors** (to account for different computational costs):
- **Input Weight Factor**: Typically 1.0 (baseline)
  - Prefill phase: processes all input tokens in parallel
  - Lower per-token latency but higher batch processing cost
  
- **Output Weight Factor**: Typically 1.5-2.0 (higher cost)
  - Decode phase: generates tokens sequentially
  - Higher per-token cost due to autoregressive generation
  - Configurable based on observed TTFT vs ITL ratios

**Cost Calculation for a Request:**
```
Request Cost = (Computed Input Tokens × Input Token Price) +
               (Cached Input Tokens × Cache Cost Per Token) +
               (Output Tokens × Output Token Price)
```

**Alternative: Separate Input/Output Cost Calculation**

For more accuracy, calculate input and output costs separately:

```
Input Token Cost = (Prefill GPU Cost + Prefill Memory Cost) / Total Input Tokens
Output Token Cost = (Decode GPU Cost + Decode Memory Cost) / Total Output Tokens
```

This requires:
- Separate tracking of prefill vs decode GPU time
- Available in P/D disaggregation deployments
- Can be estimated from TTFT and ITL metrics in unified deployments

**Example Calculation with KV Cache:**

**Scenario:** llama3-8b-instruct running on H100 GPU for 1 hour

**Infrastructure Costs:**
- GPU: 1 hour × $8.00/hr × 75% utilization = $6.00
- Memory (KV cache): 48 GB × 1 hour × $0.05/GB-hr = $2.40
- Network: 100 GB × $0.01/GB = $1.00
- Overhead: $0.60 (10% of infrastructure)
- **Total: $10.00**

**Token Throughput:**
- Total input tokens: 50M tokens
- Total output tokens: 25M tokens
- Cache hit rate: 60% (from metrics)

**Effective Tokens (accounting for cache):**
- Effective input tokens: 50M × (1 - 0.60) = 20M tokens (actually computed)
- Cached input tokens: 50M × 0.60 = 30M tokens (served from cache)
- Effective output tokens: 25M tokens (always computed)
- **Total computed tokens: 45M tokens**

**Cost Breakdown:**
- GPU cost: $6.00
- Memory cost (non-cache): $0.50
- KV cache infrastructure cost: $2.40 (48 GB cache)
- Network + overhead: $1.60
- **Total compute cost: $6.50**
- **Total cache infrastructure cost: $2.40**

**Per-Token Costs:**
- Base compute cost per token: $6.50 / 45M = $0.000000144 per computed token
- Cache infrastructure cost per token: $2.40 / 50M = $0.000000048 per token (amortized across ALL input tokens)
- Input weight factor: 1.0
- Output weight factor: 1.5

**Calculated Prices:**
- Input token price (computed): ($0.000000144 × 1.0) + $0.000000048 = $0.000000192 per token
- Input token price (cached): $0.000000048 per token (only cache infrastructure, 4× cheaper!)
- Output token price: ($0.000000144 × 1.5) + $0.000000048 = $0.000000264 per token

**Cost for a Request:**

*Scenario A: No cache hits (cold request)*
- Request: 1000 input tokens (all computed), 500 output tokens
- Cost: (1000 × $0.000000192) + (500 × $0.000000264)
- Cost: $0.000192 + $0.000132 = $0.000324

*Scenario B: 80% cache hit (warm request)*
- Request: 1000 input tokens (200 computed, 800 cached), 500 output tokens
- Cost: (200 × $0.000000192) + (800 × $0.000000048) + (500 × $0.000000264)
- Cost: $0.000038 + $0.000038 + $0.000132 = $0.000208
- **Savings: 36% cost reduction due to cache!**

**Key Insights:**
1. **Cache infrastructure cost is always paid** - amortized across all tokens
2. **Cache hits avoid compute costs** - saving GPU and memory compute costs
3. **Higher cache hit rates = lower average cost** - but never zero cost
4. **Cache ROI is measurable** - compare savings vs infrastructure cost
5. **Fair cost allocation** - teams with high cache hit rates pay proportionally less

**Cache Economics:**
- Cache infrastructure cost: $2.40/hour
- Compute cost saved per cached token: $0.000000144
- Break-even: Need ~16.7M cached tokens/hour to justify cache infrastructure
- At 60% hit rate with 50M tokens/hour: 30M cached tokens = $4.32 saved vs $2.40 cost = **$1.92 net savings**

**Configuration Options:**

While costs are calculated from infrastructure, administrators can configure:
- **Weight factors** for input vs output tokens
- **Overhead percentage** to include in calculations
- **Amortization period** for cost averaging
- **Minimum cost thresholds** to avoid division by zero

### 6.2 GPU Time-Based Costs
```
GPU Cost = GPU Hours × GPU Hourly Rate × Utilization Factor
```

### 6.3 KV Cache Costs
```
KV Cache Cost = Cache Memory GB-Hours × Memory Hourly Rate
Cache Savings = (Cache Hits / Total Requests) × Recompute Cost
```

---

## 7. Implementation Plan

### Iterative Development Approach

The implementation follows an iterative approach, starting with a minimal proof-of-concept and progressively adding functionality.

---

### Phase 1: Proof of Concept - Basic Cost Metrics (Weeks 1-2)

**Goal:** Demonstrate end-to-end cost calculation with two core metrics.

**Metrics to Implement:**
1. `opencost_inference_total_cost{model_name, model_version}` - Total inference cost
2. `opencost_inference_cost_per_million_tokens{model_name, model_version}` - Cost efficiency metric

**Dependencies (Source Metrics from vLLM/Prometheus):**
- `vllm:prompt_tokens_total{model_name}` - Input tokens
- `vllm:generation_tokens_total{model_name}` - Output tokens
- `DCGM_FI_DEV_GPU_UTIL` or `nvidia_gpu_duty_cycle` - GPU utilization
- Node cost data from OpenCost's existing allocation system

**Deliverables:**
- [ ] Create `opencost/pkg/inferencecost/` package structure
- [ ] Implement basic Prometheus collector to query vLLM metrics
- [ ] Implement simple cost calculator:
  - Calculate total infrastructure cost from GPU utilization
  - Calculate total tokens from vLLM metrics
  - Compute cost per token
- [ ] Export two Prometheus metrics from OpenCost
- [ ] Basic unit tests
- [ ] Documentation for PoC

**Simplified Calculation (PoC):**
```
Total Infrastructure Cost = GPU Cost (from OpenCost allocation)
Total Tokens = sum(vllm:prompt_tokens_total) + sum(vllm:generation_tokens_total)
Cost Per Token = Total Infrastructure Cost / Total Tokens
Cost Per Million Tokens = Cost Per Token × 1,000,000

opencost_inference_total_cost = Total Infrastructure Cost
opencost_inference_cost_per_million_tokens = Cost Per Million Tokens
```

**Success Criteria:**
- Metrics appear in Prometheus
- Values are reasonable (non-zero, within expected range)
- Can query metrics by model_name and model_version
- Basic Grafana panel displays the metrics

---

### Phase 2: Enhanced Cost Attribution (Weeks 3-4)

**Goal:** Add more accurate cost breakdown and cache awareness.

**New Metrics:**
- `opencost_inference_gpu_cost{model_name, model_version, namespace}`
- `opencost_inference_cache_cost{model_name, model_version, namespace}`
- `opencost_inference_cache_hit_rate{model_name, namespace}`

**Enhancements:**
- Separate GPU cost from memory cost
- Add KV cache cost calculation
- Implement cache hit rate tracking from `vllm:prefix_cache_*` metrics
- Add namespace-level attribution

**Deliverables:**
- [ ] Enhanced cost calculator with cache awareness
- [ ] Cache cost tracker component
- [ ] Additional Prometheus metrics
- [ ] Updated Grafana dashboards
- [ ] Integration tests

---

### Phase 3: Multi-Dimensional Aggregation (Weeks 5-6)

**Goal:** Support cost aggregation by team, workload, and variant.

**New Metrics:**
- `opencost_inference_total_cost{model_name, model_version, model_variant, namespace, team, workload}`
- Add labels for team and workload attribution

**Enhancements:**
- Extract team/workload from pod labels
- Support model variant tracking
- Implement cost aggregator for multi-dimensional queries

**Deliverables:**
- [ ] Label extraction from Kubernetes metadata
- [ ] Cost aggregator component
- [ ] Enhanced metrics with additional labels
- [ ] Multi-dimensional Grafana dashboards

---

### Phase 4: API and MCP Integration (Weeks 7-8)

**Goal:** Expose cost data via REST API and MCP server.

**New Endpoints:**
- `GET /inference?window=7d&aggregate=model,namespace`
- `GET /inference/summary?window=30d&groupBy=model`

**Enhancements:**
- Add `/inference` endpoint to OpenCost API
- Extend MCP server with `get_inference_costs` tool
- Support filtering and aggregation in queries

**Deliverables:**
- [ ] REST API endpoints
- [ ] MCP server integration
- [ ] API documentation
- [ ] Example queries and use cases

---

### Phase 5: Advanced Features (Weeks 9-10)

**Goal:** Add P/D disaggregation costs, network costs, and cache savings.

**New Metrics:**
- `opencost_inference_network_cost{model_name, namespace}`
- `opencost_inference_cache_savings{model_name, namespace}`
- Separate prefill/decode cost tracking

**Enhancements:**
- Network cost calculation for P/D transfers
- Cache savings calculation
- Prefill vs decode cost separation

**Deliverables:**
- [ ] Network cost calculator
- [ ] Cache savings tracker
- [ ] P/D cost separation
- [ ] Complete metric set

---

### Phase 6: Production Readiness (Weeks 11-12)

**Goal:** Testing, optimization, and documentation.

**Activities:**
- Comprehensive unit and integration tests (>80% coverage)
- Performance testing and optimization
- Load testing with high query volume
- Complete documentation
- Grafana dashboard library
- Deployment guides

**Deliverables:**
- [ ] Complete test suite
- [ ] Performance benchmarks
- [ ] Production documentation
- [ ] Deployment automation
- [ ] Troubleshooting guides

---

## 8. Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                         OpenCost                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  Allocation  │  │  CloudCost   │  │  Inference   │ NEW  │
│  │    Costs     │  │    Costs     │  │    Costs     │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│                            │                                 │
│                   ┌────────▼────────┐                        │
│                   │   MCP Server    │                        │
│                   └─────────────────┘                        │
└─────────────────────────────────────────────────────────────┘
                            │
                ┌───────────┴───────────┐
                │                       │
        ┌───────▼────────┐     ┌───────▼────────┐
        │   Prometheus   │     │   llm-d vLLM   │
        │   (Metrics)    │     │   + EPP        │
        └────────────────┘     └────────────────┘
```

---

## 9. Key Benefits

- **Unified Cost Visibility:** Single pane of glass for all costs
- **Leverages Existing Infrastructure:** Reuses OpenCost components
- **Comprehensive Tracking:** Tokens, GPU, cache, network costs
- **Multi-Dimensional Aggregation:** By team, workload, model, variant

---

## 10. Success Metrics

| Metric | Target |
|--------|--------|
| Query performance | < 2 seconds for 7-day window |
| Cost accuracy | Within 5% of cloud billing |
| Deployment time | < 30 minutes |
| Test coverage | > 80% |

---

## 11. Next Steps

1. Review and approve this plan
2. Set up development environment
3. Create proof-of-concept
4. Begin Phase 1 implementation

---

**Document Version:** 1.0  
**Last Updated:** 2026-05-03  
**Status:** Proposal for Review