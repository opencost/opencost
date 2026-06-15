# AI Inference Cost Tracking

OpenCost tracks infrastructure costs for AI inference workloads running [vLLM](https://vllm.ai/)-based models ([llm-d](https://llm-d.ai/) and compatible deployments). It exposes costs both as Prometheus metrics and as queryable REST API endpoints.

## Overview

The inference cost tracking feature:
1. Collects token metrics from [vLLM](https://vllm.ai/) via Prometheus (`prompt_tokens_total`, `generation_tokens_total`, prefill/decode timing, KV cache hits)
2. Collects infrastructure costs (GPU, CPU, RAM, shared infra) from OpenCost's allocation layer
3. Calculates blended and differentiated (input/output) cost per million tokens under two cost bases: `allocation` and `usage`
4. Exports four Prometheus gauge metrics per model/namespace
5. Serves two REST API endpoints for on-demand cost queries with filtering, aggregation, and time-series support

## Enabling Inference Cost Tracking

Set the following environment variable on the OpenCost deployment:

```bash
INFERENCE_COST_ENABLED=true
```

OpenCost reads `PROMETHEUS_SERVER_ENDPOINT` for both the core metrics and the [vLLM](https://vllm.ai/) metric queries, so no separate Prometheus configuration is needed.

### Full Environment Variable Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `INFERENCE_COST_ENABLED` | `false` | Enable inference cost tracking |
| `INFERENCE_MODEL_LABEL` | `llm-d.ai/model` | Pod label whose value is the vLLM model name |
| `INFERENCE_SHARED_INFRA_LABEL` | `llm-d.ai/inference-serving` | Pod label key identifying shared infra pods (EPP, gateway) |
| `INFERENCE_SHARED_INFRA_LABEL_VALUE` | `true` | Label value that marks a pod as shared infra |
| `INFERENCE_KV_CACHE_BLOCK_SIZE` | `0` | Tokens per KV cache block; must match vLLM `--block-size`. Set to `0` to disable KV cache correction |
| `INFERENCE_COLLECTION_INTERVAL` | `2m` | Background collection interval |

### Kubernetes Deployment Example

```yaml
env:
  - name: INFERENCE_COST_ENABLED
    value: "true"
  - name: INFERENCE_MODEL_LABEL
    value: "llm-d.ai/model"
  - name: INFERENCE_KV_CACHE_BLOCK_SIZE
    value: "16"   # match your vLLM --block-size
```

## Cost Bases

OpenCost computes costs under two distinct bases, surfaced on every metric and API response:

| Cost Basis | Label / Value | Description |
|------------|---------------|-------------|
| **Allocation** | `cost_basis=allocation` | `max(request, usage) × price` + idle share + shared infra share. **Reconciles to the infrastructure bill.** |
| **Usage** | `cost_basis=usage` | Actual resource consumption only. Does **not** reconcile to the bill; idle and shared infrastructure costs are excluded. |

Use `allocation` for chargeback/showback and bill reconciliation. Use `usage` for pure workload efficiency analysis.

## Prometheus Metrics

When `INFERENCE_COST_ENABLED=true`, OpenCost registers and emits four gauge metrics every collection interval. All metrics carry `model_name`, `model_version`, `namespace`, and `cost_basis` labels.

### `llm_total_cost`

**Hourly infrastructure cost** attributed to a model.

**Labels:** `model_name`, `model_version`, `namespace`, `cost_basis`

This is an instantaneous hourly rate ($/hour), not a cumulative counter.

```promql
# Current hourly cost for a model
llm_total_cost{model_name="Qwen/Qwen3-32B", cost_basis="allocation"}

# Estimated 24-hour cost (if rate stays constant)
llm_total_cost{model_name="Qwen/Qwen3-32B", cost_basis="allocation"} * 24
```

### `llm_cost_per_million_tokens`

**Blended cost per 1M delivered tokens** (prompt + generation combined).

**Labels:** `model_name`, `model_version`, `namespace`, `cost_basis`

```promql
# Current blended cost per 1M tokens
llm_cost_per_million_tokens{model_name="Qwen/Qwen3-32B", cost_basis="allocation"}

# Average over the past 24 hours
avg_over_time(llm_cost_per_million_tokens{model_name="Qwen/Qwen3-32B"}[24h])

# Compare models side-by-side
llm_cost_per_million_tokens{cost_basis="allocation"}
```

### `llm_input_cost_per_million_tokens`

**Cost per 1M effective input (prompt) tokens.** When KV cache block size is configured, cached tokens are excluded from the denominator (cost reflects tokens that actually required compute).

**Labels:** `model_name`, `model_version`, `namespace`, `cost_basis`, `allocation_method`

`allocation_method` values:
- `compute_time` — input/output split is based on [vLLM](https://vllm.ai/) prefill time; KV cache correction applied
- `compute_time_uncorrected` — based on [vLLM](https://vllm.ai/) prefill time but KV cache data was unavailable
- `multiplier` — fixed output/input cost ratio used (timing metrics unavailable; default ratio 2.5×)

```promql
llm_input_cost_per_million_tokens{
  model_name="Qwen/Qwen3-32B",
  cost_basis="allocation",
  allocation_method="compute_time"
}
```

### `llm_output_cost_per_million_tokens`

**Cost per 1M output (generation) tokens.**

**Labels:** `model_name`, `model_version`, `namespace`, `cost_basis`, `allocation_method`

```promql
llm_output_cost_per_million_tokens{
  model_name="Qwen/Qwen3-32B",
  cost_basis="allocation"
}

# Input vs output cost ratio
llm_output_cost_per_million_tokens / llm_input_cost_per_million_tokens
```

### Example Alerting Rule

```yaml
groups:
- name: inference_costs
  rules:
  - alert: HighInferenceCost
    expr: llm_cost_per_million_tokens{cost_basis="allocation"} > 10
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "High inference cost for {{ $labels.model_name }}"
      description: "Model {{ $labels.model_name }} in {{ $labels.namespace }} costs ${{ $value }}/M tokens"
```

## REST API Endpoints

Two HTTP endpoints are available when `INFERENCE_COST_ENABLED=true`. They compute costs on demand by querying Prometheus and the OpenCost allocation layer, consistent with how `/allocation` and `/assets` work.

### `GET /inferenceCost/total`

Returns a single aggregated `InferenceCostSet` covering the full requested window.

**Query parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `window` | Yes | Time window: RFC3339 `start,end` or named range (e.g. `7d`, `24h`, `2025-01-01T00:00:00Z,2025-01-02T00:00:00Z`) |
| `costBasis` | No | `allocation` (default) or `usage` |
| `aggregate` | No | Comma-separated dimensions: `model_name`, `model_version`, `namespace`, `cluster` |
| `accumulate` | No | Step size within the window: `hour`, `day`, `week`, `month` (results are then accumulated into one total) |
| `filter` | No | `prop:value` pairs joined with `+` for AND logic, e.g. `namespace:default+model_name:llama3` |

**Example:**

```bash
# Total inference costs for the last 7 days, broken down by model
curl "http://localhost:9003/inferenceCost/total?window=7d&aggregate=model_name&costBasis=allocation"
```

**Response shape:**

```json
{
  "data": {
    "inferenceCosts": {
      "Qwen/Qwen3-32B:llm-d-namespace": {
        "properties": {
          "modelName": "Qwen/Qwen3-32B",
          "namespace": "llm-d-namespace"
        },
        "window": { "start": "...", "end": "..." },
        "costBasis": "allocation",
        "totalCost": 42.50,
        "promptTokens": 12000000,
        "generationTokens": 3000000,
        "totalTokens": 15000000,
        "costPerMillionTokens": 2.83,
        "inputCost": 28.40,
        "outputCost": 14.10,
        "inputCostPerMillionTokens": 2.37,
        "outputCostPerMillionTokens": 4.70,
        "allocationMethod": "compute_time"
      }
    },
    "window": { "start": "...", "end": "..." }
  }
}
```

### `GET /inferenceCost/timeseries`

Returns an `InferenceCostSetRange` — one `InferenceCostSet` per step within the window.

Same parameters as `/total`, but `accumulate` is **required** (it defines the step size).

**Example:**

```bash
# Daily inference costs per model over the past 7 days
curl "http://localhost:9003/inferenceCost/timeseries?window=7d&accumulate=day&aggregate=model_name"

# Hourly costs for a specific namespace, usage basis
curl "http://localhost:9003/inferenceCost/timeseries?window=24h&accumulate=hour&costBasis=usage&filter=namespace:llm-d-prod"
```

**Response shape:**

```json
{
  "data": {
    "inferenceCostSets": [
      {
        "inferenceCosts": { ... },
        "window": { "start": "2025-01-01T00:00:00Z", "end": "2025-01-02T00:00:00Z" }
      },
      ...
    ],
    "window": { "start": "2025-01-01T00:00:00Z", "end": "2025-01-08T00:00:00Z" }
  }
}
```

## Architecture

The feature is implemented in `pkg/inferencecost/` and consists of:

| Component | File | Responsibility |
|-----------|------|----------------|
| **Collector** | `collector.go` | Queries the OpenCost allocation layer for infrastructure costs and Prometheus for [vLLM](https://vllm.ai/) token/timing/cache metrics |
| **Calculator** | `calculator.go` | Computes blended and differentiated (input/output) cost-per-million-token rates for both cost bases |
| **Exporter** | `exporter.go` | Registers and emits the four `llm_*` Prometheus gauges |
| **QueryService** | `queryservice.go` | Handles HTTP requests for `/inferenceCost/total` and `/inferenceCost/timeseries` |
| **Runner** | `runner.go` | Drives periodic background collection for the Prometheus exporter |
| **Types / API Types** | `types.go`, `apitypes.go` | Internal and HTTP-facing data models |

Integration point: `pkg/cmd/costmodel/costmodel.go` registers the HTTP routes and initialises the collector, calculator, exporter, and runner when `INFERENCE_COST_ENABLED=true`.

## Cost Calculation Methodology

### Infrastructure Cost Collection

The Collector fetches an `AllocationSet` from OpenCost's allocation layer for the requested window. It then attributes allocation costs to each model by matching pods that carry the `INFERENCE_MODEL_LABEL` pod label. Pods that carry `INFERENCE_SHARED_INFRA_LABEL=INFERENCE_SHARED_INFRA_LABEL_VALUE` (e.g. EPP, gateway) are treated as shared infrastructure: their costs are distributed across all models proportionally (by `AllocationTotalCost` weight) for `allocation` cost basis. For `usage` cost basis, shared infra costs are excluded.

### Input/Output Cost Split

OpenCost uses **compute-time based allocation** by default:

1. Collects cumulative processing times from [vLLM](https://vllm.ai/):
   - `vllm:request_prefill_time_seconds_sum` — total time spent on input (prefill)
   - `vllm:time_per_output_token_seconds_sum` — total time spent on output (decode)
2. Allocates infrastructure cost proportionally: `InputCost = TotalCost × (PrefillTime / TotalTime)`
3. Calculates per-million rates using `EffectiveInputTokens` (cache-corrected) for input and `GenerationTokens` for output

**KV cache correction** (when `INFERENCE_KV_CACHE_BLOCK_SIZE > 0`): cached tokens are subtracted from the prompt token denominator so that the input cost per million token reflects only the tokens that required active compute.

**Fallback**: if [vLLM](https://vllm.ai/) timing metrics are unavailable, the Calculator falls back to a fixed multiplier (default 2.5×: output tokens cost 2.5× input tokens). The `allocation_method` label/field records which path was taken.

### Example Calculation

```
Model: Qwen/Qwen3-32B  |  Window: 1 hour

Infrastructure (allocation basis):
  AllocationTotalCost = $3.20/hr (GPU + shared infra share)

Token metrics from [vLLM](https://vllm.ai/):
  PromptTokens = 12,000,000
  GenerationTokens = 3,000,000
  TotalTokens = 15,000,000
  PrefillTime = 600s, DecodeTime = 600s  → each 50%

KV cache:
  CacheHitBlocks = 50,000  |  BlockSize = 16 tokens
  CachedTokens = 800,000
  EffectiveInputTokens = 12,000,000 - 800,000 = 11,200,000

Blended:    $3.20 / 15,000,000 × 1,000,000 = $0.213/M tokens
Input:      ($3.20 × 0.5) / 11,200,000 × 1,000,000 = $0.143/M effective input tokens
Output:     ($3.20 × 0.5) / 3,000,000 × 1,000,000 = $0.533/M output tokens
```

## Required [vLLM](https://vllm.ai/) Metrics

| Metric | Required for |
|--------|-------------|
| `vllm:prompt_tokens_total` | Token counts, blended cost rate |
| `vllm:generation_tokens_total` | Token counts, blended cost rate |
| `vllm:request_prefill_time_seconds_sum` | Compute-time allocation (input/output split) |
| `vllm:time_per_output_token_seconds_sum` | Compute-time allocation (input/output split) |
| `vllm:prefix_cache_hits_total` | KV cache block correction (optional) |

All metrics must carry `model_name` and `namespace` labels. Verify availability:

```bash
kubectl exec -n <namespace> <vllm-pod> -- curl -s localhost:8000/metrics | grep -E "prompt_tokens|generation_tokens|prefill_time|output_token"
```

## Troubleshooting

### No metrics appearing

1. Confirm `INFERENCE_COST_ENABLED=true` is set on the OpenCost pod
2. Check OpenCost logs: `kubectl logs -n opencost deployment/opencost | grep -i inference`
3. Verify Prometheus is reachable from OpenCost and [vLLM](https://vllm.ai/) metrics are present

### Metrics show zero cost

- Confirm model pods carry the `INFERENCE_MODEL_LABEL` label (default: `llm-d.ai/model`)
- Check that OpenCost has allocation data for the namespace: `curl "localhost:9003/allocation?window=1h&aggregate=pod&namespace=<ns>"`

### `allocation_method=multiplier` instead of `compute_time`

[vLLM](https://vllm.ai/) timing metrics are missing or zero. Check:

```bash
kubectl exec -n <namespace> <vllm-pod> -- curl -s localhost:8000/metrics | grep prefill_time
```

### Costs look too high

Check whether shared infra pods (EPP, gateway) are correctly labelled with `INFERENCE_SHARED_INFRA_LABEL`. Without this label their costs appear as unattributed allocation overhead.

## Support

- GitHub Issues: https://github.com/opencost/opencost/issues
- Slack: [#opencost](https://cloud-native.slack.com/archives/C03D56FPD4G) on CNCF Slack
