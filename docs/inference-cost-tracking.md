# AI Inference Cost Tracking

OpenCost tracks infrastructure costs for AI inference workloads running [vLLM](https://vllm.ai/)-based models ([llm-d](https://llm-d.ai/) and compatible deployments). It exposes costs both as Prometheus metrics and as queryable REST API endpoints.

## Overview

The inference cost tracking feature:
1. Collects token metrics from [vLLM](https://vllm.ai/) via Prometheus (`prompt_tokens_total`, `generation_tokens_total`, prefill/decode timing, KV cache hits)
2. Collects infrastructure costs (GPU, CPU, RAM, shared infra) from OpenCost's allocation layer
3. Calculates blended and differentiated (input/output) cost per million tokens under two cost bases: `allocation` and `usage`
4. Exports Inference Prometheus gauge metrics per model/namespace
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
| `INFERENCE_MODEL_LABEL` | `llm-d.ai/model` | Pod label whose value is the vLLM model name. **Must match the `model_name` label on vLLM Prometheus metrics.** See [Model label](#model-label) for details. |
| `INFERENCE_SHARED_INFRA_LABEL` | `llm-d.ai/inference-shared` | Pod label key identifying shared infra pods (EPP, gateway). See [Shared infrastructure label](#shared-infrastructure-label) for details. |
| `INFERENCE_SHARED_INFRA_LABEL_VALUE` | `true` | Label value that marks a pod as shared infra. See [Shared infrastructure label](#shared-infrastructure-label) for details. |
| `INFERENCE_COLLECTION_INTERVAL` | `2m` | Background collection interval |

### Kubernetes Deployment Example

```yaml
env:
  - name: INFERENCE_COST_ENABLED
    value: "true"
  - name: INFERENCE_MODEL_LABEL
    value: "llm-d.ai/model"
```

## Cost Bases

OpenCost computes costs under two distinct bases, surfaced on every metric and API response:

| Cost Basis | Label / Value | Description |
|------------|---------------|-------------|
| **Allocation** | `cost_basis=allocation` | `max(request, usage) × price` + idle share + shared infra share. **Reconciles to the infrastructure bill.** |
| **Usage** | `cost_basis=usage` | Actual resource consumption only. Does **not** reconcile to the bill; idle and shared infrastructure costs are excluded. |

Use `allocation` for chargeback/showback and bill reconciliation. Use `usage` for pure workload efficiency analysis.

## Prometheus Metrics

When `INFERENCE_COST_ENABLED=true`, OpenCost registers and emits inference gauge metrics every collection interval. All metrics carry `model_name`, `model_version`, `namespace`, and `cost_basis` labels.

Note: `pod`, `controller`, `controller_kind`, `container` aggregation are available via [REST APIs](#rest-api-endpoints).


**Hourly infrastructure cost** attributed to a model.

**Labels:** `model_name`, `model_version`, `namespace`, `cost_basis`

This is an instantaneous hourly rate ($/hour), not a cumulative counter.

```promql
# Current hourly cost for a model
llm_total_cost{model_name="Qwen/Qwen3-32B", cost_basis="allocation"}

# Estimated 24-hour cost if current rate continues (real-time projection)
llm_total_cost{model_name="Qwen/Qwen3-32B", cost_basis="allocation"} * 24

# Actual 24-hour cost based on historical average (more accurate for reporting)
avg_over_time(llm_total_cost{model_name="Qwen/Qwen3-32B", cost_basis="allocation"}[24h]) * 24
```

### `llm_cost_per_million_tokens`

**Cost per 1M tokens.** This metric serves dual purposes based on the `phase` label:

- **Without `phase` label (blended):** Combined cost for all tokens (prompt + generation)
- **`phase=prompt`:** Cost per 1M delivered input tokens (uses `promptTokens` as denominator; see `llm_cache_savings_fraction` for KV cache utilization)
- **`phase=generation`:** Cost per 1M output tokens

**Labels:** `model_name`, `model_version`, `namespace`, `cost_basis`, `phase`, `allocation_method`

The `phase` label distinguishes between:
- *(empty)* — Blended cost across all tokens
- `prompt` — Input/prompt token cost
- `generation` — Output/generation token cost

The `allocation_method` label (present only when `phase` is set) indicates how the input/output split was calculated:

| Value | Meaning |
|-------|---------|
| `compute_time` | Cost split proportionally by vLLM prefill/decode time. KV cache utilization is reported separately in `llm_cache_savings_fraction`. |
| `prefix_caching_off` | Same time-based split but prefix caching is explicitly disabled on the vLLM instance — `llm_cache_savings_fraction` will be zero by configuration. |
| `multiplier` | Fixed output/input cost ratio (vLLM timing metrics unavailable; default ratio 2.5×). |
| *(empty)* | No tokens processed or total cost is zero (allocation join failed — see [Labeling Requirements](#labeling-requirements)). |

```promql
# Current blended cost per 1M tokens
llm_cost_per_million_tokens{model_name="Qwen/Qwen3-32B", cost_basis="allocation", phase=""}

# Input (prompt) cost per 1M delivered input tokens
llm_cost_per_million_tokens{
  model_name="Qwen/Qwen3-32B",
  cost_basis="allocation",
  phase="prompt",
  allocation_method="compute_time"
}

# Output (generation) cost per 1M tokens
llm_cost_per_million_tokens{
  model_name="Qwen/Qwen3-32B",
  cost_basis="allocation",
  phase="generation"
}

# Input vs output cost ratio
llm_cost_per_million_tokens{phase="generation"} / llm_cost_per_million_tokens{phase="prompt"}

# Average blended cost over the past 24 hours
avg_over_time(llm_cost_per_million_tokens{model_name="Qwen/Qwen3-32B", phase=""}[24h])

# Compare models side-by-side (blended)
llm_cost_per_million_tokens{cost_basis="allocation", phase=""}

# Sum input and output costs
sum by (model_name, namespace) (llm_cost_per_million_tokens{phase=~"prompt|generation"})
```

### `llm_cache_savings_fraction`

**Fraction of prompt tokens served from the KV cache** (range 0–1). A value of `0.9` means 90% of prompt tokens were cache hits and required no prefill computation.

**Labels:** `model_name`, `model_version`, `namespace`

Zero when prefix caching is disabled (`allocation_method=prefix_caching_off` on `llm_cost_per_million_tokens`) or when no cache hits occurred in the window.

```promql
# Current cache hit fraction for a model
llm_cache_savings_fraction{model_name="Qwen/Qwen3-32B"}

# Models with less than 50% cache hit rate (potential tuning opportunity)
llm_cache_savings_fraction < 0.5

# Cache hit rate trend over 24 hours
avg_over_time(llm_cache_savings_fraction{model_name="Qwen/Qwen3-32B"}[24h])

# Compare cache utilization across all models
sort_desc(llm_cache_savings_fraction)
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
| `aggregate` | No | Comma-separated dimensions: `model_name`, `model_version`, `namespace`, `cluster`, `pod`, `controller`, `controller_kind`, `container` |
| `accumulate` | No | Step size within the window: `hour`, `day`, `week`, `month` (results are then accumulated into one total) |
| `filter` | No | `prop:value` pairs joined with `+` for AND logic, e.g. `namespace:default+model_name:llama3` |
| `idle` | No | `shareWeighted/separate/shareEven` |

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
          "modelName": "Qwen/Qwen3-32B"
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
        "cacheSavingsFraction": 0.067,
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

## Labeling Requirements

Correct pod labeling is critical for cost attribution. OpenCost joins infrastructure costs (from the Kubernetes allocation layer) with token metrics (from Prometheus) using the model name and namespace as the join key.

### Model label

Every vLLM inference pod **must** carry a label whose key matches `INFERENCE_MODEL_LABEL` (default: `llm-d.ai/model`) and whose **value exactly matches the `model_name` label on the vLLM Prometheus metrics**.

```yaml
# Pod spec
metadata:
  labels:
    llm-d.ai/model: "Qwen3-32B"   # must match --served-model-name (or --model if --served-model-name is not set)
```

`--served-model-name` controls the exact string vLLM uses as the `model_name` label in Prometheus metrics. Set the pod label to that same value. If `--served-model-name` is not set, vLLM uses the fully-qualified `--model` path (e.g. `Qwen/Qwen3-32B`) as `model_name`, so the pod label must match that instead.

If this label is missing or the value differs from `model_name` in vLLM metrics, the allocation join fails: **token counts will appear in the API response but all cost fields will be zero** and `allocationMethod` will be empty.

OpenCost attempts to reconcile fully-qualified model names (e.g. `org/model`) against short names (`model`) automatically, but the namespace must always match exactly.

### Diagnosing a labeling mismatch

```bash
# Check what label value OpenCost sees in the allocation layer
curl "localhost:9003/allocation?window=1h&aggregate=label:llm-d.ai/model&namespace=<ns>" \
  | jq '.data[0] | keys'

# Check what model_name vLLM is reporting in Prometheus
curl "http://prometheus:9090/api/v1/query?query=vllm:prompt_tokens_total{namespace=\"<ns>\"}" \
  | jq '.data.result[].metric.model_name'
```

If the values differ, update the pod label to match the vLLM `model_name`. OpenCost also logs a warning when it detects and auto-corrects a mismatch:

```
InferenceCost: remapping metric key "org/model:namespace" → "model:namespace" (model-name mismatch with allocation label)
```

### Shared infrastructure label

Pods for shared infrastructure (EPP, gateway, routers) that serve multiple models should be labelled with `INFERENCE_SHARED_INFRA_LABEL` so their costs are distributed proportionally across all models rather than appearing as unattributed overhead:

```yaml
metadata:
  labels:
    llm-d.ai/inference-shared: "true"
```

## Architecture

The feature is implemented in `pkg/inferencecost/` and consists of:

| Component | File | Responsibility |
|-----------|------|----------------|
| **Collector** | `collector.go` | Queries the OpenCost allocation layer for infrastructure costs and Prometheus for [vLLM](https://vllm.ai/) token/timing/cache metrics |
| **Calculator** | `calculator.go` | Computes blended and differentiated (input/output) cost-per-million-token rates for both cost bases |
| **Exporter** | `exporter.go` | Registers and emits the `llm_*` Prometheus gauges |
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
   - `vllm:request_time_per_output_token_seconds_sum` — total time spent on output (decode)
2. Allocates infrastructure cost proportionally: `InputCost = TotalCost × (PrefillTime / TotalTime)`
3. Calculates per-million rates using `PromptTokens` for input (all delivered prompt tokens) and `GenerationTokens` for output

**KV cache savings** are reported in `cacheSavingsFraction` (`cachedTokens / promptTokens`, range 0–1). The dollar cost split already reflects cache savings implicitly — when the KV cache serves tokens without prefill work, prefill time is lower, so less cost is attributed to input. `cacheSavingsFraction` makes this benefit explicit and user-readable.

`cacheSavingsFraction` is sourced directly from `vllm:prefix_cache_hits_total`, which reports cached **tokens** (not blocks). It is non-zero when cache hits were recorded in the window and the metric is available. `vllm:cache_config_info` is queried separately, but only to detect whether prefix caching is explicitly disabled — not for any arithmetic.

**Fallback**: if [vLLM](https://vllm.ai/) timing metrics are unavailable, the Calculator falls back to a fixed multiplier (default 2.5×: output tokens cost 2.5× input tokens).

The `allocationMethod` field records which path was taken for each result (see [allocation_method values](#llm_input_cost_per_million_tokens)).

### Example Calculation

```
Model: Qwen/Qwen3-32B  |  Window: 1 hour

Infrastructure (allocation basis):
  AllocationTotalCost = $3.20/hr (GPU + shared infra share)

Token metrics from vLLM:
  PromptTokens = 12,000,000
  GenerationTokens = 3,000,000
  TotalTokens = 15,000,000
  PrefillTime = 600s, DecodeTime = 600s  → each 50%

KV cache:
  CachedTokens = 800,000  (from vllm:prefix_cache_hits_total — token-level counter)
  CacheSavingsFraction = 800,000 / 12,000,000 = 6.7%

Blended:              $3.20 / 15,000,000 × 1,000,000 = $0.213/M tokens
Input (delivered):    ($3.20 × 0.5) / 12,000,000 × 1,000,000 = $0.133/M prompt tokens
Output:               ($3.20 × 0.5) / 3,000,000 × 1,000,000 = $0.533/M output tokens
Cache savings:        6.7% of prompt tokens served from KV cache
```

## Required [vLLM](https://vllm.ai/) Metrics

| Metric | Required for |
|--------|-------------|
| `vllm:prompt_tokens_total` | Token counts, blended cost rate |
| `vllm:generation_tokens_total` | Token counts, blended cost rate |
| `vllm:request_prefill_time_seconds_sum` | Compute-time allocation (input/output split) |
| `vllm:request_time_per_output_token_seconds_sum` | Compute-time allocation (input/output split) |
| `vllm:prefix_cache_hits_total` | `cacheSavingsFraction` (token-level counter; optional) |
| `vllm:cache_config_info` | `prefix_caching_off` detection (from `enable_prefix_caching` label; optional) |

All metrics must carry `model_name` and `namespace` labels. Verify availability:

```bash
kubectl exec -n <namespace> <vllm-pod> -- curl -s localhost:8000/metrics | grep -E "prompt_tokens|generation_tokens|prefill_time|output_token"
```

## Troubleshooting

### No metrics appearing

1. Confirm `INFERENCE_COST_ENABLED=true` is set on the OpenCost pod
2. Check OpenCost logs: `kubectl logs -n opencost deployment/opencost | grep -i inference`
3. Verify Prometheus is reachable from OpenCost and [vLLM](https://vllm.ai/) metrics are present

### Metrics show zero cost / `allocationMethod` is empty

This means the allocation join failed — token data was found in Prometheus but no matching pod cost was found in the allocation layer. See [Labeling Requirements](#labeling-requirements).

Quick diagnosis:
```bash
# What label values does the allocation layer see?
curl "localhost:9003/allocation?window=1h&aggregate=label:llm-d.ai/model&namespace=<ns>" \
  | jq '.data[0] | keys'

# What model_name does vLLM report?
curl "http://prometheus:9090/api/v1/query?query=vllm:prompt_tokens_total{namespace=\"<ns>\"}" \
  | jq '.data.result[].metric.model_name'
```

If the values differ, update the pod label on the vLLM deployment to match.

### `allocationMethod=multiplier` instead of `compute_time`

[vLLM](https://vllm.ai/) timing metrics are missing or zero. Check:

```bash
kubectl exec -n <namespace> <vllm-pod> -- curl -s localhost:8000/metrics | grep prefill_time
```

### `cacheSavingsFraction` is zero but prefix caching is expected to be active

One of the following:
- **Prefix caching is disabled** on this vLLM instance (`enable_prefix_caching=false` in `vllm:cache_config_info`) — `allocationMethod` will be `prefix_caching_off`, which is accurate and expected
- **No cache hits occurred** in this window despite prefix caching being enabled — normal for low-traffic or first-request windows
- **`vllm:prefix_cache_hits_total` metric is missing or zero** — check that vLLM is emitting it. This metric reports cached tokens directly; if unavailable, `cacheSavingsFraction` will be zero.
- **`vllm:cache_config_info` metric is missing** — this only affects `prefix_caching_off` detection, not `cacheSavingsFraction`. OpenCost logs a warning if the metric exists but the pod-label join fails:
  ```
  InferenceCost: vllm:cache_config_info exists in Prometheus but the join with
  vllm:prompt_tokens_total produced no results — likely a pod-label mismatch
  ```

### Costs look too high

- Check utilization: `costBasis=allocation` includes idle time. A GPU reserved for an hour but processing very few tokens will show a high $/M token rate. 
- Check whether shared infra pods (EPP, gateway) are correctly labelled with `INFERENCE_SHARED_INFRA_LABEL`. Without this label their costs appear as unattributed allocation overhead.

## Support

- GitHub Issues: https://github.com/opencost/opencost/issues
- Slack: [#opencost](https://cloud-native.slack.com/archives/C03D56FPD4G) on CNCF Slack
