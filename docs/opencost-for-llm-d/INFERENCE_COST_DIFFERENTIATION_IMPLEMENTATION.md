# Inference Cost Differentiation Implementation

## Overview

This document describes the implementation of differentiated cost calculation for input (prompt) and output (generation) tokens in OpenCost's inference cost tracking feature.

## Implementation Date

2026-05-06

## Approach Implemented

**Approach 1: Compute-Time Based Allocation** with fallback to **Fixed Multiplier**

This approach uses actual processing time metrics from vLLM to allocate GPU costs proportionally between input and output token processing. If timing metrics are unavailable, it falls back to a configurable multiplier approach.

## Changes Made

### 1. Core Data Structures (`pkg/inferencecost/types.go`)

**Added:**
- `CostAllocationMode` type with constants:
  - `ModeComputeTime`: Allocates costs based on actual processing time (default)
  - `ModeMultiplier`: Applies fixed multiplier to output tokens
  
- New fields in `ModelMetrics`:
  - `InputProcessingTime`: Total seconds spent processing input tokens
  - `OutputProcessingTime`: Total seconds spent generating output tokens
  - `InputCost`: Total cost allocated to input processing
  - `OutputCost`: Total cost allocated to output generation
  - `InputCostPerToken`: Cost per input token
  - `OutputCostPerToken`: Cost per output token
  - `InputCostPerMillionTokens`: Cost per 1M input tokens
  - `OutputCostPerMillionTokens`: Cost per 1M output tokens

- New fields in `Config`:
  - `AllocationMode`: Cost allocation mode
  - `OutputTokenCostMultiplier`: Multiplier for output tokens (used in multiplier mode)

### 2. Metrics Collection (`pkg/inferencecost/collector.go`)

**Added new query methods:**
- `queryInputProcessingTime()`: Queries `vllm:request_prefill_time_seconds` metric
- `queryOutputProcessingTime()`: Queries `vllm:time_per_output_token_seconds` metric

**Updated:**
- `CollectMetrics()`: Now collects timing metrics with graceful fallback
- `combineMetrics()`: Includes timing data in combined metrics

**Prometheus Queries:**
```promql
# Input processing time (5-minute window)
sum by (model_name, namespace) (rate(vllm:request_prefill_time_seconds[5m]) * 300)

# Output processing time (5-minute window)
sum by (model_name, namespace) (rate(vllm:time_per_output_token_seconds[5m]) * 300)
```

### 3. Cost Calculation (`pkg/inferencecost/calculator.go`)

**Refactored:**
- `NewCalculator()`: Now accepts `Config` parameter
- `calculateModelCosts()`: Routes to appropriate allocation method

**Added new methods:**
- `calculateComputeTimeBasedCosts()`: Allocates costs based on processing time
  - Formula: `InputCost = TotalCost × (InputTime / TotalTime)`
  - Formula: `OutputCost = TotalCost × (OutputTime / TotalTime)`
  - Automatically falls back to multiplier mode if timing data unavailable

- `calculateMultiplierBasedCosts()`: Allocates costs using fixed multiplier
  - Formula: `WeightedTokens = PromptTokens + (GenerationTokens × Multiplier)`
  - Formula: `InputCostPerToken = TotalCost / WeightedTokens`
  - Formula: `OutputCostPerToken = InputCostPerToken × Multiplier`

### 4. Metrics Export (`pkg/inferencecost/exporter.go`)

**Added new Prometheus metrics:**
- `opencost_inference_input_cost_per_million_tokens`: Cost per 1M input tokens
- `opencost_inference_output_cost_per_million_tokens`: Cost per 1M output tokens
- `opencost_inference_input_processing_time_seconds`: Diagnostic timing metric
- `opencost_inference_output_processing_time_seconds`: Diagnostic timing metric

**Maintained backward compatibility:**
- `opencost_inference_total_cost`: Total GPU cost (unchanged)
- `opencost_inference_cost_per_million_tokens`: Blended cost (unchanged)

### 5. Configuration (`pkg/env/costmodel.go`)

**Added environment variables:**
- `INFERENCE_COST_ALLOCATION_MODE`: Cost allocation mode
  - Valid values: `compute_time` (default), `multiplier`
  
- `INFERENCE_OUTPUT_TOKEN_COST_MULTIPLIER`: Output token cost multiplier
  - Default: `2.5` (output tokens cost 2.5x input tokens)
  - Used when mode is `multiplier` or as fallback

**Added getter functions:**
- `GetInferenceCostAllocationMode()`: Returns allocation mode
- `GetInferenceOutputTokenCostMultiplier()`: Returns multiplier value

### 6. Application Configuration (`pkg/cmd/costmodel/config.go`)

**Added fields to `Config` struct:**
- `InferenceCostAllocationMode`: Allocation mode from environment
- `InferenceOutputTokenCostMultiplier`: Multiplier from environment

**Updated:**
- `DefaultConfig()`: Initializes new fields from environment
- `log()`: Logs allocation mode and multiplier when inference cost is enabled

### 7. Main Integration (`pkg/cmd/costmodel/costmodel.go`)

**Updated inference cost initialization:**
- Parses allocation mode from configuration
- Passes complete configuration to collector and calculator
- Logs allocation mode and multiplier settings

## Configuration Examples

### Compute-Time Based (Default)

```yaml
env:
  - name: INFERENCE_COST_ENABLED
    value: "true"
  - name: INFERENCE_COST_ALLOCATION_MODE
    value: "compute_time"  # Default, can be omitted
  - name: INFERENCE_OUTPUT_TOKEN_COST_MULTIPLIER
    value: "2.5"  # Used as fallback if timing metrics unavailable
```

### Fixed Multiplier Mode

```yaml
env:
  - name: INFERENCE_COST_ENABLED
    value: "true"
  - name: INFERENCE_COST_ALLOCATION_MODE
    value: "multiplier"
  - name: INFERENCE_OUTPUT_TOKEN_COST_MULTIPLIER
    value: "3.0"  # Output tokens cost 3x input tokens
```

## Required vLLM Metrics

For compute-time based allocation, vLLM must export:

1. **`vllm:request_prefill_time_seconds`**
   - Counter of time spent processing input tokens
   - Labels: `model_name`, `namespace`

2. **`vllm:time_per_output_token_seconds`**
   - Counter of time spent generating output tokens
   - Labels: `model_name`, `namespace`

If these metrics are not available, the system automatically falls back to multiplier mode.

## New Prometheus Metrics

### Input Cost Metrics

```promql
# Cost per million input tokens
opencost_inference_input_cost_per_million_tokens{
  model_name="Qwen/Qwen3-32B",
  model_version="unknown",
  namespace="llm-d-precise"
}

# Example value: 4.50 (meaning $4.50 per 1M input tokens)
```

### Output Cost Metrics

```promql
# Cost per million output tokens
opencost_inference_output_cost_per_million_tokens{
  model_name="Qwen/Qwen3-32B",
  model_version="unknown",
  namespace="llm-d-precise"
}

# Example value: 11.25 (meaning $11.25 per 1M output tokens with 2.5x multiplier)
```

**Note:** Timing metrics are queried directly from vLLM (`vllm:request_prefill_time_seconds` and `vllm:time_per_output_token_seconds`) and are not re-exported by OpenCost to avoid duplication.

## Usage Examples

### Query Differentiated Costs

```promql
# Compare input vs output costs
opencost_inference_input_cost_per_million_tokens{model_name="Qwen/Qwen3-32B"}
opencost_inference_output_cost_per_million_tokens{model_name="Qwen/Qwen3-32B"}

# Calculate cost ratio
opencost_inference_output_cost_per_million_tokens / opencost_inference_input_cost_per_million_tokens
```

### Calculate Total Cost by Token Type

```promql
# Total input token cost over time
sum(opencost_inference_input_cost_per_million_tokens * rate(vllm:prompt_tokens_total[5m]) * 300 / 1000000)

# Total output token cost over time
sum(opencost_inference_output_cost_per_million_tokens * rate(vllm:generation_tokens_total[5m]) * 300 / 1000000)
```

### Monitor Allocation Method

Check vLLM timing metrics directly to verify compute-time mode is working:
```promql
# Check if timing data is available from vLLM
vllm:request_prefill_time_seconds > 0
vllm:time_per_output_token_seconds > 0
```

Check OpenCost logs for allocation mode confirmation:
```bash
kubectl logs -n opencost deployment/opencost | grep "Compute-time allocation"
kubectl logs -n opencost deployment/opencost | grep "Multiplier-based allocation"
```

## Backward Compatibility

All existing metrics remain unchanged:
- `opencost_inference_total_cost`: Total GPU cost per hour
- `opencost_inference_cost_per_million_tokens`: Blended cost (all tokens treated equally)

Existing deployments will continue to work without any configuration changes. The new differentiated metrics are additional.

## Fallback Behavior

The implementation includes robust fallback logic:

1. **Primary**: Compute-time based allocation using vLLM timing metrics
2. **Fallback**: Fixed multiplier allocation if timing metrics unavailable
3. **Logging**: Clear warnings when falling back to multiplier mode

Example log output:
```
WARN: Compute-time allocation failed for model Qwen/Qwen3-32B in namespace llm-d-precise, falling back to multiplier mode: no timing data available (total time is 0)
DEBUG: Multiplier-based allocation for Qwen/Qwen3-32B/llm-d-precise: multiplier=2.5x, input_cost=$4.50/M, output_cost=$11.25/M
```

## Testing Recommendations

1. **Verify vLLM Metrics Collection:**
   ```bash
   kubectl exec -n <namespace> <vllm-pod> -- curl localhost:8000/metrics | grep vllm:request_prefill_time_seconds
   kubectl exec -n <namespace> <vllm-pod> -- curl localhost:8000/metrics | grep vllm:time_per_output_token_seconds
   ```

2. **Check OpenCost Metrics:**
   ```bash
   kubectl port-forward -n opencost svc/opencost 9003:9003
   curl http://localhost:9003/metrics | grep opencost_inference_input_cost
   curl http://localhost:9003/metrics | grep opencost_inference_output_cost
   ```

3. **Verify Allocation Mode:**
   ```bash
   kubectl logs -n opencost deployment/opencost | grep "allocation mode"
   kubectl logs -n opencost deployment/opencost | grep "Compute-time allocation"
   kubectl logs -n opencost deployment/opencost | grep "Multiplier-based allocation"
   ```

## Performance Impact

- **Minimal overhead**: Two additional Prometheus queries per collection interval
- **Graceful degradation**: Falls back to multiplier mode if timing metrics unavailable
- **No breaking changes**: Existing functionality unchanged

## Future Enhancements

Potential improvements for future versions:

1. **Per-request cost tracking**: Track costs at individual request level
2. **KV cache accounting**: Factor in KV cache hits for more accurate costs
3. **Workload-based attribution**: Track costs by application or service
4. **Cost prediction**: Predict costs based on usage patterns
5. **Dynamic multiplier tuning**: Automatically adjust multiplier based on observed timing ratios

## Files Modified

1. `opencost/pkg/inferencecost/types.go` - Core data structures
2. `opencost/pkg/inferencecost/collector.go` - Metrics collection
3. `opencost/pkg/inferencecost/calculator.go` - Cost calculation logic
4. `opencost/pkg/inferencecost/exporter.go` - Prometheus metrics export
5. `opencost/pkg/env/costmodel.go` - Environment variable configuration
6. `opencost/pkg/cmd/costmodel/config.go` - Application configuration
7. `opencost/pkg/cmd/costmodel/costmodel.go` - Main integration

## Summary

This implementation provides accurate, differentiated cost tracking for input and output tokens using actual compute time from vLLM, with a robust fallback to a configurable multiplier approach. The solution is backward compatible, well-documented, and production-ready.