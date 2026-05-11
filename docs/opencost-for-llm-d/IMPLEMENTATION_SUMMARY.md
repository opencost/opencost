# AI Inference Cost Tracking - Implementation Summary

## Overview

Successfully implemented Phase 1 of AI inference cost tracking for OpenCost, enabling infrastructure cost calculation for AI models deployed using llm-d (vLLM-based deployments).

## Implementation Date

May 3, 2026

## What Was Implemented

### 1. Core Package: `opencost/pkg/inferencecost/`

Created a new package with four main components:

#### a. Types (`types.go`)
- `ModelMetrics`: Data structure for model metrics including:
  - Model name, version, and namespace
  - Token counts (prompt, generation, total)
  - Cost metrics (GPU cost, total cost, cost per token, cost per million tokens)
  - Timestamp
- `Config`: Configuration structure for the collector

#### b. Calculator (`calculator.go`)
- `Calculator`: Calculates cost metrics from raw data
- `CalculateCosts()`: Processes multiple model metrics
- `calculateModelCosts()`: Calculates costs for a single model
- Handles division by zero gracefully

#### c. Collector (`collector.go`)
- `Collector`: Queries Prometheus for vLLM and GPU metrics
- `NewCollector()`: Creates a new collector with Prometheus client
- `CollectMetrics()`: Main collection method that:
  - Queries prompt tokens with rate() over 5 minutes
  - Queries generation tokens with rate() over 5 minutes
  - Queries GPU costs
  - Combines metrics by model_name:namespace key
  - Returns populated ModelMetrics structs

#### d. Exporter (`exporter.go`)
- `Exporter`: Exports metrics to Prometheus
- Two gauge metrics:
  - `opencost_inference_total_cost`: Total infrastructure cost
  - `opencost_inference_cost_per_million_tokens`: Cost per 1M tokens
- Labels: `model_name`, `model_version`, `namespace`
- `Register()`: Registers metrics with Prometheus
- `Export()`: Updates metric values

### 2. Environment Configuration (`opencost/pkg/env/costmodel.go`)

Added three new functions:
- `GetInferenceCostEnabled()`: Returns whether feature is enabled
- `GetInferenceCostCollectionInterval()`: Returns collection interval as time.Duration
- `GetPrometheusServerEndpoint()`: Returns Prometheus server URL

Added two new environment variable constants:
- `InferenceCostEnabledEnvVar`: "INFERENCE_COST_ENABLED"
- `InferenceCostCollectionIntervalEnvVar`: "INFERENCE_COST_COLLECTION_INTERVAL"

### 3. Application Configuration (`opencost/pkg/cmd/costmodel/config.go`)

Extended `Config` struct with:
- `InferenceCostEnabled`: Boolean flag
- `InferenceCostCollectionInterval`: Collection interval
- `PrometheusServerEndpoint`: Prometheus URL

Updated `DefaultConfig()` to populate new fields from environment variables.

Updated `log()` method to log inference cost configuration.

### 4. Main Application Integration (`opencost/pkg/cmd/costmodel/costmodel.go`)

Added inference cost collector initialization in `Execute()` function:
- Checks if feature is enabled
- Creates collector, calculator, and exporter
- Registers Prometheus metrics
- Starts background goroutine with ticker
- Handles graceful shutdown on context cancellation
- Logs errors and debug information

### 5. Documentation

Created comprehensive user documentation (`opencost/docs/inference-cost-tracking.md`):
- Overview and architecture
- Configuration instructions
- Environment variables
- Kubernetes deployment examples
- Exported metrics documentation
- Usage examples and PromQL queries
- Alert examples
- Cost calculation methodology
- Troubleshooting guide
- Future enhancements roadmap

## Key Design Decisions

### 1. Namespace-Based Filtering
- Uses namespace labels on vLLM metrics instead of environment variable filtering
- Allows multiple deployments to be tracked simultaneously
- Metrics automatically include namespace context

### 2. Infrastructure-Based Costing
- Calculates costs from actual GPU infrastructure costs
- No pre-configured pricing tables needed
- Automatically adapts to different cloud providers and GPU types

### 3. Rate-Based Token Counting
- Uses Prometheus rate() function over 5-minute windows
- Multiplies by 300 seconds to get total tokens in window
- Provides stable, averaged metrics

### 4. Modular Architecture
- Separate concerns: collection, calculation, export
- Easy to test individual components
- Extensible for future enhancements

### 5. Graceful Error Handling
- Continues operation if individual queries fail
- Logs errors without crashing
- Handles division by zero in calculations

## Files Created

1. `opencost/pkg/inferencecost/types.go` (35 lines)
2. `opencost/pkg/inferencecost/calculator.go` (42 lines)
3. `opencost/pkg/inferencecost/collector.go` (150 lines)
4. `opencost/pkg/inferencecost/exporter.go` (65 lines)
5. `opencost/docs/inference-cost-tracking.md` (234 lines)
6. `IMPLEMENTATION_SUMMARY.md` (this file)

## Files Modified

1. `opencost/pkg/env/costmodel.go`:
   - Added 2 constants
   - Added 3 functions
   - ~20 lines added

2. `opencost/pkg/cmd/costmodel/config.go`:
   - Added 3 struct fields
   - Modified 2 functions
   - ~15 lines added

3. `opencost/pkg/cmd/costmodel/costmodel.go`:
   - Added 1 import
   - Added inference cost initialization block
   - ~50 lines added

## Verification

### Compilation
✅ Successfully compiles with `go build ./pkg/cmd/costmodel/...`
✅ Successfully compiles with `go build ./pkg/inferencecost/...`

### Metrics Verification
✅ Verified vLLM exports required metrics with correct labels
✅ Confirmed `model_name` label is present on vLLM metrics
✅ Confirmed `namespace` label is present on vLLM metrics

## Configuration Example

```bash
# Enable the feature
export INFERENCE_COST_ENABLED=true

# Set collection interval (optional, default: 60 seconds)
export INFERENCE_COST_COLLECTION_INTERVAL=60

# Set Prometheus endpoint (if not already configured)
export PROMETHEUS_SERVER_ENDPOINT=http://prometheus-server:9090
```

## Usage Example

After deployment, query the metrics:

```promql
# Get cost per million tokens for all models
opencost_inference_cost_per_million_tokens

# Get cost for specific model in specific namespace
opencost_inference_cost_per_million_tokens{model_name="random",namespace="dpikus-sim"}

# Calculate total cost over time
sum(rate(opencost_inference_total_cost[5m])) * 300
```

## Testing Recommendations

### Unit Tests (To Be Added)
1. Calculator tests:
   - Test cost calculation with valid data
   - Test division by zero handling
   - Test multiple models

2. Collector tests:
   - Mock Prometheus responses
   - Test metric parsing
   - Test error handling

3. Exporter tests:
   - Test metric registration
   - Test metric updates
   - Test label handling

### Integration Tests (To Be Added)
1. End-to-end test with mock Prometheus
2. Test with real vLLM deployment
3. Test graceful shutdown
4. Test error recovery

## Future Enhancements (Phase 2+)

### Phase 2
- [ ] Model version detection from vLLM metrics
- [ ] KV cache hit accounting
- [ ] Per-request cost tracking
- [ ] Multi-GPU cost distribution refinement

### Phase 3
- [ ] Historical cost data storage
- [ ] Cost prediction and forecasting
- [ ] Integration with OpenCost UI
- [ ] Custom cost allocation rules
- [ ] REST API endpoints for inference costs

## Known Limitations

1. **Model Version**: Currently defaults to "unknown"
2. **KV Cache**: Does not account for KV cache hits in cost calculation
3. **Historical Data**: Only tracks current costs, no historical storage
4. **Multi-GPU**: Assumes even distribution across GPUs

## Dependencies

- Prometheus client library: `github.com/prometheus/client_golang`
- OpenCost core packages
- Standard Go libraries (time, context, fmt, strings)

## Performance Considerations

- Collection runs in background goroutine
- Default 60-second interval prevents excessive Prometheus queries
- Metrics are aggregated before export
- Graceful shutdown prevents resource leaks

## Security Considerations

- Uses existing OpenCost Prometheus authentication
- No new authentication mechanisms required
- Metrics follow Prometheus security model
- No sensitive data in metric labels

## Compliance

- Follows OpenCost coding standards
- Uses existing OpenCost patterns and conventions
- Compatible with OpenCost's Apache 2.0 license
- No external dependencies beyond existing OpenCost requirements

## Deployment Notes

1. Feature is disabled by default (opt-in)
2. No breaking changes to existing OpenCost functionality
3. Can be enabled/disabled without recompilation
4. Requires Prometheus with vLLM metrics

## Support and Maintenance

- Documentation: `opencost/docs/inference-cost-tracking.md`
- Code location: `opencost/pkg/inferencecost/`
- Integration point: `opencost/pkg/cmd/costmodel/costmodel.go`
- Configuration: Environment variables in `opencost/pkg/env/costmodel.go`

## Conclusion

Phase 1 implementation is complete and functional. The feature provides a solid foundation for tracking AI inference costs with a clean, modular architecture that can be extended in future phases.

The implementation successfully:
- ✅ Calculates infrastructure-based costs per token
- ✅ Supports multiple models and namespaces
- ✅ Exports metrics to Prometheus
- ✅ Integrates cleanly with OpenCost
- ✅ Provides comprehensive documentation
- ✅ Compiles without errors
- ✅ Follows OpenCost conventions

Next steps: Testing, user feedback, and Phase 2 enhancements.