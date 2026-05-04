# AI Inference Cost Tracking

OpenCost now supports tracking infrastructure costs for AI inference workloads deployed using llm-d (or any vLLM-based deployment).

## Overview

The inference cost tracking feature calculates the infrastructure cost per token for AI models by:
1. Collecting token metrics from vLLM (prompt tokens and generation tokens)
2. Collecting GPU infrastructure costs from OpenCost's existing allocation data
3. Calculating cost per token and cost per million tokens
4. Exporting metrics to Prometheus for monitoring and alerting

## Architecture

The feature consists of four main components:

1. **Collector** (`pkg/inferencecost/collector.go`): Queries Prometheus for vLLM token metrics and GPU costs
2. **Calculator** (`pkg/inferencecost/calculator.go`): Calculates cost per token and cost per million tokens
3. **Exporter** (`pkg/inferencecost/exporter.go`): Exports calculated metrics to Prometheus
4. **Integration** (`pkg/cmd/costmodel/costmodel.go`): Integrates the collector into OpenCost's main application

## Configuration

### Environment Variables

Enable inference cost tracking by setting the following environment variables:

```bash
# Enable inference cost tracking (default: false)
INFERENCE_COST_ENABLED=true

# Collection interval in seconds (default: 60)
INFERENCE_COST_COLLECTION_INTERVAL=60

# Prometheus server endpoint (required if not already set)
PROMETHEUS_SERVER_ENDPOINT=http://prometheus-server:9090
```

### Kubernetes Deployment

Add the environment variables to your OpenCost deployment:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: opencost
spec:
  template:
    spec:
      containers:
      - name: opencost
        image: opencost/opencost:latest
        env:
        - name: INFERENCE_COST_ENABLED
          value: "true"
        - name: INFERENCE_COST_COLLECTION_INTERVAL
          value: "60"
        - name: PROMETHEUS_SERVER_ENDPOINT
          value: "http://prometheus-server:9090"
```

## Exported Metrics

The feature exports two Prometheus metrics:

### 1. `opencost_inference_total_cost`

Total infrastructure cost attributed to inference for a specific model in a specific namespace.

**Labels:**
- `model_name`: Name of the AI model (e.g., "gpt-oss-20b", "llama-2-7b")
- `model_version`: Version of the model (default: "unknown" in Phase 1)
- `namespace`: Kubernetes namespace where the model is deployed

**Example:**
```promql
opencost_inference_total_cost{model_name="random",model_version="unknown",namespace="llm-d-namespace"}
```

### 2. `opencost_inference_cost_per_million_tokens`

Cost per 1 million tokens processed (input + output) for a specific model in a specific namespace.

**Labels:**
- `model_name`: Name of the AI model
- `model_version`: Version of the model (default: "unknown" in Phase 1)
- `namespace`: Kubernetes namespace where the model is deployed

**Example:**
```promql
opencost_inference_cost_per_million_tokens{model_name="gpt-oss-20b",model_version="unknown",namespace="llm-d-namespace"}
```

## Usage Examples

### Query Current Cost Per Million Tokens

```promql
opencost_inference_cost_per_million_tokens{model_name="gpt-oss-20b",namespace="llm-d-namespace"}
```

### Calculate Total Cost Over Time

```promql
sum(rate(opencost_inference_total_cost{namespace="llm-d-namespace"}[5m])) * 300
```

### Compare Costs Across Models

```promql
opencost_inference_cost_per_million_tokens
```

### Alert on High Inference Costs

```yaml
groups:
- name: inference_costs
  rules:
  - alert: HighInferenceCost
    expr: opencost_inference_cost_per_million_tokens > 10
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "High inference cost detected"
      description: "Model {{ $labels.model_name }} in namespace {{ $labels.namespace }} has a cost of {{ $value }} per million tokens"
```

## Requirements

### vLLM Metrics

The feature requires vLLM to export the following metrics:

1. `vllm:prompt_tokens_total` - Counter of prompt tokens processed
2. `vllm:generation_tokens_total` - Counter of generation tokens produced

Both metrics must include the following labels:
- `model_name`: Name of the model
- `namespace`: Kubernetes namespace

### OpenCost Metrics

The feature uses OpenCost's existing GPU cost metrics:
- `node_gpu_hourly_cost` - Hourly cost of GPU nodes

## Cost Calculation Methodology

The cost calculation follows this approach:

1. **Token Throughput**: Calculate tokens per second using rate() over a 5-minute window
2. **GPU Cost**: Sum GPU costs for nodes running the model
3. **Cost Per Token**: Divide GPU cost by token throughput
4. **Cost Per Million Tokens**: Multiply cost per token by 1,000,000

### Example Calculation

```
Prompt tokens/sec: 100
Generation tokens/sec: 50
Total tokens/sec: 150

GPU cost: $0.50/hour = $0.000139/second

Cost per token: $0.000139 / 150 = $0.00000093
Cost per million tokens: $0.00000093 * 1,000,000 = $0.93
```

## Limitations (Phase 1)

1. **Model Version**: Currently defaults to "unknown" - will be enhanced in Phase 2
2. **KV Cache**: Does not account for KV cache hits - will be added in Phase 2
3. **Multi-GPU**: Assumes even distribution across GPUs - will be refined in Phase 2
4. **Historical Data**: Only tracks current costs - historical tracking planned for Phase 3

## Troubleshooting

### No Metrics Appearing

1. Check that `INFERENCE_COST_ENABLED=true` is set
2. Verify Prometheus endpoint is accessible
3. Check OpenCost logs for errors:
   ```bash
   kubectl logs -n opencost deployment/opencost | grep inference
   ```

### Incorrect Cost Values

1. Verify vLLM metrics are being exported correctly:
   ```bash
   kubectl exec -n <namespace> <vllm-pod> -- curl localhost:8000/metrics | grep vllm:
   ```

2. Check that GPU costs are being calculated:
   ```promql
   node_gpu_hourly_cost
   ```

3. Verify namespace labels are present on vLLM metrics

### High Memory Usage

If the collector is using too much memory, increase the collection interval:
```bash
INFERENCE_COST_COLLECTION_INTERVAL=300  # 5 minutes
```

## Future Enhancements

### Phase 2 (Planned)
- Model version detection from vLLM metrics
- KV cache hit accounting
- Per-request cost tracking
- Multi-GPU cost distribution

### Phase 3 (Planned)
- Historical cost data storage
- Cost prediction and forecasting
- Integration with OpenCost UI
- Custom cost allocation rules

## Support

For issues or questions:
- GitHub Issues: https://github.com/opencost/opencost/issues
- Slack: #opencost on CNCF Slack

## Contributing

Contributions are welcome! See the main OpenCost [CONTRIBUTING.md](../CONTRIBUTING.md) for guidelines.

## License

This feature is part of OpenCost and is licensed under the Apache License 2.0.