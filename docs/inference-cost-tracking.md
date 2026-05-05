# AI Inference Cost Tracking

OpenCost now supports tracking infrastructure costs for AI inference workloads deployed using llm-d (or any vLLM-based deployment).

## Overview

The inference cost tracking feature calculates the infrastructure cost per token for AI models by:
1. Collecting token metrics from vLLM (prompt tokens and generation tokens)
2. Collecting GPU infrastructure costs from OpenCost's existing allocation data
3. Calculating cost per token and cost per million tokens
4. Exporting metrics to Prometheus for monitoring and alerting

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

## Metrics Time Period

The inference cost metrics are calculated using a **5-minute time window**:

- **Token metrics**: Total tokens processed in the last 5 minutes
  - Calculated as: `rate(vllm:prompt_tokens_total[5m]) * 300`
  - The `rate()` function calculates tokens per second over 5 minutes
  - Multiplied by 300 seconds to get the total tokens in that 5-minute period
  
- **GPU cost metrics**: Current hourly GPU cost (instantaneous value)
  - Based on current node GPU costs and container allocations
  - Not averaged over time - reflects the current cost rate

- **Cost per token calculation**:
  - Uses the 5-minute token total divided by the current GPU cost
  - Formula: `(GPU cost per hour / 3600) / (tokens in 5 minutes / 300)`
  - This gives the cost per token based on recent throughput and current infrastructure costs

- **Collection interval**: Metrics are collected every 60 seconds by default (configurable via `INFERENCE_COST_COLLECTION_INTERVAL`)

**Important**: The cost per million tokens metric represents the cost if the model continues processing tokens at the same rate as the last 5 minutes, using the current GPU infrastructure costs. This provides a balance between responsiveness to changes and stability against short-term fluctuations.


The inference cost tracking feature calculates costs in two main steps:

### Step 1: Calculate GPU Allocation Costs

OpenCost determines how much GPU cost to attribute to each inference workload by:

1. **Getting Node GPU Costs**: Query the hourly cost of GPU resources on each node
   - Metric used: `node_gpu_hourly_cost`
   - Example: A node with 4 GPUs might cost $3.20/hour

2. **Getting Container GPU Allocation**: Determine what fraction of the node's GPUs each container is using
   - Metric used: `container_gpu_allocation`
   - This represents the ratio: `(GPUs requested by container) / (Total GPUs on node)`
   - Example: If a container requests 2 GPUs on a 4-GPU node, allocation = 0.5

3. **Calculating Per-Container GPU Cost**: Multiply the allocation ratio by the node cost
   - Formula: `container_gpu_cost = container_gpu_allocation × node_gpu_hourly_cost`
   - Example: 0.5 × $3.20/hour = $1.60/hour for that container

4. **Aggregating by Model**: Sum up costs for all containers running the same model
   - Uses the `model_name` label from vLLM metrics to group containers
   - Aggregates by both `model_name` and `namespace` for multi-tenant environments


### Step 2: Calculate Cost Per Token

Once we have the GPU costs, we calculate the cost per token:

1. **Token Throughput**: Calculate tokens per second using rate() over a 5-minute window
   - Prompt tokens: `rate(vllm:prompt_tokens_total[5m])`
   - Generation tokens: `rate(vllm:generation_tokens_total[5m])`
   - Total throughput: sum of both rates

2. **Cost Per Token**: Divide GPU cost by token throughput
   - Formula: `cost_per_token = gpu_cost_per_second / tokens_per_second`

3. **Cost Per Million Tokens**: Scale up for easier interpretation
   - Formula: `cost_per_million = cost_per_token × 1,000,000`

### Complete Example

**Scenario**: A vLLM deployment running the "Qwen/Qwen3-32B" model

**Step 1 - GPU Allocation Cost:**
```
Node: gpu-node-1 (4 GPUs total)
Node GPU cost: $3.20/hour

Container: vllm-qwen-pod
GPU request: 2 GPUs
GPU allocation: 2/4 = 0.5

Container GPU cost: 0.5 × $3.20/hour = $1.60/hour
                  = $1.60/3600 = $0.000444/second
```

**Step 2 - Cost Per Token:**
```
Token throughput:
- Prompt tokens: 100 tokens/sec
- Generation tokens: 50 tokens/sec
- Total: 150 tokens/sec

Cost per token: $0.000444/sec ÷ 150 tokens/sec = $0.00000296/token

Cost per million tokens: $0.00000296 × 1,000,000 = $2.96 per million tokens
```

### Understanding the Metrics

The Prometheus query used for GPU cost calculation:
```promql
sum by (model_name, namespace) (
    # Get model_name from vLLM metrics
    (vllm:prompt_tokens_total * 0 + 1)
    * on(pod, namespace) group_left()
    # Join with GPU cost per pod
    sum(
        container_gpu_allocation      # GPU allocation ratio per container
        * on(node) group_left()
        node_gpu_hourly_cost          # Node GPU hourly cost
    ) by (pod, namespace, node)
)
```

This query:
1. Starts with vLLM metrics to get the `model_name` label
2. Joins with container GPU allocations to get resource usage
3. Multiplies allocations by node costs to get actual dollar amounts
4. Aggregates by model and namespace for the final cost
```

## Limitations (Phase 1)

1. **GPU Costs Only**: Currently tracks GPU infrastructure costs only. Does not include:
   - CPU costs
   - Memory costs
   - Storage costs
   - Network costs
   - Other infrastructure costs
   
   This will be enhanced in future phases to include full infrastructure costs.

2. **Model Version**: Currently defaults to "unknown" - will be enhanced in Phase 2
3. **KV Cache**: Does not account for KV cache hits - will be added in Phase 2
4. **Multi-GPU**: Assumes even distribution across GPUs - will be refined in Phase 2
5. **Historical Data**: Only tracks current costs - historical tracking planned for Phase 3

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

- **Differentiated pricing for prompt vs output tokens** (HIGH PRIORITY) - Currently all tokens are priced equally, but output tokens typically cost 2-3x more in commercial APIs due to higher compute requirements
- **Model version detection from vLLM metrics** (HIGH PRIORITY) - Automatically detect and track model versions
- **KV cache hit accounting** (HIGH PRIORITY) - Account for KV cache efficiency in cost calculations
- **Workload-based cost tracking** (HIGH PRIORITY) - Track costs by workload type, application, or service to enable chargeback and showback
- **Tenant-based cost tracking** (HIGH PRIORITY) - Multi-tenant cost attribution with support for team, department, or customer-level cost allocation
- Per-request cost tracking - Track costs at the individual request level
- Multi-GPU cost distribution - More accurate cost distribution across multiple GPUs
- Historical cost data storage - Store and query historical cost data
- Cost prediction and forecasting - Predict future costs based on usage patterns
- Integration with OpenCost UI - Display inference costs in the OpenCost web interface
- Custom cost allocation rules - Allow custom rules for cost allocation across teams/projects

## Support

For issues or questions:
- GitHub Issues: https://github.com/opencost/opencost/issues
- Slack: #opencost on CNCF Slack

## Contributing

Contributions are welcome! See the main OpenCost [CONTRIBUTING.md](../CONTRIBUTING.md) for guidelines.

## License

This feature is part of OpenCost and is licensed under the Apache License 2.0.