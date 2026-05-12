# AI Inference Cost Tracking

OpenCost supports tracking infrastructure costs for AI inference workloads deployed using llm-d (or any vLLM-based deployment).

Note: The current implementation is a proof of concept.

## Overview

The inference cost tracking feature calculates the infrastructure cost per token for AI models by:
1. Collecting token metrics from vLLM (prompt tokens and generation tokens)
2. Collecting GPU infrastructure costs from OpenCost's existing allocation data
3. Calculating cost per token and cost per million tokens
4. Exporting metrics to Prometheus for monitoring and alerting
5. Calculating differentiated costs for input vs output tokens based on actual compute time

Note: Currently only GPU costs are included in the costs.

## Exported Metrics
The feature exports the following Prometheus metrics:

### 1. `opencost_inference_total_cost`

**Hourly GPU infrastructure cost** for running a specific model in a specific namespace.

This metric represents the current hourly rate (in dollars per hour) for the GPU resources allocated to the model. It is calculated by summing the GPU costs across all pods running the model.

**Unit**: Dollars per hour ($/hour)

**Labels:**
- `model_name`: Name of the AI model (e.g., "gpt-oss-20b", "llama-2-7b")
- `model_version`: Version of the model (default: "unknown" in Phase 1)
- `namespace`: Kubernetes namespace where the model is deployed

**Example:**
```promql
# Current hourly GPU cost for the "gpt-oss-20b" model
opencost_inference_total_cost{model_name="gpt-oss-20b",model_version="unknown",namespace="llm-d-namespace"}
# Example value: 3.20 (meaning $3.20/hour)
```

**Note**: This is an hourly rate, not a cumulative cost. To calculate total cost over time, use:
```promql
# Total cost over 1 hour
opencost_inference_total_cost * 1

# Total cost over 24 hours (if rate stays constant)
opencost_inference_total_cost * 24
```

### 2. `opencost_inference_cost_per_million_tokens`

Cost per 1 million tokens processed (input + output) for a specific model in a specific namespace.

**Labels:**
- `model_name`: Name of the AI model
- `model_version`: Version of the model (default: "unknown" in Phase 1)
- `namespace`: Kubernetes namespace where the model is deployed

**Example:**

Get cost per million tokens for the model in the last time slot.
```
opencost_inference_cost_per_million_tokens{model_name="gpt-oss-20b",model_version="unknown",namespace="llm-d-namespace"}
```

Get the average cost per million tokens for the model over the past 24 hours
```
avg_over_time(opencost_inference_cost_per_million_tokens[24h])
```

### 3. `opencost_inference_input_cost_per_million_tokens`

Cost per 1 million **input (prompt) tokens** for a specific model in a specific namespace.

This metric provides the cost specifically for processing input tokens. The cost is calculated by allocating GPU infrastructure costs between input and output processing.

**Labels:**
- `model_name`: Name of the AI model
- `model_version`: Version of the model (default: "unknown" in Phase 1)
- `namespace`: Kubernetes namespace where the model is deployed
- `allocation_method`: Method used to differentiate input and output token costs:
  - `compute_time`: Costs allocated based on actual processing time from vLLM metrics (most accurate)
  - `multiplier`: Costs allocated using a fixed multiplier ratio (fallback when timing metrics unavailable)

**Example:**
```promql
# When using compute-time based allocation
opencost_inference_input_cost_per_million_tokens{
  model_name="Qwen/Qwen3-32B",
  model_version="unknown",
  namespace="llm-d-precise",
  allocation_method="compute_time"
}
# Example value: 4.50 (meaning $4.50 per 1M input tokens, based on actual processing time)
```

### 4. `opencost_inference_output_cost_per_million_tokens`

Cost per 1 million **output (generation) tokens** for a specific model in a specific namespace.

This metric provides the cost specifically for generating output tokens. Output tokens typically cost 2-3x more than input tokens due to higher compute requirements for generation.

**Labels:**
- `model_name`: Name of the AI model
- `model_version`: Version of the model (default: "unknown" in Phase 1)
- `namespace`: Kubernetes namespace where the model is deployed
- `allocation_method`: Method used to differentiate input and output token costs:
  - `compute_time`: Costs allocated based on actual processing time from vLLM metrics (most accurate)
  - `multiplier`: Costs allocated using a fixed multiplier ratio (fallback when timing metrics unavailable)

**Example:**
```promql
# When using compute-time based allocation
opencost_inference_output_cost_per_million_tokens{
  model_name="Qwen/Qwen3-32B",
  model_version="unknown",
  namespace="llm-d-precise",
  allocation_method="compute_time"
}
# Example value: 11.25 (meaning $11.25 per 1M output tokens, based on actual processing time)


### Query Differentiated Input vs Output Costs

```promql
# Compare input vs output costs for a specific model
opencost_inference_input_cost_per_million_tokens{model_name="Qwen/Qwen3-32B",namespace="llm-d-precise"}
opencost_inference_output_cost_per_million_tokens{model_name="Qwen/Qwen3-32B",namespace="llm-d-precise"}

# Calculate the cost ratio (how much more expensive output tokens are)
opencost_inference_output_cost_per_million_tokens / opencost_inference_input_cost_per_million_tokens
```

### Calculate Total Cost by Token Type

```promql
# Total cost for input tokens over time
sum(opencost_inference_input_cost_per_million_tokens * rate(vllm:prompt_tokens_total[5m]) * 300 / 1000000)

# Total cost for output tokens over time
sum(opencost_inference_output_cost_per_million_tokens * rate(vllm:generation_tokens_total[5m]) * 300 / 1000000)
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

## Differentiated Input/Output Token Costs

### Overview

OpenCost calculates separate costs for input (prompt) and output (generation) tokens because output tokens typically require 2-3x more compute resources than input processing. This provides:

1. **Accurate cost attribution**: Reflects actual resource consumption
2. **Better optimization insights**: Identify opportunities to reduce costs
3. **Industry alignment**: Matches how commercial AI APIs price tokens

### Cost Allocation Method

OpenCost uses **compute-time based allocation** to calculate differentiated costs:

**How it works:**
- Collects actual processing time from vLLM metrics:
  - `vllm:request_prefill_time_seconds` - Time spent processing input tokens
  - `vllm:time_per_output_token_seconds` - Time spent generating output tokens
- Allocates GPU costs proportionally based on time spent:
  - `InputCost = TotalGPUCost × (InputProcessingTime / TotalProcessingTime)`
  - `OutputCost = TotalGPUCost × (OutputProcessingTime / TotalProcessingTime)`
- Calculates per-token costs for each type

**Advantages:**
- Most accurate reflection of actual resource usage
- Automatically adapts to different models and workloads
- No manual tuning required

### Automatic Fallback

If vLLM timing metrics are unavailable, OpenCost automatically falls back to a **fixed multiplier approach** (default: 2.5x for output tokens). This ensures the system continues working even if timing metrics aren't available.

You'll see a warning in the logs when fallback occurs:
```
WARN: Compute-time allocation failed for model Qwen/Qwen3-32B, falling back to multiplier mode
```

### Configuration

**Default configuration (recommended):**
```yaml
env:
  - name: INFERENCE_COST_ENABLED
    value: "true"
  - name: INFERENCE_COST_ALLOCATION_MODE
    value: "compute_time"  # Default
  - name: INFERENCE_OUTPUT_TOKEN_COST_MULTIPLIER
    value: "2.5"  # Used as fallback if timing metrics unavailable
```

**Explicit multiplier mode (if preferred):**
```yaml
env:
  - name: INFERENCE_COST_ALLOCATION_MODE
    value: "multiplier"
  - name: INFERENCE_OUTPUT_TOKEN_COST_MULTIPLIER
    value: "3.0"  # Output tokens cost 3x input tokens
```

### Required vLLM Metrics

For compute-time based allocation, ensure vLLM exports these metrics:
- `vllm:request_prefill_time_seconds` - Cumulative time processing input
- `vllm:time_per_output_token_seconds` - Cumulative time generating output

These are available in recent vLLM versions. Check with:
```bash
kubectl exec -n <namespace> <vllm-pod> -- curl localhost:8000/metrics | grep prefill_time
kubectl exec -n <namespace> <vllm-pod> -- curl localhost:8000/metrics | grep time_per_output
```

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

- **Add Additional Costs** - CPU, RAM, Network, IDLE, ...
- **Model version detection from vLLM metrics** (HIGH PRIORITY) - Automatically detect and track model versions
- **KV cache hit accounting** (HIGH PRIORITY) - Account for KV cache efficiency in cost calculations
- **Workload-based cost tracking** (HIGH PRIORITY) - Track costs by workload type, application, or service to enable chargeback and showback
- **Tenant-based cost tracking** (HIGH PRIORITY) - Multi-tenant cost attribution with support for team, department, or customer-level cost allocation
- Per-request cost tracking - Track costs at the individual request level
- Multi-GPU cost distribution - More accurate cost distribution across multiple GPUs
- Historical cost data storage - Store and query historical cost data
- Integration with OpenCost UI - Display inference costs in the OpenCost web interface

## Support

For issues or questions:
- GitHub Issues: https://github.com/opencost/opencost/issues
- Slack: #opencost on CNCF Slack

## Contributing

Contributions are welcome! See the main OpenCost [CONTRIBUTING.md](../CONTRIBUTING.md) for guidelines.

## License

This feature is part of OpenCost and is licensed under the Apache License 2.0.