# vLLM Inference Metrics Fix - Complete Guide

## Problem Summary

OpenCost inference cost tracking is enabled (`INFERENCE_COST_ENABLED=true`) but not generating metrics because:

1. **Prometheus is not scraping vLLM pods** - No scrape configuration exists for vLLM
2. **vLLM metrics lack namespace label** - OpenCost expects `namespace` label on all metrics
3. **No metrics flowing to OpenCost** - Without proper scraping and labeling, OpenCost cannot calculate costs

## Root Cause

Your vLLM pod (`vllm-itay-minimax-5698f79b94-z2sqz`) is exposing metrics correctly:
```
vllm:prompt_tokens_total{engine="0",model_name="MiniMaxAI/MiniMax-M2.7"} 2.6580772e+07
vllm:generation_tokens_total{engine="0",model_name="MiniMaxAI/MiniMax-M2.7"} 155962.0
```

But these metrics are:
- ✅ Available at `http://pod-ip:8000/metrics`
- ✅ Have `model_name` label
- ❌ **Not being scraped by Prometheus**
- ❌ **Missing `namespace` label** (required by OpenCost)

## Solution: Add vLLM Scrape Job to Prometheus

We'll add a dedicated Prometheus scrape job that:
1. Discovers all vLLM pods across all namespaces using the `llm-d.ai/role=decode` label
2. Scrapes metrics from port 8000
3. Adds the `namespace` label to all metrics

## Implementation Steps

### Step 1: Backup Current Configuration

```bash
oc get configmap prometheus-opencost-server -n opencost -o yaml > prometheus-backup-$(date +%Y%m%d).yaml
```

### Step 2: Edit Prometheus ConfigMap

```bash
oc edit configmap prometheus-opencost-server -n opencost
```

### Step 3: Add vLLM Scrape Job

In the editor, find the `scrape_configs:` section and add this job **BEFORE** the `opencost` job:

```yaml
    - job_name: 'vllm-inference'
      honor_labels: true
      scrape_interval: 30s
      scrape_timeout: 10s
      metrics_path: /metrics
      kubernetes_sd_configs:
      - role: pod
      relabel_configs:
      # Keep only pods with llm-d.ai/role=decode label
      - source_labels: [__meta_kubernetes_pod_label_llm_d_ai_role]
        action: keep
        regex: decode
      # Add namespace label to all metrics
      - source_labels: [__meta_kubernetes_namespace]
        target_label: namespace
        action: replace
      # Add pod name for debugging
      - source_labels: [__meta_kubernetes_pod_name]
        target_label: pod
        action: replace
      # Add node name for debugging
      - source_labels: [__meta_kubernetes_pod_node_name]
        target_label: node
        action: replace
      # Set scrape port to 8000 (vLLM metrics port)
      - source_labels: [__address__]
        target_label: __address__
        regex: ([^:]+)(?::\d+)?
        replacement: $1:8000
      # Set metrics path
      - target_label: __metrics_path__
        replacement: /metrics
```

**Important:** Make sure the indentation matches the existing jobs (should be 4 spaces for `- job_name`).

### Step 4: Save and Reload Prometheus

After saving the ConfigMap, reload Prometheus:

```bash
# Delete the Prometheus pod (it will be recreated automatically)
oc delete pod -n opencost -l app.kubernetes.io/name=prometheus

# Wait for it to be ready
oc wait --for=condition=ready pod -n opencost -l app.kubernetes.io/name=prometheus --timeout=120s
```

## Verification

### 1. Check Prometheus is Scraping vLLM Pods

```bash
# Port forward to Prometheus
oc port-forward -n opencost svc/prometheus-opencost-server 9090:80
```

Then visit http://localhost:9090/targets and look for:
- Job: `vllm-inference`
- Targets: Your vLLM pods (e.g., `vllm-itay-minimax-5698f79b94-z2sqz`)
- State: **UP** (green)

### 2. Verify Metrics Have Namespace Label

In Prometheus UI (http://localhost:9090), run these queries:

```promql
# Check prompt tokens with namespace
vllm:prompt_tokens_total{namespace="llm-d-pic"}

# Check generation tokens with namespace
vllm:generation_tokens_total{namespace="llm-d-pic"}

# Check all vLLM metrics
{__name__=~"vllm:.*", namespace="llm-d-pic"}
```

You should see results like:
```
vllm:prompt_tokens_total{namespace="llm-d-pic",model_name="MiniMaxAI/MiniMax-M2.7",pod="vllm-itay-minimax-5698f79b94-z2sqz",...}
```

### 3. Check OpenCost Inference Metrics

Wait 1-2 minutes for OpenCost to collect and calculate costs, then:

```bash
# Port forward to OpenCost
oc port-forward -n opencost svc/opencost 9003:9003
```

Visit http://localhost:9003/metrics and search for:

```
opencost_inference_cost_per_million_tokens
opencost_inference_total_cost
```

Expected output:
```
opencost_inference_cost_per_million_tokens{model_name="MiniMaxAI/MiniMax-M2.7",model_version="unknown",namespace="llm-d-pic"} 0.XX
opencost_inference_total_cost{model_name="MiniMaxAI/MiniMax-M2.7",model_version="unknown",namespace="llm-d-pic"} 0.XX
```

### 4. Check OpenCost Logs

```bash
oc logs -n opencost deployment/opencost -c opencost --tail=100 | grep -i inference
```

Expected log messages:
```
Inference cost tracking is enabled
Inference cost collector started with interval: 1m0s
Collected and exported inference costs for 1 models
```

If you see errors, check:
- Prometheus endpoint is accessible
- vLLM metrics are in Prometheus
- GPU costs are available in OpenCost

## Troubleshooting

### Issue: Prometheus targets show "DOWN"

**Cause:** Prometheus cannot reach vLLM pods on port 8000

**Solutions:**
1. Verify vLLM is listening on port 8000:
   ```bash
   oc exec -n llm-d-pic vllm-itay-minimax-5698f79b94-z2sqz -- netstat -tlnp | grep 8000
   ```

2. Check network policies allow Prometheus to scrape:
   ```bash
   oc get networkpolicies -n llm-d-pic
   ```

3. Test connectivity from Prometheus pod:
   ```bash
   PROM_POD=$(oc get pod -n opencost -l app.kubernetes.io/name=prometheus -o name)
   oc exec -n opencost $PROM_POD -- wget -O- http://10.131.5.169:8000/metrics
   ```

### Issue: Metrics don't have namespace label

**Cause:** Relabeling configuration not applied correctly

**Solution:** 
1. Check the ConfigMap was updated:
   ```bash
   oc get configmap prometheus-opencost-server -n opencost -o yaml | grep -A 5 "vllm-inference"
   ```

2. Verify the relabel config includes:
   ```yaml
   - source_labels: [__meta_kubernetes_namespace]
     target_label: namespace
     action: replace
   ```

3. Reload Prometheus again:
   ```bash
   oc delete pod -n opencost -l app.kubernetes.io/name=prometheus
   ```

### Issue: OpenCost not generating inference metrics

**Possible causes:**

1. **Prometheus queries failing:**
   ```bash
   # Check OpenCost can query Prometheus
   oc exec -n opencost deployment/opencost -c opencost -- \
     curl -s "http://prometheus-opencost-server/api/v1/query?query=up"
   ```

2. **No GPU costs available:**
   OpenCost needs GPU costs to calculate inference costs. Check:
   ```promql
   opencost_allocation_gpu_cost{namespace="llm-d-pic"}
   ```

3. **Inference collector not running:**
   Check logs for startup errors:
   ```bash
   oc logs -n opencost deployment/opencost -c opencost | grep -i "inference\|error"
   ```

### Issue: Metrics appear but costs are zero

**Cause:** No token throughput or GPU costs

**Check:**
1. Token metrics are increasing:
   ```promql
   rate(vllm:prompt_tokens_total{namespace="llm-d-pic"}[5m])
   ```

2. GPU costs exist:
   ```promql
   sum(opencost_allocation_gpu_cost{namespace="llm-d-pic"})
   ```

## How It Works

1. **Prometheus discovers vLLM pods** using Kubernetes service discovery and the `llm-d.ai/role=decode` label
2. **Prometheus scrapes metrics** from each pod's port 8000
3. **Prometheus adds namespace label** during scraping via relabel_configs
4. **OpenCost queries Prometheus** every 60 seconds for:
   - `vllm:prompt_tokens_total` (with namespace and model_name)
   - `vllm:generation_tokens_total` (with namespace and model_name)
   - `opencost_allocation_gpu_cost` (GPU infrastructure costs)
5. **OpenCost calculates costs:**
   - Token throughput = rate(tokens) over 5 minutes
   - Cost per token = GPU cost / token throughput
   - Cost per million tokens = cost per token × 1,000,000
6. **OpenCost exports metrics** to its `/metrics` endpoint

## Configuration Details

### Scrape Job Explanation

```yaml
- job_name: 'vllm-inference'           # Job name in Prometheus
  honor_labels: true                    # Preserve labels from vLLM
  scrape_interval: 30s                  # Scrape every 30 seconds
  scrape_timeout: 10s                   # Timeout after 10 seconds
  metrics_path: /metrics                # vLLM metrics endpoint
  kubernetes_sd_configs:
  - role: pod                           # Discover pods (all namespaces)
  relabel_configs:
  - source_labels: [__meta_kubernetes_pod_label_llm_d_ai_role]
    action: keep                        # Only keep pods with this label
    regex: decode                       # Label value must be "decode"
  - source_labels: [__meta_kubernetes_namespace]
    target_label: namespace             # Add namespace label
    action: replace
  - source_labels: [__address__]
    target_label: __address__
    regex: ([^:]+)(?::\d+)?
    replacement: $1:8000                # Change port to 8000
```

### Why This Works Across All Namespaces

- `kubernetes_sd_configs: - role: pod` with no namespace filter discovers pods in **all namespaces**
- The `llm-d.ai/role=decode` label filter ensures only vLLM inference pods are scraped
- The namespace is added as a label from the pod's metadata

## Alternative: Using Annotations

If you prefer annotation-based discovery, you can add these annotations to your vLLM deployments:

```yaml
metadata:
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "8000"
    prometheus.io/path: "/metrics"
```

Then use the existing `kubernetes-pods` job (but you still need to add namespace labeling to that job).

## Files Created

- `prometheus-vllm-scrape-config.yaml` - The scrape job configuration
- `add-vllm-scrape-config.sh` - Interactive script to apply the configuration
- `prometheus-backup-YYYYMMDD.yaml` - Backup of your current Prometheus config

## Support

If you encounter issues:

1. Check OpenCost logs: `oc logs -n opencost deployment/opencost -c opencost`
2. Check Prometheus targets: http://localhost:9090/targets (after port-forward)
3. Verify vLLM metrics: `oc exec -n llm-d-pic <pod> -- curl localhost:8000/metrics`
4. Check this guide's troubleshooting section

## Summary

After applying this fix:
- ✅ Prometheus will scrape all vLLM pods across all namespaces
- ✅ All vLLM metrics will have the `namespace` label
- ✅ OpenCost will collect token metrics and calculate inference costs
- ✅ You'll see `opencost_inference_cost_per_million_tokens` metrics
- ✅ Costs will be broken down by model and namespace