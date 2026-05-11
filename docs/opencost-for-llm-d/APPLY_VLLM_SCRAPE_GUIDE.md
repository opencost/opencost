# Configure OpenCost Prometheus to Scrape vLLM Metrics

## Overview

This guide configures OpenCost's Prometheus to scrape vLLM metrics from all pods with the `llm-d.ai/model` label across all namespaces, making OpenCost independent of OpenShift User Workload Monitoring.

## What This Does

- Discovers all vLLM pods using the `llm-d.ai/model` label
- Scrapes metrics from port 8000 (vLLM metrics endpoint)
- Adds `namespace` label to all metrics (required by OpenCost)
- Works across all namespaces (currently 38+ vLLM pods in 12 namespaces)

## Step-by-Step Instructions

### 1. Backup Current Configuration

```bash
oc get configmap prometheus-opencost-server -n opencost -o yaml > prometheus-backup-$(date +%Y%m%d-%H%M%S).yaml
```

### 2. Edit Prometheus ConfigMap

```bash
oc edit configmap prometheus-opencost-server -n opencost
```

### 3. Add vLLM Scrape Job

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
      # Keep only pods with llm-d.ai/model label (all vLLM inference pods)
      - source_labels: [__meta_kubernetes_pod_label_llm_d_ai_model]
        action: keep
        regex: .+
      # Add namespace label to all metrics
      - source_labels: [__meta_kubernetes_namespace]
        target_label: namespace
        action: replace
      # Add pod name
      - source_labels: [__meta_kubernetes_pod_name]
        target_label: pod
        action: replace
      # Add node name
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

**Important:** Ensure proper indentation (4 spaces for `- job_name`).

### 4. Save and Reload Prometheus

After saving the ConfigMap:

```bash
# Delete Prometheus pod to reload configuration
oc delete pod -n opencost -l app.kubernetes.io/name=prometheus

# Wait for it to be ready
oc wait --for=condition=ready pod -n opencost -l app.kubernetes.io/name=prometheus --timeout=120s
```

## Verification

### 1. Check Prometheus Targets (Wait 30-60 seconds)

```bash
# Port forward to Prometheus
oc port-forward -n opencost svc/prometheus-opencost-server 9090:80
```

Visit http://localhost:9090/targets and look for:
- **Job:** `vllm-inference`
- **Targets:** 38+ vLLM pods from various namespaces
- **State:** UP (green)

### 2. Verify Metrics Have Namespace Label

In Prometheus UI (http://localhost:9090), run:

```promql
# Check prompt tokens with namespace
vllm:prompt_tokens_total{namespace="llm-d-pic"}

# Check generation tokens with namespace
vllm:generation_tokens_total{namespace="llm-d-pic"}

# See all vLLM pods being scraped
count by (namespace, pod) (vllm:prompt_tokens_total)
```

Expected results:
```
vllm:prompt_tokens_total{
  namespace="llm-d-pic",
  pod="vllm-itay-minimax-5698f79b94-z2sqz",
  model_name="MiniMaxAI/MiniMax-M2.7",
  ...
}
```

### 3. Check OpenCost Inference Metrics (Wait 1-2 minutes)

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
Collected and exported inference costs for X models
```

## Troubleshooting

### Issue: No targets appear in Prometheus

**Check pod labels:**
```bash
oc get pods -A -l llm-d.ai/model --show-labels
```

**Verify Prometheus can reach pods:**
```bash
PROM_POD=$(oc get pod -n opencost -l app.kubernetes.io/name=prometheus -o name)
oc exec -n opencost $PROM_POD -- wget -O- http://10.131.5.217:8000/metrics
```

### Issue: Targets show "DOWN"

**Check if vLLM is listening on port 8000:**
```bash
oc exec -n llm-d-pic vllm-itay-minimax-5698f79b94-z2sqz -- netstat -tlnp | grep 8000
```

**Check network policies:**
```bash
oc get networkpolicies -n llm-d-pic
```

### Issue: Metrics don't have namespace label

**Verify relabel config was applied:**
```bash
oc get configmap prometheus-opencost-server -n opencost -o yaml | grep -A 10 "vllm-inference"
```

Look for:
```yaml
- source_labels: [__meta_kubernetes_namespace]
  target_label: namespace
  action: replace
```

**Reload Prometheus:**
```bash
oc delete pod -n opencost -l app.kubernetes.io/name=prometheus
```

### Issue: OpenCost not generating inference metrics

**1. Check Prometheus has vLLM metrics:**
```promql
vllm:prompt_tokens_total{namespace="llm-d-pic"}
```

**2. Check GPU costs exist:**
```promql
opencost_allocation_gpu_cost{namespace="llm-d-pic"}
```

**3. Check OpenCost can query Prometheus:**
```bash
oc exec -n opencost deployment/opencost -c opencost -- \
  curl -s "http://prometheus-opencost-server/api/v1/query?query=up"
```

**4. Check for errors in OpenCost logs:**
```bash
oc logs -n opencost deployment/opencost -c opencost | grep -i "error\|failed"
```

## What Gets Scraped

This configuration will scrape all pods with the `llm-d.ai/model` label, including:

- **llm-d-pic**: 2 pods (MiniMax-M2.7)
- **oshrit-benchmark**: 8 pods (Qwen3-32B)
- **dpikus-precise-new**: 8 pods (GPT-OSS-120B)
- **mohamedma**: 8 pods (GPT-OSS-120B)
- **dolev-inf**: 1 pod (Qwen3-32B)
- **llm-d-optimized-baseline**: 1 pod (Qwen3-32B)
- And 10+ more pods across other namespaces

Total: 38+ vLLM pods across 12 namespaces

## How It Works

1. **Kubernetes Service Discovery** finds all pods in the cluster
2. **Label filter** keeps only pods with `llm-d.ai/model` label
3. **Prometheus scrapes** port 8000 on each pod every 30 seconds
4. **Relabeling** adds `namespace`, `pod`, and `node` labels to metrics
5. **OpenCost queries** Prometheus for vLLM metrics with namespace labels
6. **OpenCost calculates** cost per token and cost per million tokens
7. **OpenCost exports** inference cost metrics

## Benefits of This Approach

✅ **Independent** - No dependency on OpenShift User Workload Monitoring
✅ **Comprehensive** - Scrapes all vLLM pods across all namespaces
✅ **Automatic** - Discovers new vLLM pods automatically via label
✅ **Efficient** - Single scrape job for all vLLM pods
✅ **Flexible** - Easy to modify label selector if needed

## Alternative Label Selectors

If you want to be more selective, you can modify the label filter:

**Only decode pods:**
```yaml
- source_labels: [__meta_kubernetes_pod_label_llm_d_ai_role]
  action: keep
  regex: decode
```

**Specific namespace only:**
```yaml
- source_labels: [__meta_kubernetes_namespace]
  action: keep
  regex: llm-d-pic
```

**Multiple labels (AND condition):**
```yaml
- source_labels: [__meta_kubernetes_pod_label_llm_d_ai_model]
  action: keep
  regex: .+
- source_labels: [__meta_kubernetes_pod_label_llm_d_ai_role]
  action: keep
  regex: decode
```

## Files

- `prometheus-vllm-scrape-final.yaml` - The scrape configuration to add
- `APPLY_VLLM_SCRAPE_GUIDE.md` - This guide

## Next Steps

After applying this configuration:

1. Wait 1-2 minutes for metrics to appear
2. Verify in Prometheus UI that vLLM metrics have namespace labels
3. Check OpenCost metrics endpoint for inference cost metrics
4. Monitor OpenCost logs for successful collection

## Support

If you encounter issues:
1. Check Prometheus targets are UP
2. Verify vLLM metrics exist in Prometheus with namespace labels
3. Check OpenCost logs for errors
4. Ensure GPU costs are available in OpenCost