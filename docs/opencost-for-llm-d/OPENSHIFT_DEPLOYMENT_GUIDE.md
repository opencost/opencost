# Deployment Guide: AI Inference Cost Tracking on OpenShift

This guide explains how to deploy the updated OpenCost with AI inference cost tracking to your OpenShift cluster using the pre-built image from GitHub Container Registry (ghcr.io).

## Prerequisites

- Access to OpenShift cluster with `oc` CLI configured
- Permissions to modify deployments in the `opencost` namespace
- GitHub Personal Access Token (PAT) with `read:packages` scope (for pulling from ghcr.io)

## Image Location

The OpenCost image with AI inference cost tracking is available at:
```
ghcr.io/simanadler/opencost-inference:latest
```

## Deployment Steps

### Step 1: Create GitHub Container Registry Secret

To pull images from ghcr.io, you need to create a secret with your GitHub credentials:

```bash
# Switch to opencost namespace
oc project opencost

# Create the secret
# Replace YOUR_GITHUB_USERNAME and YOUR_GITHUB_TOKEN with your actual credentials
oc create secret docker-registry ghcr-secret \
  --docker-server=ghcr.io \
  --docker-username=YOUR_GITHUB_USERNAME \
  --docker-password=YOUR_GITHUB_TOKEN \
  -n opencost

# Link the secret to the default service account for pulling images
oc secrets link default ghcr-secret --for=pull -n opencost

# Also link to the service account used by OpenCost (if different)
oc secrets link opencost ghcr-secret --for=pull -n opencost 2>/dev/null || true
```

**How to get a GitHub Personal Access Token:**
1. Go to https://github.com/settings/tokens
2. Click "Generate new token" → "Generate new token (classic)"
3. Give it a name (e.g., "OpenShift OpenCost")
4. Select scope: `read:packages`
5. Click "Generate token"
6. Copy the token immediately (you won't see it again)

### Step 2: Backup Current Deployment

```bash
# Create a backup of the current deployment
oc get deployment opencost -n opencost -o yaml > opencost-deployment-backup-$(date +%Y%m%d-%H%M%S).yaml
```

### Step 3: Update Deployment Image

```bash
# Update the deployment to use the ghcr.io image
oc set image deployment/opencost \
  opencost=ghcr.io/simanadler/opencost-inference:latest \
  -n opencost

# Set image pull policy to Always to ensure latest image is pulled
oc patch deployment opencost -n opencost -p '{"spec":{"template":{"spec":{"containers":[{"name":"opencost","imagePullPolicy":"Always"}]}}}}'
```

### Step 4: Configure Environment Variables

```bash
# Add environment variables to enable inference cost tracking
oc set env deployment/opencost -n opencost \
  INFERENCE_COST_ENABLED=true \
  INFERENCE_COST_COLLECTION_INTERVAL=60 \
  PROMETHEUS_SERVER_ENDPOINT=http://prometheus-server:9090
```

### Step 4.5 (Optional): Update Prices

Create a customer price file based on configs/default.json and then do the following

```bash
# Update the pricing config file
oc create configmap pricing-configs --from-file=default.json=opencost/configs/<name of your file>.json -n opencost --dry-run=client -o yaml | oc apply -f -

# restart opencost
oc rollout restart deployment/opencost -n opencost
```

To check that the changes were deployed:

```bash
oc get configmap pricing-configs -n opencost -o jsonpath='{.data.default\.json}' | jq '.storage'
```

Note that it takes time for new prices to be reflected in the historical metrics.

### Step 5: Verify Deployment

```bash
# Check rollout status
oc rollout status deployment/opencost -n opencost

# Check pods are running
oc get pods -n opencost -l app=opencost

# View logs to confirm inference cost tracking is enabled
oc logs -f deployment/opencost -n opencost | grep inference
```

You should see log messages like:
```
Inference cost tracking is enabled
Inference cost collector started with interval: 1m0s
```

## Alternative: Manual Deployment Edit

If you prefer to edit the deployment manually:

```bash
oc edit deployment opencost -n opencost
```

Update the following sections:

1. **Image and Pull Secret**:
```yaml
spec:
  template:
    spec:
      imagePullSecrets:
      - name: ghcr-secret
      containers:
      - name: opencost
        image: ghcr.io/simanadler/opencost-inference:latest
        imagePullPolicy: Always
```

2. **Environment Variables**:
```yaml
        env:
        - name: INFERENCE_COST_ENABLED
          value: "true"
        - name: INFERENCE_COST_COLLECTION_INTERVAL
          value: "60"
        - name: PROMETHEUS_SERVER_ENDPOINT
          value: "http://prometheus-server:9090"
        # ... existing env vars ...
```

Save and exit. OpenShift will automatically roll out the new deployment.

## Using Kustomize or Helm

### Kustomize

Create a patch file `inference-cost-patch.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: opencost
spec:
  template:
    spec:
      imagePullSecrets:
      - name: ghcr-secret
      containers:
      - name: opencost
        image: ghcr.io/simanadler/opencost-inference:latest
        imagePullPolicy: Always
        env:
        - name: INFERENCE_COST_ENABLED
          value: "true"
        - name: INFERENCE_COST_COLLECTION_INTERVAL
          value: "60"
        - name: PROMETHEUS_SERVER_ENDPOINT
          value: "http://prometheus-server:9090"
```

Add to your `kustomization.yaml`:

```yaml
patchesStrategicMerge:
- inference-cost-patch.yaml
```

Apply:

```bash
oc apply -k . -n opencost
```

### Helm

If using Helm, update your `values.yaml`:

```yaml
opencost:
  image:
    repository: ghcr.io/simanadler/opencost-inference
    tag: latest
    pullPolicy: Always
  
  imagePullSecrets:
  - name: ghcr-secret
  
  env:
    INFERENCE_COST_ENABLED: "true"
    INFERENCE_COST_COLLECTION_INTERVAL: "60"
    PROMETHEUS_SERVER_ENDPOINT: "http://prometheus-server:9090"
```

Deploy:

```bash
helm upgrade opencost ./opencost-chart -n opencost -f values.yaml
```


## Verification Steps

### 1. Check Deployment Status

```bash
# Check if pods are running
oc get pods -n opencost

# Check deployment rollout status
oc rollout status deployment/opencost -n opencost

# View pod logs
oc logs -f deployment/opencost -n opencost
```

Look for log messages indicating inference cost tracking is enabled:
```
Inference cost tracking is enabled
Inference cost collector started with interval: 1m0s
```

### 2. Verify Metrics are Being Exported

```bash
# Port-forward to OpenCost service
oc port-forward -n opencost svc/opencost 9003:9003

# In another terminal, check metrics
curl http://localhost:9003/metrics | grep opencost_inference
```

You should see:
```
opencost_inference_total_cost{model_name="...",model_version="...",namespace="..."} ...
opencost_inference_cost_per_million_tokens{model_name="...",model_version="...",namespace="..."} ...
```

### 3. Query Metrics from Prometheus

If you have access to Prometheus UI:

```bash
# Port-forward to Prometheus
oc port-forward -n monitoring svc/prometheus-server 9090:9090
```

Open http://localhost:9090 and query:
```promql
opencost_inference_cost_per_million_tokens
```

### 4. Check for Errors

```bash
# View recent logs
oc logs -n opencost deployment/opencost --tail=100

# Follow logs in real-time
oc logs -n opencost deployment/opencost -f | grep -i "inference\|error"
```

## Troubleshooting

### Issue: Pods Not Starting

```bash
# Check pod status
oc describe pod -n opencost -l app=opencost

# Check events
oc get events -n opencost --sort-by='.lastTimestamp'
```

Common causes:
- Image pull errors: Check registry credentials
- Resource limits: Check if cluster has available resources
- Configuration errors: Check environment variables

### Issue: No Metrics Appearing

1. **Check if feature is enabled:**
```bash
oc exec -n opencost deployment/opencost -- env | grep INFERENCE_COST
```

2. **Check Prometheus connectivity:**
```bash
oc exec -n opencost deployment/opencost -- curl -s http://prometheus-server:9090/-/healthy
```

3. **Check vLLM metrics are available:**
```bash
# From within the cluster
oc exec -n opencost deployment/opencost -- curl -s http://prometheus-server:9090/api/v1/query?query=vllm:prompt_tokens_total
```

### Issue: High Memory Usage

If the collector is using too much memory, increase the collection interval:

```bash
oc set env deployment/opencost -n opencost INFERENCE_COST_COLLECTION_INTERVAL=300
```

### Issue: Permission Errors

Ensure OpenCost has proper RBAC permissions:

```bash
# Check service account
oc get sa opencost -n opencost

# Check role bindings
oc get rolebinding,clusterrolebinding -n opencost | grep opencost
```

## Configuration Options

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `INFERENCE_COST_ENABLED` | `false` | Enable inference cost tracking |
| `INFERENCE_COST_COLLECTION_INTERVAL` | `60` | Collection interval in seconds |
| `PROMETHEUS_SERVER_ENDPOINT` | - | Prometheus server URL |

### Recommended Settings

**Development/Testing:**
```bash
INFERENCE_COST_ENABLED=true
INFERENCE_COST_COLLECTION_INTERVAL=30
```

**Production:**
```bash
INFERENCE_COST_ENABLED=true
INFERENCE_COST_COLLECTION_INTERVAL=60
```

**High-Volume Production:**
```bash
INFERENCE_COST_ENABLED=true
INFERENCE_COST_COLLECTION_INTERVAL=300
```

## Rollback Procedure

If you need to rollback to the previous version:

```bash
# Rollback to previous deployment
oc rollout undo deployment/opencost -n opencost

# Or restore from backup
oc apply -f opencost-deployment-backup.yaml
```

## Monitoring the Deployment

### Create Alerts

Example Prometheus alert for high inference costs:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: opencost-inference-alerts
  namespace: opencost
spec:
  groups:
  - name: inference-costs
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

Apply:
```bash
oc apply -f opencost-inference-alerts.yaml
```

### Create Dashboard

Example Grafana dashboard query:

```promql
# Cost per million tokens by model
opencost_inference_cost_per_million_tokens

# Total cost over time
sum(rate(opencost_inference_total_cost[5m])) * 300

# Cost by namespace
sum by (namespace) (opencost_inference_total_cost)
```

## Production Checklist

Before deploying to production:

- [ ] Test in development environment
- [ ] Verify metrics are being collected correctly
- [ ] Set appropriate collection interval
- [ ] Configure resource limits on OpenCost pod
- [ ] Set up monitoring and alerts
- [ ] Document the deployment for your team
- [ ] Plan rollback procedure
- [ ] Test rollback procedure
- [ ] Notify stakeholders of the deployment

## Support

For issues or questions:
- Check logs: `oc logs -n opencost deployment/opencost`
- Review documentation: `opencost/docs/inference-cost-tracking.md`
- Check OpenCost GitHub issues
- Contact your platform team

## Next Steps

After successful deployment:

1. Monitor metrics for 24-48 hours
2. Adjust collection interval if needed
3. Set up dashboards and alerts
4. Document any custom configurations
5. Plan for Phase 2 enhancements