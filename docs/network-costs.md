# Network Costs Documentation

## Overview

OpenCost provides two approaches for monitoring network costs in Kubernetes clusters:

1. **Fallback Network Costs**: Built-in automatic network cost estimation using native Kubernetes metrics
2. **Full Network-Costs Component**: External component providing detailed zone/region/internet traffic tracking

Both approaches help track the cost of network egress and other paid network transfers, providing insight into network data sources and aggregate transfer costs.

## Fallback Network Costs (Automatic)

OpenCost automatically provides network cost estimates using native Kubernetes metrics when the external network-costs component is not deployed.

### Features

- Uses `container_network_transmit_bytes_total` metric from cAdvisor
- Configurable pricing rates for different traffic types
- Configurable traffic distribution percentages
- Works immediately without additional component deployment
- Prevents triple-counting issues through percentage-based distribution

### Default Configuration

| Traffic Type | Default Rate (per GiB) | Default Percentage |
|-------------|------------------------|-------------------|
| Zone        | $0.01                 | 70%               |
| Region      | $0.02                 | 20%               |
| Internet    | $0.09                 | 10%               |

### Environment Variables

Configure fallback network costs using these environment variables:

```bash
# Pricing rates (per GiB)
NETWORK_COST_FALLBACK_ZONE_RATE=0.01
NETWORK_COST_FALLBACK_REGION_RATE=0.02  
NETWORK_COST_FALLBACK_INTERNET_RATE=0.09

# Traffic distribution percentages (must sum to 100%)
NETWORK_COST_FALLBACK_ZONE_PERCENTAGE=70
NETWORK_COST_FALLBACK_REGION_PERCENTAGE=20
NETWORK_COST_FALLBACK_INTERNET_PERCENTAGE=10
```

### Activation

The fallback system automatically activates when:
- Network traffic is detected in the cluster
- External network-costs component metrics are missing

You'll see this log message when fallback is active:
```
Network traffic detected but external network-costs metrics missing. Using fallback network cost estimation based on container_network_transmit_bytes_total. Deploy network-costs component for detailed zone/region/internet network cost tracking.
```

## Full Network-Costs Component

The network-costs component provides detailed, accurate network cost tracking by accessing kernel-level networking information.

### Features

- Precise zone/region/internet traffic classification
- Kernel-level network metrics collection
- Integration with cloud provider pricing APIs
- Enhanced accuracy over percentage-based estimation
- Detailed per-pod network cost breakdowns

### Deployment

#### Prerequisites

- Kubernetes cluster with Prometheus
- Sufficient RBAC permissions for DaemonSet deployment
- Privileged pod access (required for kernel module access)

#### Helm Installation

1. **Add OpenCost Helm repository:**
   ```bash
   helm repo add opencost-charts https://opencost.github.io/opencost-helm-chart
   helm repo update
   ```

2. **Enable network costs in values.yaml:**
   ```yaml
   networkCosts:
     enabled: true
     # Optional: Enable Prometheus scraping auto-discovery
     prometheusScrape: true
   ```

3. **Install OpenCost with network costs:**
   ```bash
   helm install opencost opencost-charts/opencost \
     --namespace opencost \
     --create-namespace \
     -f values.yaml
   ```

#### Manual Kubernetes Deployment

If not using Helm, deploy the network-costs component as a DaemonSet:

```yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: kubecost-network-costs
  namespace: opencost
  labels:
    app.kubernetes.io/name: network-costs
    app.kubernetes.io/instance: kubecost
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: network-costs
      app.kubernetes.io/instance: kubecost
  template:
    metadata:
      labels:
        app.kubernetes.io/name: network-costs
        app.kubernetes.io/instance: kubecost
    spec:
      hostNetwork: true
      containers:
      - name: network-costs
        image: gcr.io/kubecost1/kubecost-network-costs:latest
        ports:
        - containerPort: 3001
          name: metrics
        securityContext:
          privileged: true
        resources:
          requests:
            cpu: 50m
            memory: 20Mi
          limits:
            cpu: 500m
            memory: 100Mi
---
apiVersion: v1
kind: Service
metadata:
  name: kubecost-network-costs
  namespace: opencost
  labels:
    app.kubernetes.io/name: network-costs
    app.kubernetes.io/instance: kubecost
spec:
  selector:
    app.kubernetes.io/name: network-costs
    app.kubernetes.io/instance: kubecost
  ports:
  - port: 3001
    name: metrics
    targetPort: metrics
```

#### RBAC Configuration

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: kubecost-network-costs
  namespace: opencost
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: kubecost-network-costs
rules:
- apiGroups: [""]
  resources: ["pods", "nodes"]
  verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: kubecost-network-costs
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: kubecost-network-costs
subjects:
- kind: ServiceAccount
  name: kubecost-network-costs
  namespace: opencost
```

### Verification

1. **Check pod status:**
   ```bash
   kubectl get pods -n opencost -l app.kubernetes.io/name=network-costs
   ```

2. **Verify Prometheus targets:**
   - Check that `kubecost-networking` target is Up in Prometheus
   - Look for `kubecost_pod_network_egress_bytes_total` metric

3. **Check logs:**
   ```bash
   kubectl logs -n opencost -l app.kubernetes.io/name=network-costs
   ```

## Comparison

| Feature | Fallback | Full Component |
|---------|----------|----------------|
| **Setup** | Automatic | Manual deployment required |
| **Accuracy** | Estimated (percentage-based) | Precise (kernel-level) |
| **Operational Overhead** | None | DaemonSet management |
| **Resource Usage** | Minimal | ~50m CPU, ~20Mi Memory per node |
| **Cloud Provider Integration** | Basic rates | Full pricing API integration |
| **Traffic Classification** | Percentage distribution | Actual zone/region/internet detection |
| **Dependencies** | None (uses cAdvisor) | Privileged DaemonSet access |
| **Suitable For** | Quick setup, reasonable estimates | Production, detailed cost tracking |

## Migration Guide

### From Fallback to Full Component

1. **Deploy network-costs component** using the deployment instructions above
2. **Verify component is working** using the verification steps
3. **Monitor transition** - OpenCost will automatically switch when external metrics are detected
4. **Remove fallback configuration** (optional) - environment variables can be left for backup

### Configuration Testing

Test your network cost configuration:

```bash
# Check if percentages sum to 100% (fallback only)
echo "Zone: $NETWORK_COST_FALLBACK_ZONE_PERCENTAGE%"
echo "Region: $NETWORK_COST_FALLBACK_REGION_PERCENTAGE%"  
echo "Internet: $NETWORK_COST_FALLBACK_INTERNET_PERCENTAGE%"

# Verify OpenCost logs for network cost messages
kubectl logs -n opencost deployment/opencost | grep -i network
```

## Troubleshooting

### Fallback Issues

**Problem**: Network costs showing as zero with fallback
- **Check**: `container_network_transmit_bytes_total` metric availability
- **Verify**: Percentages sum to 100%
- **Review**: OpenCost logs for configuration warnings

**Problem**: Invalid percentage configuration
- **Fix**: Ensure `ZONE + REGION + INTERNET = 100%`
- **Example**: 70% + 20% + 10% = 100%

### Full Component Issues

**Problem**: network-costs pods not starting
- **Check**: Node has sufficient privileges for kernel access
- **Verify**: RBAC permissions are correctly applied
- **Review**: Pod security policies allow privileged containers

**Problem**: Metrics not appearing in Prometheus
- **Verify**: Service discovery configuration
- **Check**: Prometheus scrape config includes network-costs service
- **Review**: Network connectivity between Prometheus and pods

## Best Practices

1. **Start with fallback** for immediate functionality
2. **Deploy full component** for production accuracy
3. **Monitor resource usage** and set appropriate CPU limits
4. **Validate configuration** before production deployment
5. **Keep both options** - fallback serves as backup if component fails

## Cloud Provider Specific Notes

### AWS
- Internet egress rates vary by region
- Consider Regional Data Transfer costs
- VPC endpoint usage affects calculations

### GCP  
- Network pricing differs between regions
- Premium vs Standard tier affects costs
- Consider Cloud CDN for egress optimization

### Azure
- Bandwidth pricing varies by region
- Consider Azure CDN for cost optimization
- Virtual Network Gateway impacts network costs

For cloud-specific pricing configuration, refer to your cloud provider's networking documentation and adjust the rate environment variables accordingly.