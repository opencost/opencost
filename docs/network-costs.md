# Network Costs Documentation

## Overview

OpenCost provides comprehensive network cost tracking for Kubernetes clusters through a robust percentage-based system that works out of the box:

1. **Primary Network Cost Solution**: Built-in automatic network cost estimation using configurable traffic distribution ratios
2. **Alternative Network-Costs Component**: Separate component (integration/availability unclear) for kernel-level traffic tracking

The primary solution helps track the cost of network egress and other paid network transfers using customizable percentages that users can adjust based on their historical traffic data.

## Primary Network Cost Solution (Recommended)

OpenCost provides comprehensive network cost tracking using native Kubernetes metrics with user-configurable traffic distribution ratios. This is the recommended open source solution for network cost monitoring.

### Features

- Uses `container_network_transmit_bytes_total` metric from cAdvisor
- Fully configurable pricing rates for different traffic types
- User-customizable traffic distribution percentages based on historical data
- Works immediately without additional component deployment
- Prevents triple-counting issues through intelligent percentage-based distribution
- Users can set ratios based on their own historical traffic analysis

### Default Configuration

| Traffic Type | Default Rate (per GiB) | Default Percentage |
|-------------|------------------------|-------------------|
| Zone        | $0.01                 | 70%               |
| Region      | $0.02                 | 20%               |
| Internet    | $0.09                 | 10%               |

### Environment Variables

Configure network costs using these environment variables (customize percentages based on your historical traffic data):

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

The primary network cost system automatically activates when:
- Network traffic is detected in the cluster
- Provides immediate cost tracking without requiring additional deployments

You'll see this log message when the system is active:
```
Network traffic detected. Using percentage-based network cost estimation based on container_network_transmit_bytes_total with configurable traffic distribution ratios.
```

## Alternative Network-Costs Component

A separate network-costs component exists that claims to provide detailed network cost tracking through kernel-level access, though integration and availability details are unclear.

### Features (Component-Specific)

- Claims precise zone/region/internet traffic classification
- Requires kernel-level network metrics collection
- May integrate with cloud provider pricing APIs
- Alternative approach to percentage-based estimation
- Provides per-pod network cost breakdowns when properly configured

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

| Feature | Primary Solution | Alternative Component |
|---------|------------------|----------------------|
| **Setup** | Automatic | Manual deployment required |
| **Accuracy** | Configurable (user-customizable ratios) | Claims kernel-level precision |
| **Operational Overhead** | None | DaemonSet management |
| **Resource Usage** | Minimal | ~50m CPU, ~20Mi Memory per node |
| **Cloud Provider Integration** | Configurable rates | May support pricing API integration |
| **Traffic Classification** | User-customizable percentage distribution | Claims actual zone/region/internet detection |
| **Dependencies** | None (uses cAdvisor) | Privileged DaemonSet access |
| **Availability** | Open source, included | Separate component (unclear availability) |
| **Suitable For** | All deployments, customizable precision | Specialized use cases requiring kernel access |

## Customization Guide

### Optimizing Traffic Distribution Ratios

1. **Analyze your historical traffic patterns** using your cloud provider's networking dashboards
2. **Adjust percentage ratios** based on actual zone/region/internet distribution
3. **Test configuration** using the verification steps below
4. **Monitor cost accuracy** and fine-tune ratios as needed

### Configuration Testing

Test and optimize your network cost configuration:

```bash
# Check if percentages sum to 100%
echo "Zone: $NETWORK_COST_FALLBACK_ZONE_PERCENTAGE%"
echo "Region: $NETWORK_COST_FALLBACK_REGION_PERCENTAGE%"  
echo "Internet: $NETWORK_COST_FALLBACK_INTERNET_PERCENTAGE%"

# Verify OpenCost logs for network cost messages
kubectl logs -n opencost deployment/opencost | grep -i network

# Example: Customize ratios based on your traffic analysis
# If your historical data shows 60% zone, 25% region, 15% internet:
export NETWORK_COST_FALLBACK_ZONE_PERCENTAGE=60
export NETWORK_COST_FALLBACK_REGION_PERCENTAGE=25  
export NETWORK_COST_FALLBACK_INTERNET_PERCENTAGE=15
```

## Troubleshooting

### Primary Solution Issues

**Problem**: Network costs showing as zero
- **Check**: `container_network_transmit_bytes_total` metric availability
- **Verify**: Percentages sum to 100%
- **Review**: OpenCost logs for configuration warnings

**Problem**: Invalid percentage configuration
- **Fix**: Ensure `ZONE + REGION + INTERNET = 100%`
- **Example**: 70% + 20% + 10% = 100%
- **Tip**: Base percentages on your actual traffic distribution analysis

### Alternative Component Issues

**Problem**: network-costs pods not starting
- **Check**: Node has sufficient privileges for kernel access
- **Verify**: RBAC permissions are correctly applied
- **Review**: Pod security policies allow privileged containers

**Problem**: Metrics not appearing in Prometheus
- **Verify**: Service discovery configuration
- **Check**: Prometheus scrape config includes network-costs service
- **Review**: Network connectivity between Prometheus and pods

## Best Practices

1. **Use the primary solution** for immediate, configurable network cost tracking
2. **Customize traffic ratios** based on your historical traffic analysis
3. **Monitor and adjust** percentages as your traffic patterns evolve
4. **Validate configuration** before production deployment
5. **Leverage user configurability** - this system provides flexibility to match your specific traffic distribution

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