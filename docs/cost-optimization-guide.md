# Kubernetes Cost Optimization Guide

A practical guide for identifying cost inefficiencies and optimizing resource allocation using OpenCost.

## Table of Contents

- [Overview](#overview)
- [Identifying Cost Waste](#identifying-cost-waste)
- [Resource Right-Sizing](#resource-right-sizing)
- [Multi-Tenant Cost Optimization](#multi-tenant-cost-optimization)
- [Automated Cost Controls](#automated-cost-controls)
- [Case Studies](#case-studies)

## Overview

Effective cost optimization requires a data-driven approach. OpenCost provides the visibility needed to identify waste, track spending patterns, and implement targeted optimizations.

### Key Metrics to Monitor

| Metric | Description | Target Range |
|--------|-------------|--------------|
| CPU Utilization | Actual CPU usage / Requested CPU | 60-80% average |
| Memory Utilization | Actual memory usage / Requested memory | 70-85% average |
| Idle Cost % | Cost of unused capacity | < 15% |
| Cost per Request | Total cost / Request volume | Trend downward |

## Identifying Cost Waste

### 1. Finding Over-Provisioned Resources

Query for workloads with consistently low utilization:

```bash
# Using kubectl-cost (see CLI documentation)
kubectl cost allocation --window 7d --show-all-resources
```

**Signs of over-provisioning:**
- CPU utilization consistently below 40%
- Memory utilization consistently below 50%
- High idle costs in specific namespaces

### 2. Identifying Orphaned Resources

Common sources of waste:

- **Unused PVCs**: Volumes attached to deleted workloads
- **Orphaned LoadBalancers**: Services deleted but LB resources remain
- **Stale ConfigMaps/Secrets**: Resources from previous deployments

### 3. Spotting Zombie Workloads

Workloads running but not serving traffic:

```yaml
# Example: CronJobs with missed schedules
# Check for Jobs that hang indefinitely
kubectl get jobs --all-namespaces | grep -v "Complete"
```

## Resource Right-Sizing

### CPU Optimization

**Strategy**: Set requests to match average usage, limits for peak handling

```yaml
# Before optimization (over-provisioned)
resources:
  requests:
    cpu: "2000m"    # 2 cores
  limits:
    cpu: "4000m"    # 4 cores

# After optimization (right-sized based on OpenCost data)
resources:
  requests:
    cpu: "500m"     # 0.5 cores - matches p95 usage
  limits:
    cpu: "1500m"    # 1.5 cores - handles traffic spikes
```

### Memory Optimization

**Strategy**: Account for Java heap sizes, buffer pools, and OS overhead

```yaml
# For JVM workloads, set requests based on heap + overhead
resources:
  requests:
    memory: "2Gi"   # Based on actual heap usage + 30% overhead
  limits:
    memory: "3Gi"   # Prevent OOMKill during traffic spikes
```

### Auto-Scaling Best Practices

**Horizontal Pod Autoscaler (HPA)**:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: app-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: my-app
  minReplicas: 2      # Maintain HA
  maxReplicas: 20     # Cost ceiling
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
```

## Multi-Tenant Cost Optimization

### Namespace-Based Cost Allocation

For teams sharing clusters:

1. **Label namespaces** with team/department identifiers:
   ```yaml
   labels:
     cost-center: "engineering"
     team: "platform"
     environment: "production"
   ```

2. **Set resource quotas** to prevent runaway spending:
   ```yaml
   apiVersion: v1
   kind: ResourceQuota
   metadata:
     name: team-quota
   spec:
     hard:
       requests.cpu: "20"
       requests.memory: 100Gi
       limits.cpu: "40"
       limits.memory: 200Gi
   ```

3. **Monitor with OpenCost filters**:
   ```bash
   # View costs by namespace
   curl "http://opencost:9003/allocation?window=7d&aggregate=namespace"
   ```

### Showback/Chargeback Implementation

Create transparency with regular cost reports:

```python
# Example: Weekly cost report script
import requests

response = requests.get(
    "http://opencost:9003/allocation",
    params={
        "window": "7d",
        "aggregate": "namespace,label:team"
    }
)

# Generate per-team cost breakdown
costs = response.json()["data"]
for item in costs:
    team = item.get("labels", {}).get("team", "unlabeled")
    cost = item.get("totalCost", 0)
    print(f"Team {team}: ${cost:.2f}")
```

## Automated Cost Controls

### Policy-Based Optimization

Use OPA/Gatekeeper to enforce cost policies:

```rego
# Policy: Require resource limits
package k8srequiredresources

violation[{"msg": msg}] {
  container := input.review.object.spec.containers[_]
  not container.resources.limits
  msg := sprintf("Container %s must specify resource limits", [container.name])
}
```

### Scheduled Scaling

Scale non-production environments during off-hours:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: dev-scale-down
spec:
  schedule: "0 19 * * 1-5"  # 7 PM weekdays
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: scaler
            image: bitnami/kubectl
            command:
            - kubectl
            - scale
            - deployment
            - --all
            - --replicas=0
            - -n
            - development
          restartPolicy: OnFailure
```

## Case Studies

### Case Study 1: E-commerce Platform

**Challenge**: Monthly Kubernetes costs increased 40% during holiday season

**Solution**:
1. Used OpenCost to identify peak usage patterns
2. Implemented cluster autoscaling with right-sized node pools
3. Moved batch workloads to Spot instances

**Results**:
- 35% cost reduction during peak season
- Maintained 99.9% availability
- Improved resource utilization from 25% to 68%

### Case Study 2: SaaS Multi-Tenant Application

**Challenge**: Free tier users consuming disproportionate resources

**Solution**:
1. Implemented namespace-level cost tracking
2. Set resource quotas per tenant tier
3. Created automated alerts for cost anomalies

**Results**:
- 50% reduction in free tier abuse
- Better capacity planning for paid tiers
- Improved per-customer profitability analysis

## Quick Wins Checklist

- [ ] Review top 10 most expensive namespaces weekly
- [ ] Set up cost alerts for 20% month-over-month increases
- [ ] Right-size over-provisioned workloads (>70% headroom)
- [ ] Enable cluster autoscaling with appropriate instance types
- [ ] Clean up unused PVCs and orphaned resources monthly
- [ ] Implement resource quotas for all namespaces
- [ ] Tag resources for proper cost attribution
- [ ] Review and optimize node instance types quarterly

## Additional Resources

- [OpenCost Documentation](https://www.opencost.io/docs/)
- [Kubernetes Resource Management](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)
- [AWS EKS Best Practices](https://aws.github.io/aws-eks-best-practices/)
- [GKE Cost Optimization](https://cloud.google.com/kubernetes-engine/docs/best-practices/cost-optimization)

---

*Contributing: Have a cost optimization tip? Please open a PR to share your experience!*
