# Multi-Tenant Kubernetes Cost Optimization Guide

This guide provides practical strategies for optimizing costs in multi-tenant Kubernetes environments using OpenCost. Whether you're managing a shared cluster across teams, departments, or customers, these patterns will help you achieve better cost visibility and control.

## Table of Contents

- [Overview](#overview)
- [Labeling Strategy](#labeling-strategy)
- [Namespace-Based Cost Allocation](#namespace-based-cost-allocation)
- [Team/Department Chargeback](#teamdepartment-chargeback)
- [Resource Optimization Patterns](#resource-optimization-patterns)
- [Monitoring and Alerting](#monitoring-and-alerting)
- [Best Practices](#best-practices)

## Overview

Multi-tenant Kubernetes clusters present unique cost management challenges:

- **Resource contention**: Multiple teams sharing the same infrastructure
- **Cost attribution**: Determining who is responsible for which costs
- **Noisy neighbors**: One tenant's usage affecting others
- **Budget management**: Tracking spend across organizational boundaries

OpenCost addresses these challenges through flexible aggregation and labeling capabilities.

## Labeling Strategy

A consistent labeling strategy is the foundation of effective cost allocation. We recommend the following mandatory labels:

### Required Labels

```yaml
labels:
  # Team/Organization
  team: "platform"                    # Team name
  department: "engineering"           # Department
  
  # Environment
  environment: "production"           # dev, staging, production
  
  # Application
  app: "payment-service"              # Application name
  service: "api"                      # Service component
  
  # Cost Center
  cost-center: "cc-1234"              # For chargeback
  owner: "team-platform@company.com"  # Contact information
```

### Label Configuration in OpenCost

Configure OpenCost to recognize your labels:

```yaml
# helm-values.yaml
opencost:
  exporter:
    extraEnv:
      # Map your labels to OpenCost's aliased labels
      KUBECOST_LABELS_TEAM_LABEL: "team,app"
      KUBECOST_LABELS_DEPARTMENT_LABEL: "department"
      KUBECOST_LABELS_ENVIRONMENT_LABEL: "environment"
      KUBECOST_LABELS_OWNER_LABEL: "owner"
      KUBECOST_LABELS_PRODUCT_LABEL: "app,service"
```

## Namespace-Based Cost Allocation

Namespaces are the primary isolation boundary in Kubernetes and serve as the foundation for cost allocation.

### Namespace Structure Best Practices

```
├─ team-a-prod          # Production workloads for Team A
├─ team-a-staging       # Staging environment for Team A
├─ team-b-prod          # Production workloads for Team B
├─ team-b-staging       # Staging environment for Team B
├─ shared-monitoring    # Shared infrastructure
└─ kube-system          # System workloads
```

### Cost Quotas with ResourceQuota

Set hard limits per namespace:

```yaml
apiVersion: v1
kind: ResourceQuota
metadata:
  name: team-a-quota
  namespace: team-a-prod
spec:
  hard:
    requests.cpu: "20"
    requests.memory: 100Gi
    limits.cpu: "40"
    limits.memory: 200Gi
    pods: "50"
    services: "10"
```

### Querying Namespace Costs

Use OpenCost API to get namespace-level costs:

```bash
# Get costs for last 7 days, aggregated by namespace
curl -s "http://localhost:9003/allocation?window=7d&aggregate=namespace" | \
  jq '.data[].' \
    '{namespace: .name, 
      cpuCost: .cpuCost, 
      memoryCost: .ramCost, 
      totalCost: .totalCost}'
```

## Team/Department Chargeback

Implement chargeback models using OpenCost's aggregation capabilities.

### Example: Monthly Team Report

```bash
#!/bin/bash
# generate-team-report.sh

WINDOW="30d"
TEAM_LABEL="team"

echo "=== Monthly Cost Report ==="
echo "Period: $WINDOW"
echo ""

# Get costs aggregated by team label
curl -s "http://localhost:9003/allocation?window=$WINDOW&aggregate=label:team" | \
  jq -r '.data[] | [.name, .cpuCost, .ramCost, .totalCost] | @tsv' | \
  awk 'BEGIN {printf "%-20s %-12s %-12s %-12s\n", "Team", "CPU Cost", "Memory Cost", "Total Cost"}
       {printf "%-20s $%-11.2f $%-11.2f $%-11.2f\n", $1, $2, $3, $4}'
```

### Department Roll-up

Aggregate costs at department level:

```bash
# Aggregate by department, then drill down to teams
curl -s "http://localhost:9003/allocation?window=30d&aggregate=label:department,label:team" | \
  jq -r '.data[] | [.properties.department, .properties.team, .totalCost] | @tsv' | \
  sort | awk 'BEGIN {dept=""}
    $1 != dept {dept=$1; print "\nDepartment: " dept}
    {printf "  Team: %-20s Cost: $%.2f\n", $2, $3}'
```

## Resource Optimization Patterns

### 1. Right-sizing Workloads

Regularly analyze resource requests vs actual usage:

```bash
# Get allocation efficiency by pod
curl -s "http://localhost:9003/allocation?window=7d&aggregate=pod" | \
  jq '.data[] | select(.cpuCoreUsageAverage > 0) | 
    {pod: .name, 
     cpuRequest: .cpuCoreRequestAverage, 
     cpuUsage: .cpuCoreUsageAverage,
     efficiency: (.cpuCoreUsageAverage / .cpuCoreRequestAverage * 100)}'
```

Target efficiency: 60-80%. Values below indicate over-provisioning; above 90% may risk throttling.

### 2. Idle Resource Identification

Find unused resources:

```bash
# Identify pods with low utilization
curl -s "http://localhost:9003/allocation?window=7d&aggregate=namespace,pod" | \
  jq '.data[] | select(.cpuCoreUsageAverage < 0.01 and .ramByteUsageAverage < 104857600) | 
    {namespace: .properties.namespace, 
     pod: .name, 
     status: "CANDIDATE_FOR_REMOVAL"}'
```

### 3. Spot/Preemptible Instance Optimization

For non-critical workloads, use spot instances:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: batch-processor
  labels:
    workload-type: "spot-eligible"
spec:
  template:
    spec:
      nodeSelector:
        node.kubernetes.io/lifecycle: spot
      tolerations:
      - key: "spot"
        operator: "Equal"
        value: "true"
        effect: "NoSchedule"
```

Track spot instance savings:

```bash
curl -s "http://localhost:9003/allocation?window=7d&aggregate=node" | \
  jq '.data[] | {node: .name, 
                 nodeType: .properties.nodeType,
                 totalCost: .totalCost}'
```

## Monitoring and Alerting

### Prometheus Alerts

Set up cost-based alerts:

```yaml
groups:
  - name: cost-alerts
    rules:
      # Alert when namespace exceeds daily budget
      - alert: NamespaceDailyBudgetExceeded
        expr: |
          sum(
            opencost_cost_per_day{namespace=~"team-.*"}
          ) by (namespace) > 100
        for: 1h
        labels:
          severity: warning
        annotations:
          summary: "Namespace {{ $labels.namespace }} exceeded daily budget"
          description: "Daily cost is ${{ $value }}"

      # Alert on significant cost increase
      - alert: CostSpikeDetected
        expr: |
          (
            opencost_cost_per_day 
            / 
            opencost_cost_per_day offset 7d
          ) > 2
        for: 1h
        labels:
          severity: warning
        annotations:
          summary: "Cost spike detected in {{ $labels.namespace }}"
          description: "Cost is 2x higher than last week"
```

### Grafana Dashboard

Create a multi-tenant cost dashboard:

```json
{
  "dashboard": {
    "title": "Multi-Tenant Cost Overview",
    "panels": [
      {
        "title": "Cost by Team",
        "targets": [{
          "expr": "sum(opencost_allocation_cost) by (label_team)"
        }]
      },
      {
        "title": "Namespace Cost Trend",
        "targets": [{
          "expr": "sum(opencost_allocation_cost) by (namespace)"
        }]
      }
    ]
  }
}
```

## Best Practices

### 1. Governance

- **Mandatory labels**: Enforce via admission controllers (OPA/Gatekeeper)
- **Regular audits**: Monthly reviews of cost allocations
- **Budget ownership**: Assign budget owners per namespace/team

### 2. Technical

- **Node pools**: Separate critical and non-critical workloads
- **Autoscaling**: Enable HPA and cluster-autoscaler
- **Resource quotas**: Prevent resource exhaustion

### 3. Organizational

- **Showback before chargeback**: Start with visibility, then implement billing
- **Gradual rollout**: Begin with one team, then expand
- **Training**: Educate teams on cost-aware development

## Example: Complete Setup

```yaml
# namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: team-platform-prod
  labels:
    team: platform
    department: engineering
    environment: production
    cost-center: cc-eng-001
    owner: platform-team@company.com
---
# resourcequota.yaml
apiVersion: v1
kind: ResourceQuota
metadata:
  name: platform-quota
  namespace: team-platform-prod
spec:
  hard:
    requests.cpu: "10"
    requests.memory: 50Gi
    pods: "20"
---
# deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api-service
  namespace: team-platform-prod
  labels:
    app: api-service
    team: platform
    cost-center: cc-eng-001
spec:
  replicas: 3
  template:
    metadata:
      labels:
        app: api-service
    spec:
      containers:
      - name: api
        image: myapp:latest
        resources:
          requests:
            cpu: 100m
            memory: 256Mi
          limits:
            cpu: 500m
            memory: 512Mi
```

## Additional Resources

- [OpenCost API Documentation](https://www.opencost.io/docs/integrations/api)
- [Prometheus Integration](https://www.opencost.io/docs/integrations/prometheus)
- [Kubectl Cost Plugin](https://www.opencost.io/docs/integrations/kubectl-cost)

---

*Contributed by the OpenCost community. For questions or improvements, please open an issue or PR.*
