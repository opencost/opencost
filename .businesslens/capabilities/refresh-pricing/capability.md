---
availability:
- place: cost-api::cost-reporting
references:
- kind: code
  role: implementation
  target: pkg/costmodel/router.go#RefreshPricingData
---

# Refresh pricing

Download the cloud provider's or pricing CSV's prices again, for example after
new kinds of node join the cluster.

Offered only while OpenCost runs in Kubernetes.
