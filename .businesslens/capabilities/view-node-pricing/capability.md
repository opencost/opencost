---
availability:
- place: cost-api::cost-reporting
references:
- kind: code
  role: implementation
  target: pkg/costmodel/router.go#GetAllNodePricing
- kind: code
  role: implementation
  target: pkg/costmodel/router.go#GetPricingSourceSummary
- kind: code
  role: implementation
  target: pkg/costmodel/router.go#GetPricingSourceCounts
---

# View node pricing

Show the node prices OpenCost uses, the provider's parsed pricing, and how many
of the cluster's nodes were priced each way.

Offered only while OpenCost runs in Kubernetes.
