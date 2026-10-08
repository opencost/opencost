---
availability:
- place: cost-api::cost-reporting
references:
- kind: code
  role: implementation
  target: pkg/costmodel/router.go#GetPricingSourceStatus
- kind: code
  role: implementation
  target: pkg/costmodel/router.go#GetServiceAccountStatus
---

# View pricing sources

Show whether each source of prices, and each check of the cloud credentials
OpenCost was given, is working.

Offered only while OpenCost runs in Kubernetes.
