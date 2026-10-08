---
availability:
- place: cost-api::administration
references:
- kind: code
  role: implementation
  target: pkg/costmodel/router.go#GetHelmValues
---

# View Helm values

Let the Administrator read the Helm values the installation was deployed with,
as the chart provided them.

Offered only while OpenCost runs in Kubernetes.
