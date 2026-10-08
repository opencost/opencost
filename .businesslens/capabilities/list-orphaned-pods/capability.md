---
availability:
- place: cost-api::cost-reporting
references:
- kind: code
  role: implementation
  target: pkg/costmodel/router.go#GetOrphanedPods
---

# List orphaned pods

List the pods in the cluster that no controller owns.

Offered only while OpenCost runs in Kubernetes.
