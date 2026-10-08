---
availability:
- place: cost-api::cost-reporting
references:
- kind: code
  role: implementation
  target: pkg/costmodel/handlers.go#KubeModelHandler
- kind: code
  role: implementation
  target: pkg/kubemodel/querier.go
---

# Query KubeModel

Return the KubeModel sets exported for a window, daily where the window is made
of whole days and hourly otherwise.

Offered only while OpenCost runs in Kubernetes.
