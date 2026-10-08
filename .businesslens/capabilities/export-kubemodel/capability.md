---
availability:
- place: cost-api::cost-reporting
references:
- kind: code
  role: implementation
  target: pkg/kubemodel/pipeline.go
- kind: code
  role: implementation
  target: pkg/kubemodel/janitor.go
- kind: code
  role: implementation
  target: core/pkg/compute/kubemodel/kubemodel.go
---

# Export KubeModel

Every five minutes, compute the cluster's KubeModel set for the current hour and
day and write it to storage; once a day, prune sets older than their
retention. Runs only while KubeModel export is enabled.

Offered only while OpenCost runs in Kubernetes.
