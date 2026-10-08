---
availability:
- place: cost-api::cost-reporting
references:
- kind: code
  role: implementation
  target: pkg/costmodel/router.go#ClusterInfo
- kind: code
  role: implementation
  target: pkg/costmodel/router.go#GetClusterInfoMap
- kind: code
  role: implementation
  target: pkg/costmodel/clusterinfo.go
---

# View cluster info

Describe the cluster OpenCost runs in, and every cluster its data source knows.

Offered only while OpenCost runs in Kubernetes.
