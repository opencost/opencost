---
availability:
- place: cost-api::cost-reporting
references:
- kind: code
  role: implementation
  target: pkg/costmodel/router.go#GetInstallInfo
- kind: code
  role: implementation
  target: pkg/costmodel/router.go#GetInstallNamespace
---

# View install info

Show which namespace and version of OpenCost is installed, its running
containers, and the cluster's node and pod counts.

Offered only while OpenCost runs in Kubernetes.
