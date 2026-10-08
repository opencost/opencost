---
references:
- kind: code
  role: implementation
  target: pkg/costmodel/clusterinfo.go
---

# Cluster

A Kubernetes cluster OpenCost reports on.

## Information kept

- **ID** — the cluster's identifier
- **Name** — the cluster's name
- **Provider** — the cloud provider, account, project and region
- **Provisioner** — the managed Kubernetes offering, such as GKE or EKS, or none
- **Kubernetes version** — the Kubernetes version
- **Profile** — the cluster profile, such as development or production
