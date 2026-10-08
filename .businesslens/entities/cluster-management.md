---
domain: assets
references:
- kind: code
  role: implementation
  target: pkg/costmodel/assets.go#ComputeAssets
- kind: code
  role: implementation
  target: core/pkg/opencost/asset.go
---

# Cluster management

The fee a managed Kubernetes provider charges for running a cluster's control
plane.

## Information kept

- **Cluster** — the cluster the fee is for
- **Provisioner** — the managed Kubernetes offering charging it
- **Window** — the period the cost covers
- **Hourly cost** — the fee per hour
- **Total cost** — the fee over the window
- **Carbon estimate** — estimated emissions over the window, always zero
