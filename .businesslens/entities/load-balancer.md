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

# Load balancer

A cloud load balancer created for a Kubernetes service.

## Information kept

- **Name** — the load balancer name
- **Cluster** — the cluster it serves
- **Service** — the namespace and service it was created for
- **IP address** — its ingress address
- **Private** — whether it is internal to the network
- **Window** — the period the costs cover
- **Hourly cost** — its cost per hour
- **Total cost** — its total cost over the window
- **Carbon estimate** — estimated emissions over the window, always zero
