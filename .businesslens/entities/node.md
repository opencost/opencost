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

# Node

A machine in the cluster, priced by the hour for its CPU, memory and GPUs.

## Information kept

- **Name** — the node name
- **Cluster** — the cluster it belongs to
- **Provider** — the cloud provider, account and project it runs under
- **Provider ID** — the provider's identifier for the instance
- **Node type** — the instance type
- **Labels** — the node's labels
- **Window** — the period the costs cover
- **Capacity** — CPU cores, RAM bytes and GPU count
- **Hourly prices** — the hourly price of a CPU core, a GiB of RAM and a GPU on this node
- **Preemptible** — whether the node is a spot or preemptible instance
- **Discount** — the discount applied to its CPU and RAM cost
- **CPU cost** — the cost of its CPU over the window
- **RAM cost** — the cost of its memory over the window
- **GPU cost** — the cost of its GPUs over the window
- **Total cost** — its total cost over the window
- **Carbon estimate** — estimated emissions in metric tonnes of CO2e over the window
