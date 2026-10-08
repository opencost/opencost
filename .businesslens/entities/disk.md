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

# Disk

A persistent volume or a node's attached storage, priced by size and time.

## Information kept

- **Name** — the volume name
- **Cluster** — the cluster it belongs to
- **Provider ID** — the provider's identifier for the volume
- **Storage class** — the volume's storage class
- **Claim** — the claim and namespace bound to it
- **Local** — whether it is node-local storage
- **Size** — the bytes provisioned and the peak bytes used
- **Window** — the period the costs cover
- **Hourly price** — the hourly price of the volume
- **Total cost** — its total cost over the window
- **Carbon estimate** — estimated emissions in metric tonnes of CO2e over the window
