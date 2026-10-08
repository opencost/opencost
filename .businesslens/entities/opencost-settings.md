---
singleton: true
references:
- kind: code
  role: implementation
  target: pkg/cmd/costmodel/config.go
- kind: code
  role: implementation
  target: pkg/env/costmodel.go
- kind: code
  role: implementation
  target: pkg/env/cloudcost.go
---

# OpenCost settings

The configuration an operator gives an OpenCost installation through
environment variables, command-line flags or the Helm chart. OpenCost reads it
at start-up and never changes it.

## Information kept

- **Runs in Kubernetes** — whether OpenCost runs inside a Kubernetes cluster; allocations, assets, pricing, cluster details, KubeModel, inference costs, the GCP service key, the Helm values and the MCP server exist only then
- **Admin token** — the bearer token that activates the administration operations; while it is unset they are unavailable
- **Cloud costs enabled** — whether cloud billing integrations are imported and the cloud cost operations exist; off by default
- **Custom costs enabled** — whether custom cost plugins run and the custom cost queries exist; off by default
- **Carbon estimates enabled** — whether carbon estimates for assets are offered; off by default
- **Inference costs enabled** — whether AI model serving costs are computed; off by default
- **MCP server enabled** — whether the MCP server runs; off by default
- **KubeModel export enabled** — whether KubeModel sets are exported; off by default
- **Cloud cost retention** — how many days of cloud costs are kept; 30 by default
- **Cloud cost refresh rate** — how many hours pass between cloud cost imports; 6 by default
- **Custom cost retention** — how many days of daily and hours of hourly custom costs are imported at start-up, and up to which age a query reads hourly costs; 30 days and 49 hours by default
