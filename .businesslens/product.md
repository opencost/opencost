---
id: opencost
summary: 'Open source cost monitoring for Kubernetes and cloud spend: allocates cluster costs to workloads, prices cluster assets and imports cloud and external billing data.'
category: cost-monitoring
tags:
- kubernetes
- finops
- cost-allocation
- cloud-costs
- inference-cost
- prometheus
- mcp
authors:
- name: The OpenCost authors
  url: https://www.opencost.io
license: Apache-2.0
limitations:
- OpenCost is installed into a Kubernetes cluster and reads its usage from Prometheus or from its own collector; it does not keep cluster usage history itself when Prometheus is the source.
- Reporting operations on the cost API and the MCP server need no sign-in; only the administration operations ask for the admin token.
- The web UI and kubectl cost are separate clients of the cost API.
- Prices come from the cloud provider's public or negotiated pricing, a custom pricing file, or a pricing CSV; costs are reported in that pricing's currency and never converted.
- Costs and carbon figures are estimates from metrics, prices and billing data, never charges OpenCost issues.
- Pricing and cloud integrations are configured in files, ConfigMaps or the Helm chart; the cost API never adds or edits them.
- Imported cloud costs and custom costs are held in memory and are imported again from their sources after a restart.
- External costs come from separately installed custom cost plugins.
- Rightsizing recommendations are reported only; OpenCost never changes a workload's requests.
references:
- kind: doc
  role: context
  target: README.md
- kind: doc
  role: context
  target: AGENTS.md
- kind: spec
  role: intent
  target: spec/opencost-specv01.md
  title: OpenCost Specification
---

# OpenCost

OpenCost is a vendor-neutral cost monitor for Kubernetes clusters and the cloud
accounts around them. It measures what each container requests and uses, prices
the cluster's nodes, disks, load balancers and management fees, and allocates
those costs to the workloads that consume them, leaving the rest as idle cost.
It imports billing data from cloud providers and external costs from plugins,
estimates the carbon of the cluster's assets and the cost of serving AI models
per million tokens, and exposes all of it through an HTTP cost API, an MCP
server for AI agents and Prometheus metrics.

## Intent

Give teams that share Kubernetes clusters a transparent, specification-backed
account of what their workloads cost, and connect it with the rest of their
cloud spend, without a commercial product.
