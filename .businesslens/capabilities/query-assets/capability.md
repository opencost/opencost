---
availability:
- place: cost-api::cost-reporting
- place: mcp-server
domain: assets
references:
- kind: code
  role: implementation
  target: pkg/costmodel/handlers.go#ComputeAssetsHandler
- kind: code
  role: implementation
  target: pkg/costmodel/assets.go#ComputeAssets
- kind: code
  role: implementation
  target: pkg/costmodel/autocomplete.go#ComputeAssetsAutocompleteHandler
- kind: code
  role: implementation
  target: pkg/mcp/server.go#QueryAssets
---

# Query assets

Report the cost of the cluster's nodes, disks, load balancers and cluster
management fees over a window, optionally filtered, with suggestions for the
values a filter can take; an AI agent asks the same through the MCP server.

Offered only while OpenCost runs in Kubernetes; on the MCP server, only while
the MCP server is enabled too.
