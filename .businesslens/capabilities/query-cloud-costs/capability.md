---
availability:
- place: cost-api::cost-reporting
- place: mcp-server
domain: cloud-costs
references:
- kind: code
  role: implementation
  target: pkg/cloudcost/queryservice.go
- kind: code
  role: implementation
  target: pkg/cloudcost/queryservice_helper.go#ParseCloudCostRequest
- kind: code
  role: implementation
  target: pkg/cloudcost/repositoryquerier.go
- kind: code
  role: implementation
  target: pkg/mcp/server.go#QueryCloudCosts
---

# Query cloud costs

Report imported cloud costs over a window, grouped by provider, account,
invoice entity, region, service, category or label and filtered, as daily or
accumulated sets, or as totals, a ranked graph or a sorted, paged table for one
cost metric; an AI agent asks the same through the MCP server. Offered only
while cloud costs are enabled; on the MCP server, only while OpenCost also runs
in Kubernetes and the MCP server is enabled.
