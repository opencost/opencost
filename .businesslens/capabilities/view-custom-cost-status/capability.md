---
availability:
- place: cost-api::cost-reporting
domain: custom-costs
references:
- kind: code
  role: implementation
  target: pkg/customcost/pipelineservice.go#GetCustomCostStatusHandler
- kind: code
  role: implementation
  target: pkg/customcost/status.go
---

# View custom cost status

Show whether custom costs are on and, for each custom cost plugin, how far its
hourly and daily imports reach.
