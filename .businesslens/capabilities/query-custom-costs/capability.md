---
availability:
- place: cost-api::cost-reporting
domain: custom-costs
references:
- kind: code
  role: implementation
  target: pkg/customcost/queryservice.go
- kind: code
  role: implementation
  target: pkg/customcost/queryservice_helper.go
- kind: code
  role: implementation
  target: pkg/customcost/repositoryquerier.go
---

# Query custom costs

Report the costs custom cost plugins reported over a window, as a total or a
time series, grouped, filtered and sorted, with the billed cost, the list cost
or a blend of the two. Offered only while custom costs are enabled and their
plugins started.
