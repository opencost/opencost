---
availability:
- place: cost-api::cost-reporting
domain: cloud-costs
references:
- kind: code
  role: implementation
  target: pkg/cloudcost/queryservice.go#GetCloudCostViewTableHandler
- kind: code
  role: implementation
  target: pkg/cloudcost/queryservice_helper.go#CloudCostViewTableRowsToCSV
---

# Export cloud costs

Download the cloud cost table for a window and one cost metric as a CSV file:
each grouped item's name, Kubernetes share and cost, with the window. Offered
only while cloud costs are enabled.
