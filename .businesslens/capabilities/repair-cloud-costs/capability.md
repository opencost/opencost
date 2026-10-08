---
availability:
- place: cost-api::administration
domain: cloud-costs
references:
- kind: code
  role: implementation
  target: pkg/cloudcost/pipelineservice.go#GetCloudCostRepairHandler
- kind: code
  role: implementation
  target: pkg/cloudcost/ingestionmanager.go#RepairAll
---

# Repair cloud costs

Let the Administrator re-import the days of one window, for every active cloud
integration or one, in the background. Offered only while cloud costs are
enabled.
