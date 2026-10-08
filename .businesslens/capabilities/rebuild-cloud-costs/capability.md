---
availability:
- place: cost-api::administration
domain: cloud-costs
references:
- kind: code
  role: implementation
  target: pkg/cloudcost/pipelineservice.go#GetCloudCostRebuildHandler
- kind: code
  role: implementation
  target: pkg/cloudcost/ingestionmanager.go#RebuildAll
---

# Rebuild cloud costs

Let the Administrator re-import every day of the retention period, for every
active cloud integration or one, after confirming. Offered only while cloud
costs are enabled.
