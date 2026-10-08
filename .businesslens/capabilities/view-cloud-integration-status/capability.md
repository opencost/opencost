---
availability:
- place: cost-api::cost-reporting
domain: cloud-costs
references:
- kind: code
  role: implementation
  target: pkg/cloudcost/pipelineservice.go#GetCloudCostStatusHandler
- kind: code
  role: implementation
  target: pkg/cloudcost/status.go
---

# View cloud integration status

Show every cloud integration OpenCost knows, active or not, with its redacted
configuration and how its imports are going. Offered only while cloud costs
are enabled.
