---
availability:
- place: cost-api::cost-reporting
domain: cloud-costs
references:
- kind: code
  role: implementation
  target: pkg/cloudcost/ingestor.go
- kind: code
  role: implementation
  target: pkg/cloudcost/ingestionmanager.go
- kind: code
  role: implementation
  target: pkg/cloudcost/memoryrepository.go
---

# Import cloud costs

Import each active cloud integration's billing data on a schedule: when it
becomes active, every day of the retention period not yet imported; then, every
refresh interval, the last few days again, with every sixth run covering the
month to date. Costs older than the retention period are discarded. Runs only
while cloud costs are enabled.
