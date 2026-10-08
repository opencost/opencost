---
availability:
- place: cost-api::cost-reporting
domain: custom-costs
references:
- kind: code
  role: implementation
  target: pkg/customcost/pipelineservice.go#NewPipelineService
- kind: code
  role: implementation
  target: pkg/customcost/ingestor.go
- kind: doc
  role: context
  target: https://github.com/opencost/opencost-plugins
---

# Import custom costs

Start the custom cost plugins an operator installed and import what they
report: at start-up, every hour and day of the custom cost retention not yet
held; then hourly costs every hour and daily costs every day, each run
re-querying recent periods. Imported custom costs are kept until OpenCost
restarts; nothing discards them earlier. Runs only while custom costs are
enabled.
