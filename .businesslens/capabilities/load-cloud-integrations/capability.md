---
availability:
- place: cost-api::cost-reporting
domain: cloud-costs
references:
- kind: code
  role: implementation
  target: pkg/cloud/config/controller.go
- kind: code
  role: implementation
  target: pkg/cloud/config/watcher.go
- kind: code
  role: implementation
  target: pkg/cloud/config/configurations.go
---

# Load cloud integrations

Keep OpenCost's cloud integrations in step with the cloud integration file:
every ten seconds the Product reads the file, adds integrations that appear,
replaces those that changed and removes those that are gone. Runs only while
cloud costs are enabled. Alibaba entries are listed like any other but import
nothing.
