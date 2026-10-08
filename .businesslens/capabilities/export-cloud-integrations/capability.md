---
availability:
- place: cost-api::administration
domain: cloud-costs
references:
- kind: code
  role: implementation
  target: pkg/cloud/config/controller_handlers.go#GetExportConfigHandler
---

# Export cloud integrations

Give the Administrator the configuration of every active cloud integration, or
of one, with its secrets redacted. Offered only while cloud costs are enabled.
