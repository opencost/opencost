---
availability:
- place: cost-api::administration
domain: cloud-costs
references:
- kind: code
  role: implementation
  target: pkg/cloud/config/controller_handlers.go#GetEnableConfigHandler
- kind: code
  role: implementation
  target: pkg/cloud/config/controller.go#EnableConfig
---

# Enable cloud integration

Let the Administrator activate an inactive cloud integration, so it imports
again. Offered only while cloud costs are enabled.
