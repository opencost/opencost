---
availability:
- place: cost-api::administration
domain: cloud-costs
references:
- kind: code
  role: implementation
  target: pkg/cloud/config/controller_handlers.go#GetDisableConfigHandler
- kind: code
  role: implementation
  target: pkg/cloud/config/controller.go#DisableConfig
---

# Disable cloud integration

Let the Administrator stop an active cloud integration's imports while keeping
the integration and the cloud costs it already imported. Offered only while
cloud costs are enabled.
