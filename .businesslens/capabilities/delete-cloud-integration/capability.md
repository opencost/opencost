---
availability:
- place: cost-api::administration
domain: cloud-costs
references:
- kind: code
  role: implementation
  target: pkg/cloud/config/controller_handlers.go#GetDeleteConfigHandler
- kind: code
  role: implementation
  target: pkg/cloud/config/controller.go#DeleteConfig
---

# Delete cloud integration

Let the Administrator delete a cloud integration added through the cost API.
Integrations from the cloud integration file can only be removed by editing
the file, and the cost API offers no way to add one, so every delete is
refused. Offered only while cloud costs are enabled.
