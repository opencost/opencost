---
appliesTo:
- type: entity
  id: cloud-integration
  effect: creates
permits:
- unattended: true
  when:
  - entity: opencost-settings
    fact: Cloud costs enabled
    is: true
references:
- kind: code
  role: implementation
  target: pkg/cloud/config/watcher.go
- kind: code
  role: implementation
  target: pkg/costmodel/router.go#InitializeCloudCost
---

# Cloud integrations are added only from the cloud integration file

OpenCost adds cloud integrations only by reading the cloud integration file; no
one adds one through the cost API.
