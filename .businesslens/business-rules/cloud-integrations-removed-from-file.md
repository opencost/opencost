---
appliesTo:
- type: entity
  id: cloud-integration
  effect: removes
permits:
- unattended: true
  when:
  - entity: opencost-settings
    fact: Cloud costs enabled
    is: true
references:
- kind: code
  role: implementation
  target: pkg/cloud/config/controller.go#DeleteConfig
---

# Cloud integrations are removed only by leaving the cloud integration file

A cloud integration disappears only when its entry is taken out of the cloud
integration file; asking the cost API to delete it is refused.
