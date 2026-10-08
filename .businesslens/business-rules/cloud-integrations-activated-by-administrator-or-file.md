---
appliesTo:
- type: entity
  id: cloud-integration
  effect: changes
  to: Active
permits:
- actors:
  - administrator
  when:
  - entity: opencost-settings
    fact: Admin token
    present: true
  - entity: opencost-settings
    fact: Cloud costs enabled
    is: true
- unattended: true
  when:
  - entity: opencost-settings
    fact: Cloud costs enabled
    is: true
references:
- kind: code
  role: implementation
  target: pkg/costmodel/router.go#adminAuthMiddleware
- kind: doc
  role: intent
  target: AGENTS.md
  title: Admin Auth
---

# Only the administrator or the cloud integration file activates a cloud integration

A cloud integration becomes active again when the Administrator enables it, or
when its entry in the cloud integration file changes and is valid.
