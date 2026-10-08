---
appliesTo:
- type: entity
  id: cloud-integration
  effect: reads
  facts:
  - Configuration
  contexts:
  - place: cost-api::administration
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
references:
- kind: code
  role: implementation
  target: pkg/costmodel/router.go#adminAuthMiddleware
- kind: doc
  role: intent
  target: AGENTS.md
  title: Admin Auth
---

# Only the administrator exports cloud integration configurations

Exporting cloud integration configurations is reserved to the Administrator with
the admin token, while cloud costs are enabled.
