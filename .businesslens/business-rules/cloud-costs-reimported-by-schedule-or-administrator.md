---
appliesTo:
- type: entity
  id: cloud-cost
  effect: changes
permits:
- unattended: true
  when:
  - entity: opencost-settings
    fact: Cloud costs enabled
    is: true
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

# Only the Product's schedule or the administrator re-imports cloud costs

Existing cloud costs are overwritten by scheduled refreshes, or when the
Administrator rebuilds or repairs them with the admin token.
