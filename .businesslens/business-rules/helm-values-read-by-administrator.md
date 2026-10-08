---
appliesTo:
- type: entity
  id: installation
  effect: reads
  facts:
  - Helm values
permits:
- actors:
  - administrator
  when:
  - entity: opencost-settings
    fact: Admin token
    present: true
  - entity: opencost-settings
    fact: Runs in Kubernetes
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

# Only the administrator reads the Helm values

The Helm values are returned only to the Administrator with the admin token.
While no admin token is configured, nobody can read them.

## Rationale

The values can carry credentials and are returned unredacted.
