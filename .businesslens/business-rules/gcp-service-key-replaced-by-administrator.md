---
appliesTo:
- type: entity
  id: gcp-service-key
  effect: changes
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

# Only the administrator replaces the GCP service key

The Administrator stores a new GCP service key with the admin token. While no
admin token is configured, nobody can.
