---
kind: validation
routes:
  api: Cost API
steps:
- text: The Administrator calls this administration operation without the configured admin token
  kind: actor
  actor: administrator
  entities: []
  contexts:
    api:
      place: cost-api::administration
- text: The admin token is not configured, or the request does not carry it
  kind: condition
  entities:
  - entity: opencost-settings
    effect: reads
    facts:
    - Admin token
  contexts:
    api:
      place: cost-api::administration
- text: The Product refuses the request before doing anything
  kind: product
  actor: administrator
  entities: []
  contexts:
    api:
      place: cost-api::administration
references:
- kind: code
  role: implementation
  target: pkg/costmodel/router.go#adminAuthMiddleware
---

# Refuse rebuilding cloud costs without the admin token

## Trigger

The request has no bearer token, a different one, or reaches an installation with no admin token.

## Outcome

The Administrator receives service unavailable while no admin token is configured, unauthorized without a bearer token, or forbidden with a different one. Nothing is imported again.
