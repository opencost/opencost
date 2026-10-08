---
kind: validation
routes:
  api: Cost API
steps:
- text: The Administrator names a Cloud integration that is already inactive
  kind: actor
  actor: administrator
  entities:
  - entity: cloud-integration
    effect: reads
    facts:
    - Key
    - Source
  contexts:
    api:
      place: cost-api::administration::cloud-integrations
- text: The Product refuses because the integration is already disabled
  kind: product
  actor: administrator
  entities: []
  contexts:
    api:
      place: cost-api::administration::cloud-integrations
---

# Refuse disabling an inactive integration

## Trigger

The Administrator disables an integration twice.

## Outcome

The Administrator receives a bad request; nothing changes.
