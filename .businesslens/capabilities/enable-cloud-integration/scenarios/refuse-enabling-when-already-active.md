---
kind: validation
routes:
  api: Cost API
steps:
- text: The Administrator names a Cloud integration that is already active
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
- text: The Product refuses because the integration is already active
  kind: product
  actor: administrator
  entities: []
  contexts:
    api:
      place: cost-api::administration::cloud-integrations
---

# Refuse enabling an active integration

## Trigger

The Administrator enables an integration twice.

## Outcome

The Administrator receives a bad request; nothing changes.

## Edge cases

- A missing key or source, or a key and source OpenCost does not know, is refused the same way.
