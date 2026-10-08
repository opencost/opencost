---
kind: validation
routes:
  api: Cost API
steps:
- text: The Administrator asks for one Cloud integration's configuration by a key that is not active
  kind: actor
  actor: administrator
  entities:
  - entity: cloud-integration
    effect: reads
    facts:
    - Key
  contexts:
    api:
      place: cost-api::administration::cloud-integrations
- text: The Product refuses because no active integration has that key
  kind: product
  actor: administrator
  entities: []
  contexts:
    api:
      place: cost-api::administration::cloud-integrations
---

# Refuse exporting an unknown integration

## Trigger

The Administrator names an inactive or unknown key.

## Outcome

The Administrator receives a bad request.
