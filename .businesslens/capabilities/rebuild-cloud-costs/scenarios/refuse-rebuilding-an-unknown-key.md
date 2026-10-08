---
kind: validation
routes:
  api: Cost API
steps:
- text: The Administrator asks to rebuild a key no Cloud integration imports and confirms
  kind: actor
  actor: administrator
  entities:
  - entity: cloud-integration
    effect: reads
    facts:
    - Key
  contexts:
    api:
      place: cost-api::administration
- text: The Product refuses because no integration with that key exists
  kind: product
  actor: administrator
  entities: []
  contexts:
    api:
      place: cost-api::administration
---

# Refuse rebuilding an unknown integration

## Trigger

The Administrator mistypes the key.

## Outcome

The Administrator receives a bad request; nothing is rebuilt.
