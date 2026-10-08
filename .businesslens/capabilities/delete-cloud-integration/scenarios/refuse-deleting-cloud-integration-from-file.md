---
kind: validation
routes:
  api: Cost API
steps:
- text: The Administrator asks to delete a Cloud integration by its key
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
- text: The Product refuses because no integration with that key was added through the cost API
  kind: product
  actor: administrator
  entities: []
  contexts:
    api:
      place: cost-api::administration::cloud-integrations
---

# Refuse deleting a file integration

## Trigger

The Administrator wants an integration gone.

## Outcome

The Administrator receives a bad request; the integration stays until it is taken out of the file.
