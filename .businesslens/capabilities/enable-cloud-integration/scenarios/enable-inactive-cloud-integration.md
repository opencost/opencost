---
kind: primary
routes:
  api: Cost API
steps:
- text: The Administrator names an inactive Cloud integration by its key and source
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
- text: The Product activates it and starts its imports
  kind: product
  actor: administrator
  entities:
  - entity: cloud-integration
    effect: changes
    from: Inactive
    to: Active
    facts: []
  contexts:
    api:
      place: cost-api::administration::cloud-integrations
- text: The Product replies that the integration was enabled
  kind: product
  actor: administrator
  entities: []
  contexts:
    api:
      place: cost-api::administration::cloud-integrations
---

# Enable an integration

## Trigger

The Administrator wants a disabled integration to import again.

## Outcome

The integration is active and its imports start.

## Edge cases

- An integration that failed validation can be enabled.
- The enabled state lasts until the file entry changes or OpenCost restarts.
