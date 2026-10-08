---
kind: primary
routes:
  api: Cost API
steps:
- text: The Administrator names an active Cloud integration by its key and source
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
- text: The Product deactivates it and stops its imports
  kind: product
  actor: administrator
  entities:
  - entity: cloud-integration
    effect: changes
    from: Active
    to: Inactive
    facts: []
  contexts:
    api:
      place: cost-api::administration::cloud-integrations
- text: The Product replies that the integration was disabled
  kind: product
  actor: administrator
  entities: []
  contexts:
    api:
      place: cost-api::administration::cloud-integrations
---

# Disable an integration

## Trigger

The Administrator wants an integration to stop importing.

## Outcome

The integration is inactive; its cloud costs stay until another active integration's import discards them as expired, or OpenCost restarts.

## Edge cases

- The disabled state lasts until the file entry changes or OpenCost restarts.
