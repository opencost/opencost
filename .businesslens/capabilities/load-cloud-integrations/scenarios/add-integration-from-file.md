---
kind: primary
routes:
  api: Cost API
steps:
- text: Ten seconds after the last check, the Product reads the cloud integration file
  kind: condition
  unattended: true
  entities:
  - entity: cloud-integration
    effect: reads
    facts: []
  contexts:
    api:
      place: cost-api::cost-reporting::cloud-integration-status
- text: The Product adds a new valid entry as an active Cloud integration
  kind: product
  entities:
  - entity: cloud-integration
    effect: creates
    to: Active
    facts:
    - Key
    - Source
    - Provider
    - Integration type
    - Configuration
    - Valid
  contexts:
    api:
      place: cost-api::cost-reporting::cloud-integration-status
---

# Add an integration from the file

## Trigger

An operator adds a billing export to the cloud integration file.

## Outcome

The new integration is active and starts importing.

## Edge cases

- An entry whose configuration changed is replaced: it becomes active again if valid, ending any earlier disable, and inactive if not.
