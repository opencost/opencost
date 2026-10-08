---
kind: edge
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
- text: The Product adds an entry that fails validation as an inactive Cloud integration
  kind: product
  entities:
  - entity: cloud-integration
    effect: creates
    to: Inactive
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

# Add an invalid integration from the file

## Trigger

An operator adds an incomplete billing export to the file.

## Outcome

The integration is listed as invalid and inactive and imports nothing.

## Edge cases

- A file that cannot be parsed is reported in the log and treated as holding no integrations.
