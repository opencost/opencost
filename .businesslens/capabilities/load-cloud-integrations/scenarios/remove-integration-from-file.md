---
kind: alternative
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
- text: The Product removes each Cloud integration no longer in the file and stops its imports
  kind: product
  entities:
  - entity: cloud-integration
    effect: removes
    from: Active
  contexts:
    api:
      place: cost-api::cost-reporting::cloud-integration-status
---

# Remove an integration from the file

## Trigger

An operator takes a billing export out of the file.

## Outcome

The integration is no longer listed; the cloud costs it imported stay until another active integration's import discards them as expired, or OpenCost restarts.

## Edge cases

- An inactive integration taken out of the file is removed the same way.
