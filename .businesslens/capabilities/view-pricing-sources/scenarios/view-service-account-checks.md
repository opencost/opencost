---
kind: alternative
routes:
  api: Cost API
steps:
- text: The User asks for the service account status
  kind: actor
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::pricing-sources
- text: The Product returns each Service account check with its status and details
  kind: product
  actor: user
  entities:
  - entity: service-account-check
    effect: reads
    facts:
    - Message
    - Status
    - Additional information
  contexts:
    api:
      place: cost-api::cost-reporting::pricing-sources
---

# View service account checks

## Trigger

The User wants to know whether the cloud credentials work.

## Outcome

The User sees each check and how to fix a failure.
