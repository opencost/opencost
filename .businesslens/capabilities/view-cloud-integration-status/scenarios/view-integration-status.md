---
kind: primary
routes:
  api: Cost API
steps:
- text: The User asks for the status of cloud integrations
  kind: actor
  actor: user
  entities:
  - entity: cloud-integration
    effect: reads
    facts: []
  contexts:
    api:
      place: cost-api::cost-reporting::cloud-integration-status
- text: The Product returns each Cloud integration with its connection status, runs and coverage
  kind: product
  actor: user
  entities:
  - entity: cloud-integration
    effect: reads
    facts:
    - Key
    - Source
    - Provider
    - Integration type
    - Configuration
    - Valid
    - Connection status
    - Last run
    - Next run
    - Runs
    - Coverage
    - Refresh rate
  contexts:
    api:
      place: cost-api::cost-reporting::cloud-integration-status
---

# View integration status

## Trigger

The User wants to know whether cloud billing data is arriving.

## Outcome

The User sees every integration, active and inactive, with secrets redacted.
