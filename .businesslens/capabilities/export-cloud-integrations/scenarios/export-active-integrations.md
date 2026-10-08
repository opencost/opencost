---
kind: primary
routes:
  api: Cost API
steps:
- text: The Administrator asks for the cloud integration configurations
  kind: actor
  actor: administrator
  entities:
  - entity: cloud-integration
    effect: reads
    facts: []
  contexts:
    api:
      place: cost-api::administration::cloud-integrations
- text: The Product returns every active Cloud integration's configuration with each secret replaced by REDACTED
  kind: product
  actor: administrator
  entities:
  - entity: cloud-integration
    effect: reads
    facts:
    - Key
    - Provider
    - Integration type
    - Configuration
  contexts:
    api:
      place: cost-api::administration::cloud-integrations
---

# Export active integrations

## Trigger

The Administrator wants to copy or review the billing setup.

## Outcome

The Administrator has the configurations, grouped by provider, safe to share.
