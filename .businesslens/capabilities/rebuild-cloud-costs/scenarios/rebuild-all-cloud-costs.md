---
kind: primary
routes:
  api: Cost API
steps:
- text: The Administrator asks to rebuild cloud costs and confirms
  kind: actor
  actor: administrator
  entities:
  - entity: cloud-cost
    effect: reads
    facts: []
  contexts:
    api:
      place: cost-api::administration
- text: The Product restarts every active Cloud integration's import and queries every day of the retention period again
  kind: product
  actor: administrator
  entities:
  - entity: cloud-integration
    effect: reads
    facts:
    - Key
  - entity: cloud-cost
    effect: changes
    facts:
    - List cost
    - Net cost
    - Amortized net cost
    - Invoiced cost
    - Amortized cost
    - Kubernetes percent
  contexts:
    api:
      place: cost-api::administration
- text: The Product replies that rebuilding has started for all providers
  kind: product
  actor: administrator
  entities: []
  contexts:
    api:
      place: cost-api::administration
---

# Rebuild all cloud costs

## Trigger

The Administrator suspects imported cloud costs are wrong.

## Outcome

Every day held is imported again, overwriting what was there.
