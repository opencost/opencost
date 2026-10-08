---
kind: edge
routes:
  api: Cost API
steps:
- text: The User asks for inference costs while they are disabled
  kind: actor
  actor: user
  entities:
  - entity: inference-cost
    effect: reads
    facts: []
  contexts:
    api:
      place: cost-api::cost-reporting::inference-costs
- text: The Product reports that the service is not available
  kind: product
  actor: user
  entities:
  - entity: opencost-settings
    effect: reads
    facts:
    - Inference costs enabled
  contexts:
    api:
      place: cost-api::cost-reporting::inference-costs
---

# Inference costs disabled

## Trigger

The User queries an installation that does not compute inference costs.

## Outcome

The User receives a not-implemented response rather than a missing page.
