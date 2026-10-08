---
kind: validation
routes:
  api: Cost API
steps:
- text: The Administrator asks to repair cloud costs for a window OpenCost cannot read
  kind: actor
  actor: administrator
  entities:
  - entity: cloud-cost
    effect: reads
    facts: []
  contexts:
    api:
      place: cost-api::administration
- text: The Product refuses the request as an invalid parameter
  kind: product
  actor: administrator
  entities: []
  contexts:
    api:
      place: cost-api::administration
---

# Refuse a repair with an invalid window

## Trigger

The Administrator sends an unreadable window.

## Outcome

The Administrator receives a bad request; nothing is imported.
