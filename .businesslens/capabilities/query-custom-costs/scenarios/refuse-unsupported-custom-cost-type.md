---
kind: validation
routes:
  api: Cost API
steps:
- text: The User asks for custom costs with a cost type other than blended, list or billed
  kind: actor
  actor: user
  entities:
  - entity: custom-cost
    effect: reads
    facts: []
  contexts:
    api:
      place: cost-api::cost-reporting::custom-costs
- text: The Product refuses the request naming the unsupported cost type
  kind: product
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::custom-costs
---

# Refuse an unsupported custom cost type

## Trigger

The User sends an unknown cost type.

## Outcome

The User receives a bad request; nothing is read.

## Edge cases

- A missing window is refused the same way.
