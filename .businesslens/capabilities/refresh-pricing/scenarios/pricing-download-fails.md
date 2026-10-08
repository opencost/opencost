---
kind: edge
routes:
  api: Cost API
steps:
- text: The User asks OpenCost to refresh pricing
  kind: actor
  actor: user
  entities:
  - entity: pricing
    effect: reads
    facts: []
  contexts:
    api:
      place: cost-api::cost-reporting::node-pricing
- text: The Product cannot download the prices and reports the error
  kind: product
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::node-pricing
---

# Pricing download fails

## Trigger

The provider's pricing source is unreachable.

## Outcome

The User receives the error; the prices held are unchanged.
