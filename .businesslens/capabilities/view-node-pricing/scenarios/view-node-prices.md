---
kind: primary
routes:
  api: Cost API
steps:
- text: The User asks for all pricing
  kind: actor
  actor: user
  entities:
  - entity: pricing
    effect: reads
    facts: []
  contexts:
    api:
      place: cost-api::cost-reporting::node-pricing
- text: The Product returns the prices it holds and the provider's parsed Pricing
  kind: product
  actor: user
  entities:
  - entity: pricing
    effect: reads
    facts:
    - Node prices
    - Pricing source summary
  contexts:
    api:
      place: cost-api::cost-reporting::node-pricing
---

# View node prices

## Trigger

The User wants to check the prices behind node costs.

## Outcome

The User has the prices in the provider's own shape.
