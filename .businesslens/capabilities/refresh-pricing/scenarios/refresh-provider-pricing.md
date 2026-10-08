---
kind: primary
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
- text: The Product downloads the provider's prices again and replaces the prices it holds
  kind: product
  actor: user
  entities:
  - entity: pricing
    effect: changes
    facts:
    - Node prices
  contexts:
    api:
      place: cost-api::cost-reporting::node-pricing
---

# Refresh provider pricing

## Trigger

New node types joined the cluster or prices changed.

## Outcome

Node costs use the newly downloaded prices.
