---
kind: validation
routes:
  api: Cost API
steps:
- text: The User asks for asset costs over a window OpenCost does not recognise
  kind: actor
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::asset-costs
- text: The Product refuses the request as an invalid window
  kind: product
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::asset-costs
---

# Refuse an invalid asset window

## Trigger

The User sends a window OpenCost cannot read.

## Outcome

The User receives a bad request; nothing is computed.
