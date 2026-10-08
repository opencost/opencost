---
kind: validation
routes:
  api: Cost API
steps:
- text: The User asks for costs over a window OpenCost does not recognise
  kind: actor
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::allocation-costs
- text: The Product refuses the request as an invalid window
  kind: product
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::allocation-costs
---

# Refuse an invalid allocation window

## Trigger

The User sends a window that is neither a named period, a duration nor a pair of times.

## Outcome

The User receives a bad request naming the illegal window; nothing is computed.
