---
kind: edge
routes:
  api: Cost API
steps:
- text: The User asks for costs over a window
  kind: actor
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::allocation-costs
- text: The Product cannot read the usage metrics for the window from Prometheus or its collector
  kind: condition
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::allocation-costs
- text: The Product reports the error and returns no costs
  kind: product
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::allocation-costs
---

# Metrics source unavailable

## Trigger

Prometheus is unreachable or a query against it fails.

## Outcome

The User receives an internal server error; no partial costs are returned.
