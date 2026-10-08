---
kind: validation
routes:
  api: Cost API
steps:
- text: The User asks for an inference cost time series without saying hour, day, week or month
  kind: actor
  actor: user
  entities:
  - entity: inference-cost
    effect: reads
    facts: []
  contexts:
    api:
      place: cost-api::cost-reporting::inference-costs
- text: The Product refuses because the time series needs a step
  kind: product
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::inference-costs
---

# Refuse a time series without a step

## Trigger

The User leaves out the step.

## Outcome

The User receives a bad request.
