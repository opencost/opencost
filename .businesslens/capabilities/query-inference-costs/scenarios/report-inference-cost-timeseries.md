---
kind: alternative
routes:
  api: Cost API
steps:
- text: The User asks for inference costs over a week, hour by hour
  kind: actor
  actor: user
  entities:
  - entity: inference-cost
    effect: reads
    facts: []
  contexts:
    api:
      place: cost-api::cost-reporting::inference-costs
- text: The Product returns one set of Inference cost items per hour
  kind: product
  actor: user
  entities:
  - entity: inference-cost
    effect: reads
    facts:
    - Model
    - Namespace
    - Workload type
    - Cost basis
    - Window
    - Total cost
    - Tokens
    - Cost per million tokens
    - Input cost
    - Output cost
    - Cache savings fraction
    - Allocation method
  contexts:
    api:
      place: cost-api::cost-reporting::inference-costs
---

# Report inference cost time series

## Trigger

The User wants to see how serving costs change.

## Outcome

The User has a set per hour.
