---
kind: primary
routes:
  api: Cost API
steps:
- text: The User asks for inference costs over the last day
  kind: actor
  actor: user
  entities:
  - entity: inference-cost
    effect: reads
    facts: []
  contexts:
    api:
      place: cost-api::cost-reporting::inference-costs
- text: The Product joins the serving workloads' Allocation costs with the models' token counts
  kind: product
  actor: user
  entities:
  - entity: allocation
    effect: reads
    facts:
    - Namespace
    - Labels
    - Total cost
  contexts:
    api:
      place: cost-api::cost-reporting::inference-costs
- text: The Product returns each model's Inference cost per million tokens, split between input and output
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

# Report inference costs

## Trigger

The User wants to know what each served model costs per token.

## Outcome

The User has, per model and namespace, total cost, tokens, cost per million input and output tokens and cache savings.

## Edge cases

- With no tokens served, the usage-basis cost is zero.
- A grouping or filter property the Product does not support is refused.
