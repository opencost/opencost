---
kind: validation
routes:
  api: Cost API
steps:
- text: The User asks for proportional asset resource costs without including idle
  kind: actor
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::allocation-costs
- text: The Product refuses the request because idle must be included
  kind: product
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::allocation-costs
---

# Refuse proportional asset costs without idle

## Trigger

The User wants each workload's share of node and disk cost but leaves idle out.

## Outcome

The User receives a bad request saying idle must be included.
