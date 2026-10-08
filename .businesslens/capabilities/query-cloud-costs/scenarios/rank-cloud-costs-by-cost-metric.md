---
kind: alternative
routes:
  api: Cost API
steps:
- text: The User asks for the totals, graph or table of cloud costs for one cost metric, sorted and paged
  kind: actor
  actor: user
  entities:
  - entity: cloud-cost
    effect: reads
    facts: []
  contexts:
    api:
      place: cost-api::cost-reporting::cloud-costs
- text: The Product ranks the grouped Cloud cost items by the chosen cost metric and returns the requested shape
  kind: product
  actor: user
  entities:
  - entity: cloud-cost
    effect: reads
    facts:
    - Service
    - Labels
    - Day
    - Amortized net cost
    - Kubernetes percent
  contexts:
    api:
      place: cost-api::cost-reporting::cloud-costs
---

# Rank cloud costs by cost metric

## Trigger

The User wants the biggest cloud costs at a glance.

## Outcome

The User has the top costs by amortized net cost, the default metric, or the metric they chose.

## Edge cases

- The graph keeps the nine largest items per day and folds the rest into Other.
- A cost metric, sort field or order the Product does not know is refused.
