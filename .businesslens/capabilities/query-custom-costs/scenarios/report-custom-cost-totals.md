---
kind: primary
routes:
  api: Cost API
steps:
- text: The User asks for custom cost totals over the last week, grouped by domain
  kind: actor
  actor: user
  entities:
  - entity: custom-cost
    effect: reads
    facts: []
  contexts:
    api:
      place: cost-api::cost-reporting::custom-costs
- text: The Product sums each domain's Custom cost items, using the billed cost where there is one and the list cost otherwise
  kind: product
  actor: user
  entities:
  - entity: custom-cost
    effect: reads
    facts:
    - Domain
    - Cost source
    - Zone
    - Account name
    - Charge category
    - Description
    - Resource
    - Provider ID
    - Billed cost
    - List cost
    - List unit price
    - Usage
    - Window
  contexts:
    api:
      place: cost-api::cost-reporting::custom-costs
- text: The Product returns the grouped costs sorted by cost, largest first, with their total
  kind: product
  actor: user
  entities:
  - entity: custom-cost
    effect: reads
    facts:
    - Domain
    - Billed cost
    - List cost
  contexts:
    api:
      place: cost-api::cost-reporting::custom-costs
---

# Report custom cost totals

## Trigger

The User wants to see external spend next to cluster costs.

## Outcome

The User has each domain's cost for the week and the overall total.

## Edge cases

- Items whose chosen cost is zero are left out.
- A grouping property or filter the Product cannot parse is refused.
