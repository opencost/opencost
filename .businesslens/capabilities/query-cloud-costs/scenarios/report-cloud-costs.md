---
kind: primary
routes:
  api: Cost API
steps:
- text: The User asks for cloud costs over the last week, grouped by service
  kind: actor
  actor: user
  entities:
  - entity: cloud-cost
    effect: reads
    facts: []
  contexts:
    api:
      place: cost-api::cost-reporting::cloud-costs
- text: The Product sums each day's Cloud cost items by service across every integration and returns one set per day
  kind: product
  actor: user
  entities:
  - entity: cloud-cost
    effect: reads
    facts:
    - Provider
    - Provider ID
    - Account
    - Invoice entity
    - Region
    - Service
    - Category
    - Labels
    - Day
    - List cost
    - Net cost
    - Amortized net cost
    - Invoiced cost
    - Amortized cost
    - Kubernetes percent
  contexts:
    api:
      place: cost-api::cost-reporting::cloud-costs
---

# Report cloud costs

## Trigger

The User wants to see what each cloud service cost.

## Outcome

The User has, for each day of the week, every service's cost under all five cost metrics and its Kubernetes share.

## Edge cases

- Asking to accumulate returns one set for the whole window, or one per hour, day, week, month or quarter.
- Items with no value for the grouping are reported under __unallocated__.
- A grouping or filter the Product cannot parse is refused.
