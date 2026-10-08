---
kind: alternative
routes:
  api: Cost API
steps:
- text: The User asks which services matching a search text appear in a window
  kind: actor
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::cloud-costs
- text: The Product returns the matching values, sorted and without duplicates, up to the limit
  kind: product
  actor: user
  entities:
  - entity: cloud-cost
    effect: reads
    facts:
    - Service
  contexts:
    api:
      place: cost-api::cost-reporting::cloud-costs
---

# Suggest cloud cost filter values

## Trigger

The User is building a cloud cost filter.

## Outcome

The User has the matching service names.

## Edge cases

- Any cloud cost property, label key or label value can be suggested the same way.
- A limit above one thousand is refused.
