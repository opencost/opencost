---
kind: alternative
routes:
  api: Cost API
steps:
- text: The User asks which asset types or provider IDs matching a search text exist in a window
  kind: actor
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::asset-costs
- text: The Product returns the matching values, sorted and without duplicates, up to the limit
  kind: product
  actor: user
  entities:
  - entity: node
    effect: reads
    facts:
    - Name
    - Provider
    - Provider ID
    - Labels
  - entity: disk
    effect: reads
    facts:
    - Name
    - Provider ID
  contexts:
    api:
      place: cost-api::cost-reporting::asset-costs
---

# Suggest asset filter values

## Trigger

The User is building an asset filter.

## Outcome

The User has the matching values.

## Edge cases

- Asset types and categories come from a fixed list rather than the window's data.
