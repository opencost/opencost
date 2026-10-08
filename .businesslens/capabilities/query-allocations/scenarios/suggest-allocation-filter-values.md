---
kind: alternative
routes:
  api: Cost API
steps:
- text: The User asks which namespaces matching a search text had costs in a window
  kind: actor
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::allocation-costs
- text: The Product returns the matching namespace values of every Allocation, sorted and without duplicates, up to the limit
  kind: product
  actor: user
  entities:
  - entity: allocation
    effect: reads
    facts:
    - Namespace
  contexts:
    api:
      place: cost-api::cost-reporting::allocation-costs
---

# Suggest allocation filter values

## Trigger

The User is building a filter and wants the values it can take.

## Outcome

The User has the namespaces that match, at most one hundred unless they ask for up to a thousand.

## Edge cases

- Clusters, nodes, controller kinds, controllers, pods, containers, labels and namespace labels can be suggested the same way.
- A limit above one thousand is refused.
