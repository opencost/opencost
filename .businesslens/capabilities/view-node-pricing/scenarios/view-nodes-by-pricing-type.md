---
kind: alternative
routes:
  api: Cost API
steps:
- text: The User asks how prices were assigned
  kind: actor
  actor: user
  entities:
  - entity: pricing
    effect: reads
    facts: []
  contexts:
    api:
      place: cost-api::cost-reporting::node-pricing
- text: The Product returns how many machines it priced each way, and the total
  kind: product
  actor: user
  entities:
  - entity: pricing
    effect: reads
    facts:
    - Nodes by pricing type
  contexts:
    api:
      place: cost-api::cost-reporting::node-pricing
---

# View nodes by pricing type

## Trigger

The User suspects nodes fell back to default prices.

## Outcome

The User has node counts per pricing type.
