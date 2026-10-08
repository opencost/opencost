---
kind: edge
routes:
  api: Cost API
steps:
- text: The User asks how prices were assigned before OpenCost has costed any machine
  kind: actor
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::node-pricing
- text: The Product reports that machine costs are not yet calculated
  kind: product
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::node-pricing
---

# Node costs not yet calculated

## Trigger

OpenCost has just started.

## Outcome

The User receives an error and can ask again later.
