---
kind: validation
routes:
  api: Cost API
steps:
- text: The User sends a level OpenCost does not know
  kind: actor
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::logging-settings
- text: The Product refuses the request and keeps the current level
  kind: product
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::logging-settings
---

# Refuse an unknown log level

## Trigger

The User mistypes the level.

## Outcome

The User receives a bad request naming the level given.
