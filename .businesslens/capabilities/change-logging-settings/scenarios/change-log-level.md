---
kind: primary
routes:
  api: Cost API
steps:
- text: The User reads the current log level
  kind: actor
  actor: user
  entities:
  - entity: logging-settings
    effect: reads
    facts:
    - Log level
  contexts:
    api:
      place: cost-api::cost-reporting::logging-settings
- text: The User sends debug as the new level
  kind: actor
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::logging-settings
- text: The Product applies the new level at once
  kind: product
  actor: user
  entities:
  - entity: logging-settings
    effect: changes
    facts:
    - Log level
  contexts:
    api:
      place: cost-api::cost-reporting::logging-settings
---

# Change the log level

## Trigger

The User needs more detail to diagnose a problem.

## Outcome

OpenCost logs at the new level until it restarts.
