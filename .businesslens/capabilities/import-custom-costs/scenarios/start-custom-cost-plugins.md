---
kind: primary
routes:
  api: Cost API
steps:
- text: OpenCost starts with plugins enabled
  kind: condition
  unattended: true
  entities:
  - entity: opencost-settings
    effect: reads
    facts:
    - Custom costs enabled
  contexts:
    api:
      place: cost-api::cost-reporting::custom-cost-status
- text: The Product launches each plugin that has a configuration file and a matching executable
  kind: product
  entities:
  - entity: custom-cost-plugin
    effect: creates
    facts:
    - Domain
    - Hourly coverage
    - Daily coverage
  contexts:
    api:
      place: cost-api::cost-reporting::custom-cost-status
---

# Start custom cost plugins

## Trigger

OpenCost starts.

## Outcome

Each installed Custom cost plugin is running and listed by domain.
