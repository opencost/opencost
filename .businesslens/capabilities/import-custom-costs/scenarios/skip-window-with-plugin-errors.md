---
kind: edge
routes:
  api: Cost API
steps:
- text: An hour, or a day, has passed since the last import
  kind: condition
  unattended: true
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::custom-costs
- text: A Custom cost plugin reports errors for a window
  kind: condition
  entities:
  - entity: custom-cost-plugin
    effect: reads
    facts:
    - Domain
  contexts:
    api:
      place: cost-api::cost-reporting::custom-cost-status
- text: The Product stores nothing from that plugin for the window
  kind: product
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::custom-costs
---

# Skip a window with plugin errors

## Trigger

A vendor API fails for a period.

## Outcome

That plugin's costs for the window are missing; other plugins are unaffected.
