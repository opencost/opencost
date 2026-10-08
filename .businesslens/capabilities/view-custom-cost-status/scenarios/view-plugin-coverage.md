---
kind: primary
routes:
  api: Cost API
steps:
- text: The User asks for the plugin status
  kind: actor
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::custom-cost-status
- text: The Product returns each Custom cost plugin with its hourly and daily coverage and the refresh rates
  kind: product
  actor: user
  entities:
  - entity: custom-cost-plugin
    effect: reads
    facts:
    - Domain
    - Hourly coverage
    - Daily coverage
  contexts:
    api:
      place: cost-api::cost-reporting::custom-cost-status
---

# View plugin coverage

## Trigger

The User wants to know whether plugin data is arriving.

## Outcome

The User sees each plugin's coverage.
