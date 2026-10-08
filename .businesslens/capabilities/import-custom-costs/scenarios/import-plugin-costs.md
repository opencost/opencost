---
kind: primary
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
- text: The Product asks each Custom cost plugin for its costs since the last run less the query window
  kind: product
  entities:
  - entity: custom-cost-plugin
    effect: reads
    facts:
    - Domain
  contexts:
    api:
      place: cost-api::cost-reporting::custom-cost-status
- text: The Product stores each reported Custom cost
  kind: product
  entities:
  - entity: custom-cost
    effect: creates
    facts:
    - Domain
    - Cost source
    - Zone
    - Account name
    - Charge category
    - Description
    - Resource
    - Provider ID
    - Billed cost
    - List cost
    - List unit price
    - Usage
    - Window
  contexts:
    api:
      place: cost-api::cost-reporting::custom-costs
- text: The Product extends the plugin's coverage
  kind: product
  entities:
  - entity: custom-cost-plugin
    effect: changes
    facts:
    - Hourly coverage
    - Daily coverage
  contexts:
    api:
      place: cost-api::cost-reporting::custom-cost-status
---

# Import plugin costs

## Trigger

The hourly or daily schedule fires.

## Outcome

The plugins' latest costs can be queried.
