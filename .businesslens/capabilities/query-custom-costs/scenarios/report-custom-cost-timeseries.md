---
kind: alternative
routes:
  api: Cost API
steps:
- text: The User asks for custom costs over a window as a daily time series
  kind: actor
  actor: user
  entities:
  - entity: custom-cost
    effect: reads
    facts: []
  contexts:
    api:
      place: cost-api::cost-reporting::custom-costs
- text: The Product returns one grouped total per day of the window
  kind: product
  actor: user
  entities:
  - entity: custom-cost
    effect: reads
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
---

# Report custom cost time series

## Trigger

The User wants to see how external spend changes.

## Outcome

The User has a total per day.

## Edge cases

- Without a chosen step, windows within the hourly retention are reported per hour and longer ones per day.
