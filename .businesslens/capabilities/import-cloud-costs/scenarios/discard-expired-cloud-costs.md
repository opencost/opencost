---
kind: alternative
routes:
  api: Cost API
steps:
- text: An import run has finished
  kind: condition
  unattended: true
  entities:
  - entity: opencost-settings
    effect: reads
    facts:
    - Cloud cost retention
  contexts:
    api:
      place: cost-api::cost-reporting::cloud-costs
- text: The Product discards every Cloud cost older than the retention period
  kind: product
  entities:
  - entity: cloud-cost
    effect: removes
  contexts:
    api:
      place: cost-api::cost-reporting::cloud-costs
- text: The Product shrinks the Cloud integration's coverage to match
  kind: product
  entities:
  - entity: cloud-integration
    effect: changes
    facts:
    - Coverage
  contexts:
    api:
      place: cost-api::cost-reporting::cloud-integration-status
---

# Discard expired cloud costs

## Trigger

A run completes.

## Outcome

Only the retention period's cloud costs remain.
