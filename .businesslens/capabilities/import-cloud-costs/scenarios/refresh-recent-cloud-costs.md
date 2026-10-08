---
kind: alternative
routes:
  api: Cost API
steps:
- text: The refresh interval has passed since an active Cloud integration last imported
  kind: condition
  unattended: true
  entities:
  - entity: cloud-integration
    effect: reads
    facts:
    - Last run
    - Refresh rate
  contexts:
    api:
      place: cost-api::cost-reporting::cloud-integration-status
- text: The Product imports again the days since three days before the last run, and from the start of the month on every sixth run
  kind: product
  entities:
  - entity: cloud-cost
    effect: changes
    facts:
    - List cost
    - Net cost
    - Amortized net cost
    - Invoiced cost
    - Amortized cost
    - Kubernetes percent
  contexts:
    api:
      place: cost-api::cost-reporting::cloud-costs
- text: The Product records the outcome on the Cloud integration
  kind: product
  entities:
  - entity: cloud-integration
    effect: changes
    facts:
    - Connection status
    - Last run
    - Next run
    - Runs
    - Coverage
  contexts:
    api:
      place: cost-api::cost-reporting::cloud-integration-status
---

# Refresh recent cloud costs

## Trigger

Six hours, or the configured refresh rate, have passed.

## Outcome

Recent cloud costs reflect the provider's latest billing data.
