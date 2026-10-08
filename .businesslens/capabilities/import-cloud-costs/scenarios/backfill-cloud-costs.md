---
kind: primary
routes:
  api: Cost API
steps:
- text: A Cloud integration has just become active
  kind: condition
  unattended: true
  entities:
  - entity: cloud-integration
    effect: reads
    facts:
    - Key
    - Provider
  contexts:
    api:
      place: cost-api::cost-reporting::cloud-integration-status
- text: The Product queries the provider's billing export, a week at a time, for every day of the retention period it does not hold
  kind: product
  entities:
  - entity: opencost-settings
    effect: reads
    facts:
    - Cloud cost retention
  contexts:
    api:
      place: cost-api::cost-reporting::cloud-costs
- text: The Product stores each billed item of each day as a Cloud cost
  kind: product
  entities:
  - entity: cloud-cost
    effect: creates
    facts:
    - Provider
    - Provider ID
    - Account
    - Invoice entity
    - Region
    - Service
    - Category
    - Labels
    - Day
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

# Backfill cloud costs

## Trigger

An integration is added to the file, enabled, or loaded at start-up.

## Outcome

The retention period is covered with cloud costs and the integration reports a successful connection.
