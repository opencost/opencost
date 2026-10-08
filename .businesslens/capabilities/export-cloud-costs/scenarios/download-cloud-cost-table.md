---
kind: primary
routes:
  api: Cost API
steps:
- text: The User asks for last week's cloud cost table, grouped by service, as CSV
  kind: actor
  actor: user
  entities:
  - entity: cloud-cost
    effect: reads
    facts: []
  contexts:
    api:
      place: cost-api::cost-reporting::cloud-costs
- text: The Product sums the Cloud cost items by service under the chosen cost metric and returns one CSV row per service
  kind: product
  actor: user
  entities:
  - entity: cloud-cost
    effect: reads
    facts:
    - Service
    - Day
    - Amortized net cost
    - Kubernetes percent
  contexts:
    api:
      place: cost-api::cost-reporting::cloud-costs
---

# Download the cloud cost table

## Trigger

The User wants the cloud cost table in a spreadsheet.

## Outcome

The User has a CSV file with each service's name, Kubernetes share, cost and the window.

## Edge cases

- A cost metric, sort field or order the Product does not know is refused before anything is read.
