---
kind: validation
routes:
  api: Cost API
steps:
- text: The User asks for the cloud cost table as CSV without a window
  kind: actor
  actor: user
  entities:
  - entity: cloud-cost
    effect: reads
    facts: []
  contexts:
    api:
      place: cost-api::cost-reporting::cloud-costs
- text: The Product refuses the request because the window is required
  kind: product
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::cloud-costs
---

# Refuse a cloud cost export without a window

## Trigger

The User leaves out the window.

## Outcome

The User receives a bad request and no file.
