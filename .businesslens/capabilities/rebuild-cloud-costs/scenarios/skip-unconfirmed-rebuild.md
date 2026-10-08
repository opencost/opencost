---
kind: edge
routes:
  api: Cost API
steps:
- text: The Administrator asks to rebuild cloud costs without confirming
  kind: actor
  actor: administrator
  entities:
  - entity: cloud-cost
    effect: reads
    facts: []
  contexts:
    api:
      place: cost-api::administration
- text: The Product replies with how to confirm and rebuilds nothing
  kind: product
  actor: administrator
  entities: []
  contexts:
    api:
      place: cost-api::administration
---

# Skip an unconfirmed rebuild

## Trigger

The Administrator leaves out the confirmation.

## Outcome

Nothing changes; the reply explains how to confirm.
