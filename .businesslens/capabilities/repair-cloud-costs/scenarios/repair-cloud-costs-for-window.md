---
kind: primary
routes:
  api: Cost API
steps:
- text: The Administrator asks to repair cloud costs for the last three days
  kind: actor
  actor: administrator
  entities:
  - entity: cloud-cost
    effect: reads
    facts: []
  contexts:
    api:
      place: cost-api::administration
- text: The Product starts importing those whole days again for every active Cloud integration and replies at once
  kind: product
  actor: administrator
  entities:
  - entity: cloud-integration
    effect: reads
    facts:
    - Key
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
      place: cost-api::administration
---

# Repair cloud costs for a window

## Trigger

A provider corrected recent billing data.

## Outcome

Those days' cloud costs are replaced once the background import finishes.

## Edge cases

- Naming an integration key repairs only that integration; an unknown key is refused.
