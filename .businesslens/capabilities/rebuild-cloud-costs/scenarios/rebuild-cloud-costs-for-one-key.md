---
kind: alternative
routes:
  api: Cost API
steps:
- text: The Administrator asks to rebuild one Cloud integration by its key and confirms
  kind: actor
  actor: administrator
  entities:
  - entity: cloud-integration
    effect: reads
    facts:
    - Key
  contexts:
    api:
      place: cost-api::administration
- text: The Product queries every day of the retention period again for that integration
  kind: product
  actor: administrator
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
      place: cost-api::administration
---

# Rebuild cloud costs for one key

## Trigger

One billing export was corrected at the provider.

## Outcome

That integration's cloud costs are imported again.
