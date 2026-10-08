---
kind: primary
routes:
  api: Cost API
steps:
- text: The User asks for orphaned pods
  kind: actor
  actor: user
  entities:
  - entity: pod
    effect: reads
    facts: []
  contexts:
    api:
      place: cost-api::cost-reporting::orphaned-pods
- text: The Product returns every Pod without owner references
  kind: product
  actor: user
  entities:
  - entity: pod
    effect: reads
    facts:
    - Name
    - Namespace
    - Labels
    - Annotations
    - Owner references
    - Status
  contexts:
    api:
      place: cost-api::cost-reporting::orphaned-pods
---

# List orphaned pods in the cluster

## Trigger

The User looks for workloads that will not be rescheduled or attributed to a controller.

## Outcome

The User has the orphaned pods, or none.
