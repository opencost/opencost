---
kind: primary
routes:
  api: Cost API
steps:
- text: The Administrator asks for the Helm values
  kind: actor
  actor: administrator
  entities: []
  contexts:
    api:
      place: cost-api::administration::helm-values
- text: The Product returns the Installation's Helm values as deployed
  kind: product
  actor: administrator
  entities:
  - entity: installation
    effect: reads
    facts:
    - Helm values
  contexts:
    api:
      place: cost-api::administration::helm-values
---

# View deployed Helm values

## Trigger

The Administrator is reviewing how OpenCost was deployed.

## Outcome

The Administrator has the values, unredacted.
