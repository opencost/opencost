---
kind: edge
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
- text: The chart provided no values
  kind: condition
  actor: administrator
  entities:
  - entity: installation
    effect: reads
    facts:
    - Helm values
  contexts:
    api:
      place: cost-api::administration::helm-values
- text: The Product replies that values reporting is disabled
  kind: product
  actor: administrator
  entities: []
  contexts:
    api:
      place: cost-api::administration::helm-values
---

# Helm values not reported

## Trigger

The installation was deployed without values reporting.

## Outcome

The Administrator learns no values are available.
