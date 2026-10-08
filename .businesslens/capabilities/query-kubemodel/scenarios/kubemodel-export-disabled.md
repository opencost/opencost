---
kind: edge
routes:
  api: Cost API
steps:
- text: The User asks for KubeModel while export is disabled
  kind: actor
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::kubemodel
- text: The Product reports that the KubeModel pipeline is not initialized
  kind: product
  actor: user
  entities:
  - entity: opencost-settings
    effect: reads
    facts:
    - KubeModel export enabled
  contexts:
    api:
      place: cost-api::cost-reporting::kubemodel
---

# KubeModel export disabled

## Trigger

The User queries an installation that does not export KubeModel.

## Outcome

The User receives a service-unavailable response.
