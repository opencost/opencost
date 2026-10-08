---
kind: validation
routes:
  api: Cost API
steps:
- text: The User asks for KubeModel without a readable window
  kind: actor
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::kubemodel
- text: The Product refuses the request as an invalid window
  kind: product
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::kubemodel
---

# Refuse an invalid KubeModel window

## Trigger

The User sends an unreadable window.

## Outcome

The User receives a bad request.
