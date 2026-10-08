---
kind: alternative
routes:
  api: Cost API
steps:
- text: A day has passed since the last pruning
  kind: condition
  unattended: true
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::kubemodel
- text: The Product deletes each KubeModel set older than its resolution's retention
  kind: product
  entities:
  - entity: kubemodel-set
    effect: removes
  contexts:
    api:
      place: cost-api::cost-reporting::kubemodel
---

# Prune old KubeModel sets

## Trigger

The daily clean-up fires.

## Outcome

Only sets within retention remain.
