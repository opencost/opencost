---
kind: primary
routes:
  api: Cost API
steps:
- text: The User asks for KubeModel over the last two days
  kind: actor
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::kubemodel
- text: The Product returns each daily KubeModel set exported for those days
  kind: product
  actor: user
  entities:
  - entity: kubemodel-set
    effect: reads
    facts:
    - Window
    - Cluster
    - Namespaces
    - Workloads
    - Infrastructure
    - Pods
  contexts:
    api:
      place: cost-api::cost-reporting::kubemodel
---

# Read KubeModel sets

## Trigger

The User wants the cluster's inventory for a period.

## Outcome

The User has the sets; periods with no export are skipped.
