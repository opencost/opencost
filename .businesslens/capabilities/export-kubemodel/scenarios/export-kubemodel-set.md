---
kind: primary
routes:
  api: Cost API
steps:
- text: Five minutes have passed since the last export
  kind: condition
  unattended: true
  entities:
  - entity: opencost-settings
    effect: reads
    facts:
    - KubeModel export enabled
  contexts:
    api:
      place: cost-api::cost-reporting::kubemodel
- text: The Product writes the current hour's and day's KubeModel set to storage
  kind: product
  entities:
  - entity: kubemodel-set
    effect: creates
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

# Export a KubeModel set

## Trigger

The export schedule fires.

## Outcome

The latest KubeModel sets can be queried.

## Edge cases

- By default only the cluster, namespaces and resource quotas are filled in.
