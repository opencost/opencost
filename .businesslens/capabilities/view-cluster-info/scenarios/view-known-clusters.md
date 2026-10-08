---
kind: alternative
routes:
  api: Cost API
steps:
- text: The User asks for the map of known clusters
  kind: actor
  actor: user
  entities:
  - entity: cluster
    effect: reads
    facts: []
  contexts:
    api:
      place: cost-api::cost-reporting::cluster-info
- text: The Product returns every Cluster the data source holds metrics for
  kind: product
  actor: user
  entities:
  - entity: cluster
    effect: reads
    facts:
    - ID
    - Name
    - Provider
    - Provisioner
    - Kubernetes version
    - Profile
  contexts:
    api:
      place: cost-api::cost-reporting::cluster-info
---

# View known clusters

## Trigger

The User's Prometheus holds several clusters.

## Outcome

The User has each known cluster by ID.
