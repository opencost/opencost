---
kind: primary
routes:
  api: Cost API
steps:
- text: The User asks for cluster info
  kind: actor
  actor: user
  entities:
  - entity: cluster
    effect: reads
    facts: []
  contexts:
    api:
      place: cost-api::cost-reporting::cluster-info
- text: The Product returns the local Cluster with its provider, provisioner and Kubernetes version
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

# View the local cluster

## Trigger

The User wants to know what OpenCost is monitoring.

## Outcome

The User has the cluster's identity and provider.

## Edge cases

- When a cluster info file is configured, its contents are returned instead.
- The management platform alone, such as GKE or EKS, can be asked for on its own.
