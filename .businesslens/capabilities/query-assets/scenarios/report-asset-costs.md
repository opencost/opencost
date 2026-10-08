---
kind: primary
routes:
  api: Cost API
steps:
- text: The User asks for asset costs over a window
  kind: actor
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::asset-costs
- text: The Product returns each Node, Disk, Load balancer and Cluster management fee with its cost over the window
  kind: product
  actor: user
  entities:
  - entity: node
    effect: reads
    facts:
    - Name
    - Cluster
    - Provider
    - Provider ID
    - Node type
    - Labels
    - Window
    - Capacity
    - Preemptible
    - Discount
    - CPU cost
    - RAM cost
    - GPU cost
    - Total cost
  - entity: disk
    effect: reads
    facts:
    - Name
    - Cluster
    - Provider ID
    - Storage class
    - Claim
    - Local
    - Size
    - Window
    - Total cost
  - entity: load-balancer
    effect: reads
    facts:
    - Name
    - Cluster
    - IP address
    - Private
    - Window
    - Total cost
  - entity: cluster-management
    effect: reads
    facts:
    - Cluster
    - Provisioner
    - Window
    - Total cost
  contexts:
    api:
      place: cost-api::cost-reporting::asset-costs
---

# Report asset costs

## Trigger

The User wants to know what the cluster's infrastructure cost.

## Outcome

The User has every asset of the window with its cost.

## Edge cases

- A filter the Product cannot parse is reported as an error.
- Fargate nodes are not reported as assets.
