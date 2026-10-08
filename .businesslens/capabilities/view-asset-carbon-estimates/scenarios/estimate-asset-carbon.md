---
kind: primary
routes:
  api: Cost API
steps:
- text: The User asks for carbon estimates over a window
  kind: actor
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::asset-costs
- text: The Product returns the Carbon estimate of each Node, Disk, Load balancer and Cluster management fee over the window
  kind: product
  actor: user
  entities:
  - entity: node
    effect: reads
    facts:
    - Name
    - Carbon estimate
  - entity: disk
    effect: reads
    facts:
    - Name
    - Carbon estimate
  - entity: load-balancer
    effect: reads
    facts:
    - Name
    - Carbon estimate
  - entity: cluster-management
    effect: reads
    facts:
    - Cluster
    - Carbon estimate
  contexts:
    api:
      place: cost-api::cost-reporting::asset-costs
---

# Estimate asset carbon

## Trigger

The User wants the cluster's footprint in CO2e.

## Outcome

The User has an estimate in metric tonnes of CO2e for each asset.

## Edge cases

- A machine type or region missing from the carbon data uses the provider's average region.
- Load balancers and cluster management fees are reported with zero emissions.
- Assets on providers other than AWS, GCP and Azure are reported with zero emissions.
