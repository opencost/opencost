---
appliesTo:
- type: entity
  id: node
  facts:
  - Carbon estimate
- type: entity
  id: disk
  facts:
  - Carbon estimate
- type: entity
  id: load-balancer
  facts:
  - Carbon estimate
- type: entity
  id: cluster-management
  facts:
  - Carbon estimate
references:
- kind: code
  role: implementation
  target: pkg/carbon/carbonassets.go#lookupCarbonCoeff
- kind: code
  role: context
  target: pkg/carbon/carbonlookupdata.csv
---

# Carbon is estimated from provider, region and machine type

A node's carbon estimate uses its provider, region and machine type, and a
disk's its provider and region, falling back to the provider's average region;
the coefficient is applied per hour the asset ran. Only AWS, GCP and Azure have
coefficients; assets elsewhere, load balancers and cluster management fees are
estimated at zero.
