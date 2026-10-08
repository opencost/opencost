---
kind: edge
routes:
  api: Cost API
steps:
- text: OpenCost starts with plugins enabled
  kind: condition
  unattended: true
  entities:
  - entity: opencost-settings
    effect: reads
    facts:
    - Custom costs enabled
  contexts:
    api:
      place: cost-api::cost-reporting::custom-cost-status
- text: A plugin configuration file is misnamed or its executable is missing
  kind: condition
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::custom-cost-status
- text: The Product starts no plugins and offers no queries of their costs
  kind: product
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::custom-cost-status
---

# A misconfigured plugin stops custom costs

## Trigger

An operator installs a plugin incorrectly.

## Outcome

Custom costs report as not enabled until the installation is fixed.
