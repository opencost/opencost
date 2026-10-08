---
kind: alternative
routes:
  api: Cost API
steps:
- text: The User asks for a summary of costs over a window, grouped by controller
  kind: actor
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::allocation-costs
- text: The Product returns each Allocation's costs, requests, usage and efficiency without its properties or total
  kind: product
  actor: user
  entities:
  - entity: allocation
    effect: reads
    facts:
    - Name
    - Window
    - Resource requests
    - Resource usage
    - CPU cost
    - GPU cost
    - RAM cost
    - PV cost
    - Network cost
    - Load balancer cost
    - Shared cost
    - External cost
    - Idle cost
    - Efficiency
  contexts:
    api:
      place: cost-api::cost-reporting::allocation-costs
---

# Summarize allocation costs

## Trigger

The User wants a lighter report of the same costs.

## Outcome

The User has per-controller cost figures without workload properties.
