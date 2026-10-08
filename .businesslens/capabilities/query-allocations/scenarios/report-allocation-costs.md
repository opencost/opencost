---
kind: primary
routes:
  api: Cost API
steps:
- text: The User asks for costs over a window, grouped by namespace, with idle included
  kind: actor
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::allocation-costs
- text: The Product computes each container's Allocation for the window and sums them by namespace
  kind: product
  actor: user
  entities:
  - entity: allocation
    effect: reads
    facts:
    - Name
    - Namespace
    - Window
    - Allocated resources
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
    - Total cost
    - Efficiency
  contexts:
    api:
      place: cost-api::cost-reporting::allocation-costs
- text: The Product returns the namespace allocations and one __idle__ Allocation holding the idle cost
  kind: product
  actor: user
  entities:
  - entity: allocation
    effect: reads
    facts:
    - Name
    - Idle cost
    - Total cost
  contexts:
    api:
      place: cost-api::cost-reporting::allocation-costs
---

# Report allocation costs

## Trigger

The User wants to know what each namespace cost over a window.

## Outcome

The User has one allocation per namespace with its resource costs, efficiency and total, plus the idle cost of the window.

## Edge cases

- An allocation with no value for the grouping is reported under __unallocated__.
- A grouping OpenCost does not recognise is ignored rather than refused.
- With idle shared, idle cost is spread over the other allocations by their cost instead of reported on its own.
- With idle counted per node, idle cost is reported per cluster and node before grouping.
- A filter the Product cannot parse is reported as an error.
