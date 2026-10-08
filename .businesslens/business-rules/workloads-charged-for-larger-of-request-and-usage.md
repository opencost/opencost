---
appliesTo:
- type: entity
  id: allocation
  facts:
  - Allocated resources
  - CPU cost
  - RAM cost
  - GPU cost
references:
- kind: code
  role: implementation
  target: pkg/costmodel/costmodel.go#getContainerAllocation
- kind: spec
  role: intent
  target: spec/opencost-specv01.md
  title: Workload Costs
---

# A workload is charged for the larger of what it requests and what it uses

CPU, memory and GPU allocated to a container is the larger of its request and
its usage, and its cost is that allocation times the node's price.

## Rationale

Capacity a workload reserves is unavailable to others whether or not it is used.
