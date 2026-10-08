---
appliesTo:
- type: entity
  id: allocation
  facts:
  - Idle cost
references:
- kind: code
  role: implementation
  target: pkg/costmodel/allocation_helpers.go
- kind: spec
  role: intent
  target: spec/opencost-specv01.md
  title: Idle Costs
---

# Idle cost is the asset cost no workload was allocated

For each cluster, or each node when asked, idle cost is the CPU, GPU and RAM
cost of the assets minus the cost allocated to workloads, never below zero.
