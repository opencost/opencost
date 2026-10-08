---
appliesTo:
- type: entity
  id: allocation
  facts:
  - Proportional asset resource costs
references:
- kind: code
  role: implementation
  target: pkg/costmodel/costmodel.go#QueryAllocation
---

# Proportional asset resource costs are reported only with idle included

A request for each workload's share of node and disk costs is refused unless
idle is included.
