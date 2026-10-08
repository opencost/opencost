---
appliesTo:
- type: entity
  id: rightsizing-recommendation
  facts:
  - Recommended requests
  - Buffer multiplier
references:
- kind: code
  role: implementation
  target: pkg/mcp/server.go#computeEfficiencyMetric
---

# Recommended requests add headroom over allocated use

A rightsizing recommendation is the workload's average allocated CPU and memory
times the buffer multiplier — 1.2, or 20% headroom, unless the AI agent asks
for another — and never below a thousandth of a core or one MiB. The
recommended cost prices the recommended requests at what the workload's
requests cost per hour, keeping its other costs.
