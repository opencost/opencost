---
appliesTo:
- type: entity
  id: inference-cost
  facts:
  - Input cost
  - Output cost
  - Allocation method
references:
- kind: code
  role: implementation
  target: pkg/inferencecost/calculator.go#calculateMultiplierSplit
- kind: doc
  role: intent
  target: docs/inference-cost-tracking.md
---

# Without compute-time data, a generated token costs two and a half prompt tokens

Inference cost is split between input and output by prefill and decode time
when both are known; otherwise each generated token is weighted 2.5 times a
prompt token not served from the prefix cache.
