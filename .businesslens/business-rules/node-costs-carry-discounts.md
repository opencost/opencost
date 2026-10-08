---
appliesTo:
- type: entity
  id: node
  facts:
  - CPU cost
  - RAM cost
  - Discount
- type: entity
  id: pricing
  facts:
  - Discount
  - Negotiated discount
references:
- kind: code
  role: implementation
  target: pkg/costmodel/allocation_helpers.go#applyNodeDiscount
---

# Node CPU and RAM costs carry the configured discounts

A node's CPU and RAM prices are reduced by the configured discount and
negotiated discount before its costs, and the allocations on it, are computed.
