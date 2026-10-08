---
appliesTo:
- type: entity
  id: custom-cost
  effect: creates
permits:
- unattended: true
  when:
  - entity: opencost-settings
    fact: Custom costs enabled
    is: true
references:
- kind: code
  role: implementation
  target: pkg/customcost/ingestor.go
---

# Custom costs are imported only from custom cost plugins on schedule

Custom costs and their plugins appear only through the Product's own start-up
and hourly and daily imports, while custom costs are enabled.
