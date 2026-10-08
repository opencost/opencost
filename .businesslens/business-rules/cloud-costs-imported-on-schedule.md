---
appliesTo:
- type: entity
  id: cloud-cost
  effect: creates
permits:
- unattended: true
  when:
  - entity: opencost-settings
    fact: Cloud costs enabled
    is: true
references:
- kind: code
  role: implementation
  target: pkg/cloudcost/ingestor.go
---

# Cloud costs are imported only by the Product's schedule

New cloud costs arrive only from the scheduled imports of active cloud
integrations.
