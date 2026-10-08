---
appliesTo:
- type: entity
  id: cloud-cost
  effect: removes
permits:
- unattended: true
  when:
  - entity: opencost-settings
    fact: Cloud costs enabled
    is: true
references:
- kind: code
  role: implementation
  target: pkg/cloudcost/memoryrepository.go
- kind: code
  role: implementation
  target: pkg/env/cloudcost.go
---

# Cloud costs older than the retention period are discarded

After each import, cloud costs older than the cloud cost retention — 30 days
unless configured — are discarded.
