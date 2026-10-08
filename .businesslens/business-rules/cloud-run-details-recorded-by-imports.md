---
appliesTo:
- type: entity
  id: cloud-integration
  effect: changes
  facts:
  - Connection status
  - Last run
  - Next run
  - Runs
  - Coverage
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

# Only cloud cost imports record an integration's run details

Connection status, run times, run count and coverage change only as the
Product's scheduled imports run.
