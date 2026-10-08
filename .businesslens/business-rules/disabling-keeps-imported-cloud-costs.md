---
appliesTo:
- type: capability
  id: disable-cloud-integration
- type: capability
  id: load-cloud-integrations
- type: entity
  id: cloud-cost
references:
- kind: code
  role: implementation
  target: pkg/cloudcost/ingestionmanager.go
---

# Disabling a cloud integration keeps the cloud costs it imported

Disabling or removing a cloud integration stops its imports; the cloud costs it
already imported stay and can still be queried. They are discarded once older
than the cloud cost retention, but only when another active integration's
import runs; with no active integration left, they stay until OpenCost
restarts.
