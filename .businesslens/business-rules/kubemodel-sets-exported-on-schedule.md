---
appliesTo:
- type: entity
  id: kubemodel-set
  effect: creates
permits:
- unattended: true
  when:
  - entity: opencost-settings
    fact: KubeModel export enabled
    is: true
references:
- kind: code
  role: implementation
  target: pkg/kubemodel/pipeline.go
---

# KubeModel sets are written only by the export schedule

KubeModel sets are written every five minutes while KubeModel export is
enabled; no request writes one.
