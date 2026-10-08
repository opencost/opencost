---
appliesTo:
- type: entity
  id: kubemodel-set
  effect: removes
permits:
- unattended: true
  when:
  - entity: opencost-settings
    fact: KubeModel export enabled
    is: true
references:
- kind: code
  role: implementation
  target: pkg/kubemodel/janitor.go#retentionFor
---

# KubeModel sets older than their retention are pruned

Daily sets older than 30 days, hourly sets older than 49 hours and ten-minute
sets older than 36 periods are pruned once a day.
