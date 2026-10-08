---
appliesTo:
- type: entity
  id: custom-cost-plugin
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
  target: pkg/customcost/pipelineservice.go#getRegisteredPlugins
---

# Custom cost plugins are started only by OpenCost at start-up

OpenCost starts the custom cost plugins installed in its plugin directories when
it starts with custom costs enabled.
