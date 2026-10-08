---
kind: alternative
routes:
  api: Cost API
steps:
- text: The User asks for the plugin status
  kind: actor
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::custom-cost-status
- text: Plugins are disabled, or they failed to start
  kind: condition
  entities:
  - entity: opencost-settings
    effect: reads
    facts:
    - Custom costs enabled
  contexts:
    api:
      place: cost-api::cost-reporting::custom-cost-status
- text: The Product reports the plugins as not enabled, with no domains
  kind: product
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::custom-cost-status
---

# Custom costs reported off

## Trigger

The User checks an installation where custom costs are not running.

## Outcome

The User learns custom costs are off.
