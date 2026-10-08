---
kind: alternative
routes:
  api: Cost API
steps:
- text: The User reads the excluded patterns
  kind: actor
  actor: user
  entities:
  - entity: logging-settings
    effect: reads
    facts:
    - Excluded patterns
  contexts:
    api:
      place: cost-api::cost-reporting::logging-settings
- text: The User sends a new list of patterns
  kind: actor
  actor: user
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::logging-settings
- text: The Product replaces the list, ignoring blank entries, and drops every later message containing one
  kind: product
  actor: user
  entities:
  - entity: logging-settings
    effect: changes
    facts:
    - Excluded patterns
  contexts:
    api:
      place: cost-api::cost-reporting::logging-settings
---

# Change log exclusions

## Trigger

A noisy message floods the log.

## Outcome

Matching messages are no longer written; an empty list clears all exclusions.
