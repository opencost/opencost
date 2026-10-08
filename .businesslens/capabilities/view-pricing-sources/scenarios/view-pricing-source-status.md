---
kind: primary
routes:
  api: Cost API
steps:
- text: The User asks for the pricing source status
  kind: actor
  actor: user
  entities:
  - entity: pricing-source
    effect: reads
    facts: []
  contexts:
    api:
      place: cost-api::cost-reporting::pricing-sources
- text: The Product returns each Pricing source with whether it is enabled and available, and its error
  kind: product
  actor: user
  entities:
  - entity: pricing-source
    effect: reads
    facts:
    - Name
    - Enabled
    - Available
    - Error
  contexts:
    api:
      place: cost-api::cost-reporting::pricing-sources
---

# View pricing source status

## Trigger

The User wants to know why prices look wrong.

## Outcome

The User sees which pricing sources work.
