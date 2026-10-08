---
kind: primary
routes:
  api: Cost API
steps:
- text: The Administrator sends a service account key
  kind: actor
  actor: administrator
  entities: []
  contexts:
    api:
      place: cost-api::administration
- text: The Product stores it as the GCP service key, replacing any earlier one
  kind: product
  actor: administrator
  entities:
  - entity: gcp-service-key
    effect: changes
    facts:
    - Key
  contexts:
    api:
      place: cost-api::administration
---

# Store a GCP service key

## Trigger

The Administrator sets up GCP access.

## Outcome

OpenCost holds the new key.

## Edge cases

- An empty key replaces the stored key with nothing.
- A failure to store the key is described in the reply, which still reports success.
