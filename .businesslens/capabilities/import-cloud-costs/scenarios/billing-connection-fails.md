---
kind: edge
routes:
  api: Cost API
steps:
- text: The refresh interval has passed since an active Cloud integration last imported
  kind: condition
  unattended: true
  entities:
  - entity: cloud-integration
    effect: reads
    facts:
    - Last run
    - Refresh rate
  contexts:
    api:
      place: cost-api::cost-reporting::cloud-integration-status
- text: The Product cannot read the provider's billing export
  kind: condition
  entities: []
  contexts:
    api:
      place: cost-api::cost-reporting::cloud-integration-status
- text: The Product records a failed connection on the Cloud integration
  kind: product
  entities:
  - entity: cloud-integration
    effect: changes
    facts:
    - Connection status
    - Last run
    - Next run
    - Runs
  contexts:
    api:
      place: cost-api::cost-reporting::cloud-integration-status
---

# Billing connection fails

## Trigger

The provider rejects the credentials or the export is missing.

## Outcome

The integration's status shows why no data arrived; earlier cloud costs stay.

## Edge cases

- Invalid configuration, a parse error and missing data are reported as their own connection statuses.
