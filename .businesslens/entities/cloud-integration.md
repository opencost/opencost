---
domain: cloud-costs
relations:
- entity: cloud-cost
  verb: imports
  cardinality: one-to-many
references:
- kind: code
  role: implementation
  target: pkg/cloud/config/statuses.go
- kind: code
  role: implementation
  target: pkg/cloud/config/controller.go
- kind: code
  role: implementation
  target: pkg/cloudcost/integration.go#GetIntegrationFromConfig
---

# Cloud integration

A connection to one cloud provider's billing export — AWS Athena or S3 cost
and usage reports, a GCP BigQuery billing export, Azure storage billing
exports, the Oracle Usage API, the STACKIT cost API or IBM usage reports —
read from the cloud integration file. Only an active integration imports
cloud costs.

## Information kept

- **Key** — identifies the billing source, such as the account and bucket, or the project and table
- **Source** — where the integration came from, such as the cloud integration file
- **Provider** — the cloud provider billed
- **Integration type** — the kind of billing export, such as Athena, S3, BigQuery or Azure storage
- **Configuration** — the connection settings, with every secret replaced by REDACTED
- **Valid** — whether the configuration passed validation when it was read
- **Connection status** — the outcome of the latest import: no connection, invalid configuration, failed connection, parse error, data missing or connection successful
- **Last run** — when the latest import ran
- **Next run** — when the next import will run
- **Runs** — how many imports have run
- **Coverage** — the window of days imported so far
- **Refresh rate** — how often it imports

## States

### Active

OpenCost imports this integration's billing data on schedule.

### Inactive

OpenCost keeps the integration but imports nothing from it, because its entry in the cloud integration file is not valid or the Administrator disabled it.
