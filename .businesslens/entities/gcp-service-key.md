---
singleton: true
references:
- kind: code
  role: implementation
  target: pkg/costmodel/router.go#AddServiceKey
---

# GCP service key

The Google Cloud service account key OpenCost uses for GCP pricing and BigQuery
billing access.

## Information kept

- **Key** — the service account key document
