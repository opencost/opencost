---
availability:
- place: cost-api::administration
references:
- kind: code
  role: implementation
  target: pkg/costmodel/router.go#AddServiceKey
---

# Add GCP service key

Let the Administrator give OpenCost a Google Cloud service account key, stored
for GCP pricing and BigQuery billing access.

Offered only while OpenCost runs in Kubernetes.
