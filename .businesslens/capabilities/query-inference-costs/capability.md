---
availability:
- place: cost-api::cost-reporting
references:
- kind: code
  role: implementation
  target: pkg/inferencecost/queryservice.go
- kind: code
  role: implementation
  target: pkg/inferencecost/calculator.go
- kind: code
  role: implementation
  target: pkg/inferencecost/collector.go
- kind: doc
  role: intent
  target: docs/inference-cost-tracking.md
---

# Query inference costs

Report the cost of serving AI models on vLLM over a window, per model and
namespace or another grouping, as a total or a time series, on an allocation
basis that shares idle and shared serving infrastructure in or a usage basis
that leaves them out. Offered only while inference costs are enabled.

Offered only while OpenCost runs in Kubernetes.
