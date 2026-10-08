---
availability:
- place: prometheus-metrics
references:
- kind: code
  role: implementation
  target: pkg/costmodel/metrics.go
- kind: code
  role: implementation
  target: pkg/metrics/kubemetrics.go
- kind: code
  role: implementation
  target: pkg/metrics/metricsconfig.go
- kind: code
  role: implementation
  target: pkg/inferencecost/exporter.go
- kind: code
  role: implementation
  target: pkg/cmd/agent/agent.go#Execute
---

# Scrape cost metrics

Publish OpenCost's prices and allocations as Prometheus metrics, refreshed every
minute: hourly node, volume, load balancer and cluster management prices,
network traffic prices, container CPU, memory and GPU allocations, Kubernetes
object metrics and, while inference costs are enabled, AI model serving costs.
Metrics listed as disabled in the metrics configuration are left out. Agent
mode publishes the same metrics without the cost API.
