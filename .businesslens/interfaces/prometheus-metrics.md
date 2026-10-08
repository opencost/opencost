---
type: api
actors:
- metrics-scraper
references:
- kind: code
  role: implementation
  target: pkg/costmodel/metrics.go
- kind: code
  role: implementation
  target: pkg/cmd/agent/agent.go#Execute
- kind: doc
  role: context
  target: https://www.opencost.io/docs/integrations/prometheus
---

# Prometheus metrics

The Prometheus metrics endpoint, served beside the cost API and, in agent mode,
on its own. It publishes node, volume, load balancer, cluster management and
network prices, container allocations, Kubernetes object metrics and, when
enabled, AI model serving costs.
