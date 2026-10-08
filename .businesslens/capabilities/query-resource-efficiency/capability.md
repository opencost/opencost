---
availability:
- place: mcp-server
references:
- kind: code
  role: implementation
  target: pkg/mcp/server.go#QueryEfficiency
- kind: code
  role: implementation
  target: pkg/mcp/server.go#computeEfficiencyMetric
---

# Query resource efficiency

Tell an AI agent how efficiently workloads use the CPU and memory they request
over a window, recommend requests with headroom over their allocated use — 20%
unless the agent asks for another multiplier — and what those requests would
save. OpenCost never applies a recommendation to the cluster.

Offered only while OpenCost runs in Kubernetes.
