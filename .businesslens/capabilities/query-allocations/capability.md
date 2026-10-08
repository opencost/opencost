---
availability:
- place: cost-api::cost-reporting
- place: mcp-server
references:
- kind: code
  role: implementation
  target: pkg/costmodel/aggregation.go#ComputeAllocationHandler
- kind: code
  role: implementation
  target: pkg/costmodel/aggregation.go#ComputeAllocationHandlerSummary
- kind: code
  role: implementation
  target: pkg/costmodel/autocomplete.go#ComputeAllocationAutocompleteHandler
- kind: code
  role: implementation
  target: pkg/costmodel/costmodel.go#QueryAllocation
- kind: code
  role: implementation
  target: pkg/mcp/server.go#QueryAllocations
---

# Query allocations

Report what workloads cost over a window, grouped by any mix of cluster, node,
namespace, controller, pod, container, service, label or annotation, as one
set or a set per step. The User can include idle cost, share it across
workloads, count idle per node, filter which workloads count, and ask for a
lighter summary; an AI agent asks the same through the MCP server.

Offered only while OpenCost runs in Kubernetes; on the MCP server, only while
the MCP server is enabled too.
