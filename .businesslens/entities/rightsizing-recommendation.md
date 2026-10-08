---
references:
- kind: code
  role: implementation
  target: pkg/mcp/server.go#computeEfficiencyMetric
---

# Rightsizing recommendation

A suggestion for one workload's CPU and memory requests over a window, and what
it would save. What it reports as used is the workload's average allocated CPU
and memory — the larger of its request and its usage — not its measured usage
alone.

## Information kept

- **Name** — the workload grouping it is for
- **Window** — the period it was measured over
- **Efficiency** — CPU and memory used over requested, zero when nothing was requested
- **Requested** — CPU cores and RAM bytes requested
- **Used** — average CPU cores and RAM bytes allocated over the window
- **Recommended requests** — the CPU and RAM requests suggested
- **Resulting efficiency** — CPU and memory efficiency at the suggested requests
- **Current cost** — the workload's total cost over the window
- **Recommended cost** — the total cost at the suggested requests
- **Savings** — the cost saved, in money and as a percentage
- **Buffer multiplier** — the headroom applied over observed use
