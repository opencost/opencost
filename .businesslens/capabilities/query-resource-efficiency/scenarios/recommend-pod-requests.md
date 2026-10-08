---
kind: primary
routes:
  mcp: MCP server
steps:
- text: The AI agent asks for efficiency over the last week
  kind: actor
  actor: ai-agent
  entities: []
  contexts:
    mcp:
      place: mcp-server::efficiency
- text: The Product reads each workload's average requests and allocated resources over the week, grouped as asked and without idle or shared costs
  kind: product
  actor: ai-agent
  entities:
  - entity: allocation
    effect: reads
    facts:
    - Name
    - Resource requests
    - Allocated resources
    - CPU cost
    - RAM cost
    - Total cost
  contexts:
    mcp:
      place: mcp-server::efficiency
- text: The Product returns a Rightsizing recommendation for each workload with its savings
  kind: product
  actor: ai-agent
  entities:
  - entity: rightsizing-recommendation
    effect: reads
    facts:
    - Name
    - Window
    - Efficiency
    - Requested
    - Used
    - Recommended requests
    - Resulting efficiency
    - Current cost
    - Recommended cost
    - Savings
    - Buffer multiplier
  contexts:
    mcp:
      place: mcp-server::efficiency
---

# Recommend pod requests

## Trigger

An AI agent is asked where workloads are over-provisioned.

## Outcome

The AI agent has, for each pod, its efficiency, suggested requests and the cost those requests would save; nothing in the cluster changes.

## Edge cases

- A window with no duration is refused.
- Workloads are grouped by pod unless the AI agent asks for another grouping.
- A workload that requested nothing has an efficiency of zero.
- Long windows are measured in steps of a day, six hours or an hour unless the agent chooses a step.
