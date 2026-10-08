---
kind: primary
routes:
  mcp: MCP server
steps:
- text: The AI agent asks for costs over a window grouped by namespace
  kind: actor
  actor: ai-agent
  entities: []
  contexts:
    mcp:
      place: mcp-server::allocation-costs
- text: The Product returns each Allocation with its resource costs, resource hours and total
  kind: product
  actor: ai-agent
  entities:
  - entity: allocation
    effect: reads
    facts:
    - Name
    - Window
    - CPU cost
    - GPU cost
    - RAM cost
    - PV cost
    - Network cost
    - Shared cost
    - External cost
    - Total cost
    - Resource hours
  contexts:
    mcp:
      place: mcp-server::allocation-costs
---

# Query allocations through MCP

## Trigger

An AI agent needs workload costs to answer its person's question.

## Outcome

The AI agent has the allocations of the first step of the window.

## Edge cases

- A filter the Product cannot parse is refused before anything is computed.
- A step that is not a positive duration is refused.
