---
kind: primary
routes:
  mcp: MCP server
steps:
- text: The AI agent asks for asset costs over a window
  kind: actor
  actor: ai-agent
  entities: []
  contexts:
    mcp:
      place: mcp-server::asset-costs
- text: The Product returns each Node, Disk, Load balancer and Cluster management fee with its cost
  kind: product
  actor: ai-agent
  entities:
  - entity: node
    effect: reads
    facts:
    - Name
    - Provider
    - Node type
    - Labels
    - Window
    - Capacity
    - Preemptible
    - Discount
    - CPU cost
    - RAM cost
    - GPU cost
    - Total cost
  - entity: disk
    effect: reads
    facts:
    - Name
    - Storage class
    - Claim
    - Local
    - Size
    - Window
    - Total cost
  - entity: load-balancer
    effect: reads
    facts:
    - Name
    - IP address
    - Private
    - Window
    - Total cost
  - entity: cluster-management
    effect: reads
    facts:
    - Cluster
    - Window
    - Total cost
  contexts:
    mcp:
      place: mcp-server::asset-costs
---

# Query assets through MCP

## Trigger

An AI agent needs infrastructure costs.

## Outcome

The AI agent has every asset of the window with its cost.
