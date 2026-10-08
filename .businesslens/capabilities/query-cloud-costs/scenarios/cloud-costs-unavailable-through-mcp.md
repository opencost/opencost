---
kind: edge
routes:
  mcp: MCP server
steps:
- text: The AI agent asks for cloud costs while cloud costs are disabled
  kind: actor
  actor: ai-agent
  entities:
  - entity: cloud-cost
    effect: reads
    facts: []
  contexts:
    mcp:
      place: mcp-server::cloud-costs
- text: The Product reports that the cloud integration file is not configured
  kind: product
  actor: ai-agent
  entities:
  - entity: cloud-integration
    effect: reads
    facts: []
  contexts:
    mcp:
      place: mcp-server::cloud-costs
---

# Cloud costs unavailable through MCP

## Trigger

An AI agent asks for cloud costs on an installation that does not import them.

## Outcome

The AI agent receives an error pointing at the cloud integration file.
