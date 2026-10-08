---
kind: primary
routes:
  mcp: MCP server
steps:
- text: The AI agent asks for cloud costs over a window for one provider and service
  kind: actor
  actor: ai-agent
  entities:
  - entity: cloud-cost
    effect: reads
    facts: []
  contexts:
    mcp:
      place: mcp-server::cloud-costs
- text: The Product returns the matching Cloud cost items with totals by provider, service and region
  kind: product
  actor: ai-agent
  entities:
  - entity: cloud-cost
    effect: reads
    facts:
    - Provider
    - Account
    - Region
    - Service
    - Category
    - Day
    - List cost
    - Net cost
    - Amortized net cost
    - Invoiced cost
    - Amortized cost
    - Kubernetes percent
  contexts:
    mcp:
      place: mcp-server::cloud-costs
---

# Query cloud costs through MCP

## Trigger

An AI agent needs cloud spend.

## Outcome

The AI agent has the items, their net, amortized and invoiced totals and the Kubernetes share.

## Edge cases

- A filter expression replaces the provider, service, category and account arguments.
- A filter expression the Product cannot parse is ignored and the query runs unfiltered.
- The region argument does not narrow the result.
