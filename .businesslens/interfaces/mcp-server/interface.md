---
type: agent
actors:
- ai-agent
references:
- kind: code
  role: implementation
  target: pkg/cmd/costmodel/costmodel.go#StartMCPServer
- kind: code
  role: implementation
  target: pkg/mcp/server.go
- kind: doc
  role: intent
  target: README.md
  title: MCP Server
---

# MCP server

A Model Context Protocol server, on port 8081 by default, through which an AI
agent queries allocation, asset and cloud costs and asks for rightsizing
recommendations. It runs only when the MCP server is enabled and OpenCost runs
inside Kubernetes, and it asks for no credentials.
