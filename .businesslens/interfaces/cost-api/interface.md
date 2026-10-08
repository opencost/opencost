---
type: api
actors:
- user
- administrator
references:
- kind: code
  role: implementation
  target: pkg/cmd/costmodel/costmodel.go#Execute
- kind: code
  role: implementation
  target: pkg/costmodel/router.go#Initialize
- kind: spec
  role: context
  target: docs/swagger.json
- kind: doc
  role: context
  target: AGENTS.md
---

# Cost API

OpenCost's HTTP API, served by the cost model on port 9003 by default. Readers
query allocations, assets, cloud, custom and inference costs and the
installation's pricing and cluster details without credentials; a separate set
of administration operations needs the admin token.

## Intent

The contract every OpenCost client builds on, including the OpenCost UI and
kubectl cost.
