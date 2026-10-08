---
singleton: true
references:
- kind: code
  role: implementation
  target: core/pkg/util/apiutil/loglevel.go
- kind: code
  role: implementation
  target: core/pkg/util/apiutil/logexclude.go
- kind: code
  role: implementation
  target: core/pkg/log/log.go
---

# Logging settings

How much OpenCost writes to its log while it runs.

## Information kept

- **Log level** — the lowest severity written, such as info or debug
- **Excluded patterns** — text fragments; any log message containing one is dropped
