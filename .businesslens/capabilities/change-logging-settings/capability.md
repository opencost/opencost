---
availability:
- place: cost-api::cost-reporting
references:
- kind: code
  role: implementation
  target: core/pkg/util/apiutil/loglevel.go#SetLogLevel
- kind: code
  role: implementation
  target: core/pkg/util/apiutil/logexclude.go#SetLogExclude
- kind: code
  role: implementation
  target: core/pkg/util/apiutil/apiutil.go#ApplyContainerDiagnosticEndpoints
---

# Change logging settings

Read and change how much OpenCost logs while it runs: the log level, and the
text fragments whose messages are dropped. Changes last until OpenCost
restarts.
