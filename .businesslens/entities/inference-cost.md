---
references:
- kind: code
  role: implementation
  target: pkg/inferencecost/apitypes.go
- kind: doc
  role: intent
  target: docs/inference-cost-tracking.md
---

# Inference cost

The cost of serving one AI model in one namespace over a window, from vLLM
token metrics and the allocated cost of the pods serving it. It is computed
when asked for and kept nowhere.

## Information kept

- **Model** — the model name and version
- **Namespace** — the namespace serving it
- **Workload type** — the kind of serving workload
- **Cost basis** — allocation, which shares idle and shared infrastructure in, or usage, which leaves them out
- **Window** — the period it covers
- **Total cost** — the cost of serving the model over the window
- **Hourly cost** — that cost per hour
- **Tokens** — prompt, generation and total tokens served
- **Cost per million tokens** — total cost per million tokens
- **Input cost** — the cost of prompt tokens, in total and per million
- **Output cost** — the cost of generated tokens, in total and per million
- **Cache savings fraction** — the share of prompt tokens served from the prefix cache
- **Allocation method** — how cost was split between input and output: by compute time or by token multiplier
